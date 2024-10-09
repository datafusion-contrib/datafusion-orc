// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{io::Read, marker::PhantomData};

use bytes::BytesMut;

use crate::{
    encoding::{rle::GenericRle, util::try_read_u8, PrimitiveValueEncoder},
    error::Result,
    memory::EstimateMemory,
};

use self::{
    delta::{read_delta_values, write_fixed_delta, write_varying_delta},
    direct::{read_direct_values, write_direct},
    patched_base::{read_patched_base, write_patched_base},
    short_repeat::{read_short_repeat_values, write_short_repeat},
};

use super::{util::calculate_percentile_bits, EncodingSign, NInt, VarintSerde};

mod delta;
mod direct;
mod patched_base;
mod short_repeat;

const MAX_RUN_LENGTH: usize = 512;
/// Minimum number of repeated values required to use Short Repeat sub-encoding
const SHORT_REPEAT_MIN_LENGTH: usize = 3;
const SHORT_REPEAT_MAX_LENGTH: usize = 10;
const BASE_VALUE_LIMIT: i64 = 1 << 56;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// TODO: put header data in here, e.g. base value, len, etc.
enum EncodingType {
    ShortRepeat,
    Direct,
    PatchedBase,
    Delta,
}

impl EncodingType {
    /// Checking highest two bits for encoding type.
    #[inline]
    fn from_header(header: u8) -> Self {
        match header & 0b_1100_0000 {
            0b_1100_0000 => Self::Delta,
            0b_1000_0000 => Self::PatchedBase,
            0b_0100_0000 => Self::Direct,
            0b_0000_0000 => Self::ShortRepeat,
            _ => unreachable!(),
        }
    }

    /// Return byte with highest two bits set according to variant.
    #[inline]
    fn to_header(self) -> u8 {
        match self {
            EncodingType::Delta => 0b_1100_0000,
            EncodingType::PatchedBase => 0b_1000_0000,
            EncodingType::Direct => 0b_0100_0000,
            EncodingType::ShortRepeat => 0b_0000_0000,
        }
    }
}

pub struct RleV2Decoder<N: NInt, R: Read, S: EncodingSign> {
    reader: R,
    decoded_ints: Vec<N>,
    /// Indexes into decoded_ints to make it act like a queue
    current_head: usize,
    deltas: Vec<i64>,
    sign: PhantomData<S>,
}

impl<N: NInt, R: Read, S: EncodingSign> RleV2Decoder<N, R, S> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            decoded_ints: Vec::with_capacity(MAX_RUN_LENGTH),
            current_head: 0,
            deltas: Vec::with_capacity(MAX_RUN_LENGTH),
            sign: Default::default(),
        }
    }
}

impl<N: NInt, R: Read, S: EncodingSign> GenericRle<N> for RleV2Decoder<N, R, S> {
    fn advance(&mut self, n: usize) {
        self.current_head += n;
    }

    fn available(&self) -> &[N] {
        &self.decoded_ints[self.current_head..]
    }

    fn decode_batch(&mut self) -> Result<()> {
        self.current_head = 0;
        self.decoded_ints.clear();
        let header = match try_read_u8(&mut self.reader)? {
            Some(byte) => byte,
            None => return Ok(()),
        };

        match EncodingType::from_header(header) {
            EncodingType::ShortRepeat => read_short_repeat_values::<_, _, S>(
                &mut self.reader,
                &mut self.decoded_ints,
                header,
            )?,
            EncodingType::Direct => {
                read_direct_values::<_, _, S>(&mut self.reader, &mut self.decoded_ints, header)?
            }
            EncodingType::PatchedBase => {
                read_patched_base::<_, _, S>(&mut self.reader, &mut self.decoded_ints, header)?
            }
            EncodingType::Delta => read_delta_values::<_, _, S>(
                &mut self.reader,
                &mut self.decoded_ints,
                &mut self.deltas,
                header,
            )?,
        }

        Ok(())
    }
}

struct DeltaEncodingCheckResult<N: NInt> {
    base_value: N,
    min: N,
    max: N,
    first_delta: i64,
    max_delta: i64,
    is_monotonic: bool,
    is_fixed_delta: bool,
    adjacent_deltas: Vec<i64>,
}

/// Calculate the necessary values to determine if sequence can be delta encoded.
fn delta_encoding_check<N: NInt>(literals: &[N]) -> DeltaEncodingCheckResult<N> {
    let base_value = literals[0];
    let mut min = base_value.min(literals[1]);
    let mut max = base_value.max(literals[1]);
    // Saturating should be fine here (and below) as we later check the
    // difference between min & max and defer to direct encoding if it
    // is too large (so the corrupt delta here won't actually be used).
    // TODO: is there a more explicit way of ensuring this behaviour?
    let first_delta = literals[1].as_i64().saturating_sub(base_value.as_i64());
    let mut current_delta;
    let mut max_delta = 0;

    let mut is_increasing = first_delta.is_positive();
    let mut is_decreasing = first_delta.is_negative();
    let mut is_fixed_delta = true;

    let mut adjacent_deltas = vec![];

    // We've already preprocessed the first step above
    for i in 2..literals.len() {
        let l1 = literals[i];
        let l0 = literals[i - 1];

        min = min.min(l1);
        max = max.max(l1);

        current_delta = l1.as_i64().saturating_sub(l0.as_i64());

        is_increasing &= current_delta >= 0;
        is_decreasing &= current_delta <= 0;

        is_fixed_delta &= current_delta == first_delta;
        let current_delta = current_delta.saturating_abs();
        adjacent_deltas.push(current_delta);
        max_delta = max_delta.max(current_delta);
    }
    let is_monotonic = is_increasing || is_decreasing;

    DeltaEncodingCheckResult {
        base_value,
        min,
        max,
        first_delta,
        max_delta,
        is_monotonic,
        is_fixed_delta,
        adjacent_deltas,
    }
}

/// Runs are guaranteed to have length > 1.
#[derive(Debug, Clone, Eq, PartialEq)]
enum RleV2EncodingState<N: NInt> {
    /// When buffer is empty and no values to encode.
    Empty,
    /// Special state for first value as we determine after the first
    /// value whether to go fixed or variable run.
    One(N),
    /// Run of identical value of specified count.
    FixedRun { value: N, count: usize },
    /// Run of variable values.
    VariableRun { literals: Vec<N> },
}

impl<N: NInt> Default for RleV2EncodingState<N> {
    fn default() -> Self {
        Self::Empty
    }
}

pub struct RleV2Encoder<N: NInt, S: EncodingSign> {
    /// Stores the run length encoded sequences.
    data: BytesMut,
    /// Used in state machine for determining which sub-encoding
    /// for a sequence to use.
    state: RleV2EncodingState<N>,
    phantom: PhantomData<S>,
}

impl<N: NInt, S: EncodingSign> RleV2Encoder<N, S> {
    // Algorithm adapted from:
    // https://github.com/apache/orc/blob/main/java/core/src/java/org/apache/orc/impl/RunLengthIntegerWriterV2.java

    /// Process each value to build up knowledge to determine which encoding to use. We attempt
    /// to identify runs of identical values (fixed runs), otherwise falling back to variable
    /// runs (varying values).
    ///
    /// When in a fixed run state, as long as identical values are found, we keep incrementing
    /// the run length up to a maximum of 512, flushing to fixed delta run if so. If we encounter
    /// a differing value, we flush to short repeat or fixed delta depending on the length and
    /// reset the state (if the current run is small enough, we switch direct to variable run).
    ///
    /// When in a variable run state, if we find 3 identical values in a row as the latest values,
    /// we flush the variable run to a sub-encoding then switch to fixed run, otherwise continue
    /// incrementing the run length up to a max length of 512, before flushing and resetting the
    /// state. For a variable run, extra logic must take place to determine which sub-encoding to
    /// use when flushing, see [`Self::determine_variable_run_encoding`] for more details.
    fn process_value(&mut self, value: N) {
        match &mut self.state {
            // When we start, or when a run was flushed to a sub-encoding
            RleV2EncodingState::Empty => {
                self.state = RleV2EncodingState::One(value);
            }
            // Here we determine if we look like we're in a fixed run or variable run
            RleV2EncodingState::One(one_value) => {
                if value == *one_value {
                    self.state = RleV2EncodingState::FixedRun { value, count: 2 };
                } else {
                    // TODO: alloc here
                    let mut literals = Vec::with_capacity(MAX_RUN_LENGTH);
                    literals.push(*one_value);
                    literals.push(value);
                    self.state = RleV2EncodingState::VariableRun { literals };
                }
            }
            // When we're in a run of identical values
            RleV2EncodingState::FixedRun {
                value: fixed_value,
                count,
            } => {
                if value == *fixed_value {
                    // Continue fixed run, flushing to delta when max length reached
                    *count += 1;
                    if *count == MAX_RUN_LENGTH {
                        write_fixed_delta::<_, S>(&mut self.data, value, 0, *count - 2);
                        self.state = RleV2EncodingState::Empty;
                    }
                } else {
                    // If fixed run is broken by a different value.
                    match count {
                        // Note that count cannot be 0 or 1 here as that is encoded
                        // by Empty and One states in self.state
                        2 => {
                            // If fixed run is smaller than short repeat then just include
                            // it at the start of the variable run we're switching to.
                            // TODO: alloc here
                            let mut literals = Vec::with_capacity(MAX_RUN_LENGTH);
                            literals.push(*fixed_value);
                            literals.push(*fixed_value);
                            literals.push(value);
                            self.state = RleV2EncodingState::VariableRun { literals };
                        }
                        SHORT_REPEAT_MIN_LENGTH..=SHORT_REPEAT_MAX_LENGTH => {
                            // If we have enough values for a Short Repeat, then encode as
                            // such.
                            write_short_repeat::<_, S>(&mut self.data, *fixed_value, *count);
                            self.state = RleV2EncodingState::One(value);
                        }
                        _ => {
                            // Otherwise if too large, use Delta encoding.
                            write_fixed_delta::<_, S>(&mut self.data, *fixed_value, 0, *count - 2);
                            self.state = RleV2EncodingState::One(value);
                        }
                    }
                }
            }
            // When we're in a run of varying values
            RleV2EncodingState::VariableRun { literals } => {
                let length = literals.len();
                let last_value = literals[length - 1];
                let second_last_value = literals[length - 2];
                if value == last_value && value == second_last_value {
                    // Last 3 values (including current new one) are identical. Break the current
                    // variable run, flushing it to a sub-encoding, then switch to a fixed run
                    // state.

                    // Pop off the last two values (which are identical to value) and flush
                    // the variable run to writer
                    literals.truncate(literals.len() - 2);
                    determine_variable_run_encoding::<_, S>(&mut self.data, literals);

                    self.state = RleV2EncodingState::FixedRun { value, count: 3 };
                } else {
                    // Continue variable run, flushing sub-encoding if max length reached
                    literals.push(value);
                    if literals.len() == MAX_RUN_LENGTH {
                        determine_variable_run_encoding::<_, S>(&mut self.data, literals);
                        self.state = RleV2EncodingState::Empty;
                    }
                }
            }
        }
    }

    /// Flush any buffered values to the writer.
    fn flush(&mut self) {
        let state = std::mem::take(&mut self.state);
        match state {
            RleV2EncodingState::Empty => {}
            RleV2EncodingState::One(value) => {
                let value = S::zigzag_encode(value);
                write_direct(&mut self.data, &[value], Some(value));
            }
            RleV2EncodingState::FixedRun { value, count: 2 } => {
                // Direct has smallest overhead
                let value = S::zigzag_encode(value);
                write_direct(&mut self.data, &[value, value], Some(value));
            }
            RleV2EncodingState::FixedRun { value, count } if count <= SHORT_REPEAT_MAX_LENGTH => {
                // Short repeat must have length [3, 10]
                write_short_repeat::<_, S>(&mut self.data, value, count);
            }
            RleV2EncodingState::FixedRun { value, count } => {
                write_fixed_delta::<_, S>(&mut self.data, value, 0, count - 2);
            }
            RleV2EncodingState::VariableRun { mut literals } => {
                determine_variable_run_encoding::<_, S>(&mut self.data, &mut literals);
            }
        }
    }
}

impl<N: NInt, S: EncodingSign> EstimateMemory for RleV2Encoder<N, S> {
    fn estimate_memory_size(&self) -> usize {
        self.data.len()
    }
}

impl<N: NInt, S: EncodingSign> PrimitiveValueEncoder<N> for RleV2Encoder<N, S> {
    fn new() -> Self {
        Self {
            data: BytesMut::new(),
            state: RleV2EncodingState::Empty,
            phantom: Default::default(),
        }
    }

    fn write_one(&mut self, value: N) {
        self.process_value(value);
    }

    fn take_inner(&mut self) -> bytes::Bytes {
        self.flush();
        std::mem::take(&mut self.data).into()
    }
}

fn determine_variable_run_encoding<N: NInt, S: EncodingSign>(
    writer: &mut BytesMut,
    literals: &mut [N],
) {
    // Direct will have smallest overhead for tiny runs
    if literals.len() <= SHORT_REPEAT_MIN_LENGTH {
        for v in literals.iter_mut() {
            *v = S::zigzag_encode(*v);
        }
        write_direct(writer, literals, None);
        return;
    }

    // Invariant: literals.len() > 3
    let DeltaEncodingCheckResult {
        base_value,
        min,
        max,
        first_delta,
        max_delta,
        is_monotonic,
        is_fixed_delta,
        adjacent_deltas,
    } = delta_encoding_check(literals);

    // Quick check for delta overflow, if so just move to Direct as it has less
    // overhead than Patched Base.
    // TODO: should min/max be N or i64 here?
    if max.checked_sub(&min).is_none() {
        for v in literals.iter_mut() {
            *v = S::zigzag_encode(*v);
        }
        write_direct(writer, literals, None);
        return;
    }

    // Any subtractions here on are safe due to above check

    if is_fixed_delta {
        write_fixed_delta::<_, S>(writer, literals[0], first_delta, literals.len() - 2);
        return;
    }

    // First delta used to indicate if increasing or decreasing, so must be non-zero
    if first_delta != 0 && is_monotonic {
        write_varying_delta::<_, S>(writer, base_value, first_delta, max_delta, &adjacent_deltas);
        return;
    }

    // In Java implementation, Patched Base encoding base value cannot exceed 56
    // bits in value otherwise it can overflow the maximum 8 bytes used to encode
    // the value when signed MSB encoding is used (adds an extra bit).
    let min = min.as_i64();
    if min.abs() >= BASE_VALUE_LIMIT && min != i64::MIN {
        for v in literals.iter_mut() {
            *v = S::zigzag_encode(*v);
        }
        write_direct(writer, literals, None);
        return;
    }

    // TODO: another allocation here
    let zigzag_literals = literals
        .iter()
        .map(|&v| S::zigzag_encode(v))
        .collect::<Vec<_>>();
    let zigzagged_90_percentile_bit_width = calculate_percentile_bits(&zigzag_literals, 0.90);
    // TODO: can derive from min/max?
    let zigzagged_100_percentile_bit_width = calculate_percentile_bits(&zigzag_literals, 1.00);
    // If variation of bit width between largest value and lower 90% of values isn't
    // significant enough, just use direct encoding as patched base wouldn't be as
    // efficient.
    if (zigzagged_100_percentile_bit_width.saturating_sub(zigzagged_90_percentile_bit_width)) <= 1 {
        // TODO: pass through the 100p here
        write_direct(writer, &zigzag_literals, None);
        return;
    }

    // Base value for patched base is the minimum value
    // Patch data values are the literals with the base value subtracted
    // We use base_reduced_literals to store these base reduced literals
    let mut max_data_value = 0;
    let mut base_reduced_literals = vec![];
    for l in literals.iter() {
        // All base reduced literals become positive here
        let base_reduced_literal = l.as_i64() - min;
        base_reduced_literals.push(base_reduced_literal);
        max_data_value = max_data_value.max(base_reduced_literal);
    }

    // Aka 100th percentile
    let base_reduced_literals_max_bit_width = max_data_value.closest_aligned_bit_width();
    // 95th percentile width is used to find the 5% of values to encode with patches
    let base_reduced_literals_95th_percentile_bit_width =
        calculate_percentile_bits(&base_reduced_literals, 0.95);

    // Patch only if we have outliers, based on bit width
    if base_reduced_literals_max_bit_width != base_reduced_literals_95th_percentile_bit_width {
        write_patched_base(
            writer,
            &mut base_reduced_literals,
            min,
            base_reduced_literals_max_bit_width,
            base_reduced_literals_95th_percentile_bit_width,
        );
    } else {
        // TODO: pass through the 100p here
        write_direct(writer, &zigzag_literals, None);
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use proptest::prelude::*;

    use crate::encoding::{
        integer::{SignedEncoding, UnsignedEncoding},
        PrimitiveValueDecoder,
    };

    use super::*;

    // TODO: have tests varying the out buffer, to ensure decode() is called
    //       multiple times

    fn test_helper<S: EncodingSign>(data: &[u8], expected: &[i64]) {
        let mut reader = RleV2Decoder::<i64, _, S>::new(Cursor::new(data));
        let mut actual = vec![0; expected.len()];
        reader.decode(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn reader_test() {
        let data = [2, 1, 64, 5, 80, 1, 1];
        let expected = [1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 1];
        test_helper::<UnsignedEncoding>(&data, &expected);

        // direct
        let data = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];
        let expected = [23713, 43806, 57005, 48879];
        test_helper::<UnsignedEncoding>(&data, &expected);

        // patched base
        let data = [
            102, 9, 0, 126, 224, 7, 208, 0, 126, 79, 66, 64, 0, 127, 128, 8, 2, 0, 128, 192, 8, 22,
            0, 130, 0, 8, 42,
        ];
        let expected = [
            2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090,
        ];
        test_helper::<UnsignedEncoding>(&data, &expected);

        let data = [196, 9, 2, 2, 74, 40, 166];
        let expected = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29];
        test_helper::<UnsignedEncoding>(&data, &expected);

        let data = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];
        let expected = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29];
        test_helper::<UnsignedEncoding>(&data, &expected);

        let data = [7, 1];
        let expected = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1];
        test_helper::<UnsignedEncoding>(&data, &expected);
    }

    #[test]
    fn short_repeat() {
        let data = [0x0a, 0x27, 0x10];
        let expected = [10000, 10000, 10000, 10000, 10000];
        test_helper::<UnsignedEncoding>(&data, &expected);
    }

    #[test]
    fn direct() {
        let data = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];
        let expected = [23713, 43806, 57005, 48879];
        test_helper::<UnsignedEncoding>(&data, &expected);
    }

    #[test]
    fn direct_signed() {
        let data = [110, 3, 0, 185, 66, 1, 86, 60, 1, 189, 90, 1, 125, 222];
        let expected = [23713, 43806, 57005, 48879];
        test_helper::<SignedEncoding>(&data, &expected);
    }

    #[test]
    fn delta() {
        let data = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];
        let expected = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29];
        test_helper::<UnsignedEncoding>(&data, &expected);
    }

    #[test]
    fn patched_base() {
        let data = [
            0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46,
            0x50, 0x5a, 0xfc, 0xe8,
        ];
        let expected = [
            2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090,
        ];
        test_helper::<UnsignedEncoding>(&data, &expected);
    }

    #[test]
    fn patched_base_1() {
        let data = vec![
            144, 109, 4, 164, 141, 16, 131, 194, 0, 240, 112, 64, 60, 84, 24, 3, 193, 201, 128,
            120, 60, 33, 4, 244, 3, 193, 192, 224, 128, 56, 32, 15, 22, 131, 129, 225, 0, 112, 84,
            86, 14, 8, 106, 193, 192, 228, 160, 64, 32, 14, 213, 131, 193, 192, 240, 121, 124, 30,
            18, 9, 132, 67, 0, 224, 120, 60, 28, 14, 32, 132, 65, 192, 240, 160, 56, 61, 91, 7, 3,
            193, 192, 240, 120, 76, 29, 23, 7, 3, 220, 192, 240, 152, 60, 52, 15, 7, 131, 129, 225,
            0, 144, 56, 30, 14, 44, 140, 129, 194, 224, 120, 0, 28, 15, 8, 6, 129, 198, 144, 128,
            104, 36, 27, 11, 38, 131, 33, 48, 224, 152, 60, 111, 6, 183, 3, 112, 0, 1, 78, 5, 46,
            2, 1, 1, 141, 3, 1, 1, 138, 22, 0, 65, 1, 4, 0, 225, 16, 209, 192, 4, 16, 8, 36, 16, 3,
            48, 1, 3, 13, 33, 0, 176, 0, 1, 94, 18, 0, 68, 0, 33, 1, 143, 0, 1, 7, 93, 0, 25, 0, 5,
            0, 2, 0, 4, 0, 1, 0, 1, 0, 2, 0, 16, 0, 1, 11, 150, 0, 3, 0, 1, 0, 1, 99, 157, 0, 1,
            140, 54, 0, 162, 1, 130, 0, 16, 112, 67, 66, 0, 2, 4, 0, 0, 224, 0, 1, 0, 16, 64, 16,
            91, 198, 1, 2, 0, 32, 144, 64, 0, 12, 2, 8, 24, 0, 64, 0, 1, 0, 0, 8, 48, 51, 128, 0,
            2, 12, 16, 32, 32, 71, 128, 19, 76,
        ];
        // expected data generated from Orc Java implementation
        let expected = vec![
            20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2, 3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32,
            1, 2, 3, 1, 8, 30, 1, 3, 414, 1, 1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11,
            1, 2, 2, 1, 1, 52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
            2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -13, 1, 2, 3, 13, 1, 92, 3,
            13, 5, 14, 9, 141, 12, 6, 15, 25, -1, -1, -1, 23, 1, -1, -1, -71, -2, -1, -1, -1, -1,
            2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4, 13, 1, 5, 4, 4, 19, 5, -1, -1, -1,
            34, -17, -200, -1, -943, -13, -3, 1, 2, -1, -1, 1, 8, -1, 1483, -2, -1, -1, -12751, -1,
            -1, -1, 66, 1, 3, 8, 131, 14, 5, 1, 2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2,
            2, 1, 5, 10, 3, 1, 1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3,
            2, 2, 16,
        ];
        test_helper::<SignedEncoding>(&data, &expected);
    }

    // TODO: be smarter about prop test here, generate different patterns of ints
    //        - e.g. increasing/decreasing sequences, outliers, repeated
    //        - to ensure all different subencodings are being used (and might make shrinking better)
    //       currently 99% of the time here the subencoding will be Direct due to random generation

    fn roundtrip_helper<N: NInt, S: EncodingSign>(values: &[N]) -> Result<Vec<N>> {
        let mut writer = RleV2Encoder::<N, S>::new();
        writer.write_slice(values);
        let data = writer.take_inner();

        let mut reader = RleV2Decoder::<N, _, S>::new(Cursor::new(data));
        let mut actual = vec![N::zero(); values.len()];
        reader.decode(&mut actual).unwrap();

        Ok(actual)
    }

    proptest! {
        #[test]
        fn roundtrip_i16(values in prop::collection::vec(any::<i16>(), 1..1_000)) {
            let out = roundtrip_helper::<_, SignedEncoding>(&values)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_i32(values in prop::collection::vec(any::<i32>(), 1..1_000)) {
            let out = roundtrip_helper::<_, SignedEncoding>(&values)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_i64(values in prop::collection::vec(any::<i64>(), 1..1_000)) {
            let out = roundtrip_helper::<_, SignedEncoding>(&values)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_i64_unsigned(values in prop::collection::vec(0..=i64::MAX, 1..1_000)) {
            let out = roundtrip_helper::<_, UnsignedEncoding>(&values)?;
            prop_assert_eq!(out, values);
        }
    }
}
