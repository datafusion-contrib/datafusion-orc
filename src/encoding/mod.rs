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

//! Encoding/decoding logic for writing/reading primitive values from ORC types.

use arrow::buffer::NullBuffer;
use bytes::Bytes;

use crate::{error::Result, memory::EstimateMemory};

pub mod boolean;
pub mod byte;
pub mod decimal;
pub mod float;
pub mod integer;
mod rle;
pub mod timestamp;
mod util;

/// Encodes primitive values into an internal buffer, usually with a specialized run length
/// encoding for better compression.
pub trait PrimitiveValueEncoder<V>: EstimateMemory
where
    V: Copy,
{
    fn new() -> Self;

    fn write_one(&mut self, value: V);

    fn write_slice(&mut self, values: &[V]) {
        for &value in values {
            self.write_one(value);
        }
    }

    /// Take the encoded bytes, replacing it with an empty buffer.
    // TODO: Figure out how to retain the allocation instead of handing
    //       it off each time.
    fn take_inner(&mut self) -> Bytes;
}

pub trait PrimitiveValueDecoder<V> {
    /// Decode out.len() values into out at a time, failing if it cannot fill
    /// the buffer.
    fn decode(&mut self, out: &mut [V]) -> Result<()>;

    /// Decode into `out` according to the `true` elements in `present`.
    ///
    /// `present` must be the same length as `out`.
    fn decode_spaced(&mut self, out: &mut [V], present: &NullBuffer) -> Result<()> {
        debug_assert_eq!(out.len(), present.len());

        // First get all the non-null values into a contiguous range.
        let non_null_count = present.len() - present.null_count();
        if non_null_count == 0 {
            // All nulls, don't bother decoding anything
            return Ok(());
        }
        // We read into the back because valid_indices() below is not reversible,
        // so we just reverse our algorithm.
        let range_start = out.len() - non_null_count;
        self.decode(&mut out[range_start..])?;
        if non_null_count == present.len() {
            // No nulls, don't need to space out
            return Ok(());
        }

        // From the head of the contiguous range (at the end of the buffer) we swap
        // with the null elements to ensure it matches with the present buffer.
        let head_indices = range_start..out.len();
        for (correct_index, head_index) in present.valid_indices().zip(head_indices) {
            // head_index points to the value we need to move to correct_index
            out.swap(correct_index, head_index);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Emits numbers increasing from 0.
    struct DummyDecoder;

    impl PrimitiveValueDecoder<i32> for DummyDecoder {
        fn decode(&mut self, out: &mut [i32]) -> Result<()> {
            let values = (0..out.len()).map(|x| x as i32).collect::<Vec<_>>();
            out.copy_from_slice(&values);
            Ok(())
        }
    }

    fn gen_spaced_dummy_decoder_expected(present: &[bool]) -> Vec<i32> {
        let mut value = 0;
        let mut expected = vec![];
        for &is_present in present {
            if is_present {
                expected.push(value);
                value += 1;
            } else {
                expected.push(-1);
            }
        }
        expected
    }

    proptest! {
        #[test]
        fn decode_spaced_proptest(present: Vec<bool>) {
            let mut decoder = DummyDecoder;
            let mut out = vec![-1; present.len()];
            decoder.decode_spaced(&mut out, &NullBuffer::from(present.clone())).unwrap();
            let expected = gen_spaced_dummy_decoder_expected(&present);
            prop_assert_eq!(out, expected);
        }
    }

    #[test]
    fn decode_spaced_edge_cases() {
        let mut decoder = DummyDecoder;
        let len = 10;

        // all present
        let mut out = vec![-1; len];
        let present = vec![true; len];
        let present = NullBuffer::from(present);
        decoder.decode_spaced(&mut out, &present).unwrap();
        let expected: Vec<_> = (0..len).map(|i| i as i32).collect();
        assert_eq!(out, expected);

        // all null
        let mut out = vec![-1; len];
        let present = vec![false; len];
        let present = NullBuffer::from(present);
        decoder.decode_spaced(&mut out, &present).unwrap();
        let expected = vec![-1; len];
        assert_eq!(out, expected);
    }
}
