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

use std::io::Read;

use bytes::{BufMut, BytesMut};
use snafu::OptionExt;

use crate::{
    encoding::{
        integer::{
            rle_v2::{EncodingType, MAX_RUN_LENGTH},
            util::{
                extract_run_length_from_header, read_ints, read_varint_zigzagged,
                rle_v2_decode_bit_width, rle_v2_encode_bit_width, write_aligned_packed_ints,
                write_varint_zigzagged,
            },
            EncodingSign, SignedEncoding, VarintSerde,
        },
        util::read_u8,
    },
    error::{OrcError, OutOfSpecSnafu, Result},
};

use super::NInt;

/// We use i64 and u64 for delta to make things easier and to avoid edge cases,
/// as for example for i16, the delta may be too large to represent in an i16.
// TODO: expand on the above
pub fn read_delta_values<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    deltas: &mut Vec<i64>,
    header: u8,
) -> Result<()> {
    // Encoding format:
    // 2 bytes header
    //   - 2 bits for encoding type (constant 3)
    //   - 5 bits for encoded delta bitwidth (0 to 64)
    //   - 9 bits for run length (1 to 512)
    // Base value (signed or unsigned) varint
    // Delta value signed varint
    // Sequence of delta values

    let encoded_delta_bit_width = (header >> 1) & 0x1f;
    // Uses same encoding table as for direct & patched base,
    // but special case where 0 indicates 0 width (for fixed delta)
    let delta_bit_width = if encoded_delta_bit_width == 0 {
        encoded_delta_bit_width as usize
    } else {
        rle_v2_decode_bit_width(encoded_delta_bit_width)
    };

    let second_byte = read_u8(reader)?;
    let length = extract_run_length_from_header(header, second_byte);

    let base_value = read_varint_zigzagged::<N, _, S>(reader)?;
    out_ints.push(base_value);

    // Always signed since can be decreasing sequence
    let delta_base = read_varint_zigzagged::<i64, _, SignedEncoding>(reader)?;
    // TODO: does this get inlined?
    let op: fn(N, i64) -> Option<N> = if delta_base.is_positive() {
        |acc, delta| acc.add_i64(delta)
    } else {
        |acc, delta| acc.sub_i64(delta)
    };
    let delta_base = delta_base.abs(); // TODO: i64::MIN?

    if delta_bit_width == 0 {
        // If width is 0 then all values have fixed delta of delta_base
        // Skip first value since that's base_value
        (1..length).try_fold(base_value, |acc, _| {
            let acc = op(acc, delta_base).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding delta integer",
            })?;
            out_ints.push(acc);
            Ok::<_, OrcError>(acc)
        })?;
    } else {
        deltas.clear();
        // Add delta base and first value
        let second_value = op(base_value, delta_base).context(OutOfSpecSnafu {
            msg: "over/underflow when decoding delta integer",
        })?;
        out_ints.push(second_value);
        // Run length includes base value and first delta, so skip them
        let length = length - 2;

        // Unpack the delta values
        read_ints(deltas, length, delta_bit_width, reader)?;
        let mut acc = second_value;
        // Each element is the delta, so find actual value using running accumulator
        for delta in deltas {
            acc = op(acc, *delta).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding delta integer",
            })?;
            out_ints.push(acc);
        }
    }
    Ok(())
}

pub fn write_varying_delta<N: NInt, S: EncodingSign>(
    writer: &mut BytesMut,
    base_value: N,
    first_delta: i64,
    max_delta: i64,
    subsequent_deltas: &[i64],
) {
    debug_assert!(
        max_delta > 0,
        "varying deltas must have at least one non-zero delta"
    );
    let bit_width = max_delta.closest_aligned_bit_width();
    // We can't have bit width of 1 for delta as that would get decoded as
    // 0 bit width on reader, which indicates fixed delta, so bump 1 to 2
    // in this case.
    let bit_width = if bit_width == 1 { 2 } else { bit_width };
    // Add 2 to len for the base_value and first_delta
    let header = derive_delta_header(bit_width, subsequent_deltas.len() + 2);
    writer.put_slice(&header);

    write_varint_zigzagged::<_, S>(writer, base_value);
    // First delta always signed to indicate increasing/decreasing sequence
    write_varint_zigzagged::<_, SignedEncoding>(writer, first_delta);

    // Bitpacked deltas
    write_aligned_packed_ints(writer, bit_width, subsequent_deltas);
}

pub fn write_fixed_delta<N: NInt, S: EncodingSign>(
    writer: &mut BytesMut,
    base_value: N,
    fixed_delta: i64,
    subsequent_deltas_len: usize,
) {
    // Assuming len excludes base_value and first delta
    let header = derive_delta_header(0, subsequent_deltas_len + 2);
    writer.put_slice(&header);

    write_varint_zigzagged::<_, S>(writer, base_value);
    // First delta always signed to indicate increasing/decreasing sequence
    write_varint_zigzagged::<_, SignedEncoding>(writer, fixed_delta);
}

fn derive_delta_header(delta_width: usize, run_length: usize) -> [u8; 2] {
    debug_assert!(
        (1..=MAX_RUN_LENGTH).contains(&run_length),
        "delta run length cannot exceed 512 values"
    );
    // [1, 512] to [0, 511]
    let run_length = run_length as u16 - 1;
    // 0 is special value to indicate fixed delta
    let delta_width = if delta_width == 0 {
        0
    } else {
        rle_v2_encode_bit_width(delta_width)
    };
    // No need to mask as we guarantee max length is 512
    let encoded_length_high_bit = (run_length >> 8) as u8;
    let encoded_length_low_bits = (run_length & 0xFF) as u8;

    let header1 = EncodingType::Delta.to_header() | delta_width << 1 | encoded_length_high_bit;
    let header2 = encoded_length_low_bits;

    [header1, header2]
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::encoding::integer::UnsignedEncoding;

    use super::*;

    // TODO: figure out how to write proptests for these

    #[test]
    fn test_fixed_delta_positive() {
        let mut buf = BytesMut::new();
        let mut out = vec![];
        let mut deltas = vec![];
        write_fixed_delta::<i64, UnsignedEncoding>(&mut buf, 0, 10, 100 - 2);
        let header = buf[0];
        read_delta_values::<i64, _, UnsignedEncoding>(
            &mut Cursor::new(&buf[1..]),
            &mut out,
            &mut deltas,
            header,
        )
        .unwrap();

        let expected = (0..100).map(|i| i * 10).collect::<Vec<i64>>();
        assert_eq!(expected, out);
    }

    #[test]
    fn test_fixed_delta_negative() {
        let mut buf = BytesMut::new();
        let mut out = vec![];
        let mut deltas = vec![];
        write_fixed_delta::<i64, UnsignedEncoding>(&mut buf, 10_000, -63, 150 - 2);
        let header = buf[0];
        read_delta_values::<i64, _, UnsignedEncoding>(
            &mut Cursor::new(&buf[1..]),
            &mut out,
            &mut deltas,
            header,
        )
        .unwrap();

        let expected = (0..150).map(|i| 10_000 - i * 63).collect::<Vec<i64>>();
        assert_eq!(expected, out);
    }

    #[test]
    fn test_varying_delta_positive() {
        let deltas = [
            1, 6, 98, 12, 65, 9, 0, 0, 1, 128, 643, 129, 469, 123, 4572, 124,
        ];
        let max = *deltas.iter().max().unwrap();

        let mut buf = BytesMut::new();
        let mut out = vec![];
        let mut deltas = vec![];
        write_varying_delta::<i64, UnsignedEncoding>(&mut buf, 0, 10, max, &deltas);
        let header = buf[0];
        read_delta_values::<i64, _, UnsignedEncoding>(
            &mut Cursor::new(&buf[1..]),
            &mut out,
            &mut deltas,
            header,
        )
        .unwrap();

        let mut expected = vec![0, 10];
        let mut i = 1;
        for d in deltas {
            expected.push(d + expected[i]);
            i += 1;
        }
        assert_eq!(expected, out);
    }

    #[test]
    fn test_varying_delta_negative() {
        let deltas = [
            1, 6, 98, 12, 65, 9, 0, 0, 1, 128, 643, 129, 469, 123, 4572, 124,
        ];
        let max = *deltas.iter().max().unwrap();

        let mut buf = BytesMut::new();
        let mut out = vec![];
        let mut deltas = vec![];
        write_varying_delta::<i64, UnsignedEncoding>(&mut buf, 10_000, -1, max, &deltas);
        let header = buf[0];
        read_delta_values::<i64, _, UnsignedEncoding>(
            &mut Cursor::new(&buf[1..]),
            &mut out,
            &mut deltas,
            header,
        )
        .unwrap();

        let mut expected = vec![10_000, 9_999];
        let mut i = 1;
        for d in deltas {
            expected.push(expected[i] - d);
            i += 1;
        }
        assert_eq!(expected, out);
    }
}
