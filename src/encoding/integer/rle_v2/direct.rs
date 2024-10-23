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

use crate::{
    encoding::{
        integer::{
            rle_v2::{EncodingType, MAX_RUN_LENGTH},
            util::{
                extract_run_length_from_header, read_ints, rle_v2_decode_bit_width,
                rle_v2_encode_bit_width, write_aligned_packed_ints,
            },
            EncodingSign,
        },
        util::read_u8,
    },
    error::{OutOfSpecSnafu, Result},
};

use super::NInt;

pub fn read_direct_values<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    header: u8,
) -> Result<()> {
    let encoded_bit_width = (header >> 1) & 0x1F;
    let bit_width = rle_v2_decode_bit_width(encoded_bit_width);

    if (N::BYTE_SIZE * 8) < bit_width {
        return OutOfSpecSnafu {
            msg: "byte width of direct encoding exceeds byte size of integer being decoded to",
        }
        .fail();
    }

    let second_byte = read_u8(reader)?;
    let length = extract_run_length_from_header(header, second_byte);

    // Write the unpacked values and zigzag decode to result buffer
    read_ints(out_ints, length, bit_width, reader)?;

    for lit in out_ints.iter_mut() {
        *lit = S::zigzag_decode(*lit);
    }

    Ok(())
}

/// `values` and `max` must be zigzag encoded. If `max` is not provided, it is derived
/// by iterating over `values`.
pub fn write_direct<N: NInt>(writer: &mut BytesMut, values: &[N], max: Option<N>) {
    debug_assert!(
        (1..=MAX_RUN_LENGTH).contains(&values.len()),
        "direct run length cannot exceed 512 values"
    );

    let max = max.unwrap_or_else(|| {
        // Assert guards that values is non-empty
        *values.iter().max_by_key(|x| x.bits_used()).unwrap()
    });

    let bit_width = max.closest_aligned_bit_width();
    let encoded_bit_width = rle_v2_encode_bit_width(bit_width);
    // From [1, 512] to [0, 511]
    let encoded_length = values.len() as u16 - 1;
    // No need to mask as we guarantee max length is 512
    let encoded_length_high_bit = (encoded_length >> 8) as u8;
    let encoded_length_low_bits = (encoded_length & 0xFF) as u8;

    let header1 =
        EncodingType::Direct.to_header() | (encoded_bit_width << 1) | encoded_length_high_bit;
    let header2 = encoded_length_low_bits;

    writer.put_u8(header1);
    writer.put_u8(header2);
    write_aligned_packed_ints(writer, bit_width, values);
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use proptest::prelude::*;

    use crate::encoding::integer::{SignedEncoding, UnsignedEncoding};

    use super::*;

    fn roundtrip_direct_helper<N: NInt, S: EncodingSign>(values: &[N]) -> Result<Vec<N>> {
        let mut buf = BytesMut::new();
        let mut out = vec![];

        write_direct(&mut buf, values, None);
        let header = buf[0];
        read_direct_values::<_, _, S>(&mut Cursor::new(&buf[1..]), &mut out, header)?;

        Ok(out)
    }

    #[test]
    fn test_direct_edge_case() {
        let values: Vec<i16> = vec![109, -17809, -29946, -17285];
        let encoded = values
            .iter()
            .map(|&v| SignedEncoding::zigzag_encode(v))
            .collect::<Vec<_>>();
        let out = roundtrip_direct_helper::<_, SignedEncoding>(&encoded).unwrap();
        assert_eq!(out, values);
    }

    proptest! {
        #[test]
        fn roundtrip_direct_i16(values in prop::collection::vec(any::<i16>(), 1..=512)) {
            let encoded = values.iter().map(|v| SignedEncoding::zigzag_encode(*v)).collect::<Vec<_>>();
            let out = roundtrip_direct_helper::<_, SignedEncoding>(&encoded)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_direct_i32(values in prop::collection::vec(any::<i32>(), 1..=512)) {
            let encoded = values.iter().map(|v| SignedEncoding::zigzag_encode(*v)).collect::<Vec<_>>();
            let out = roundtrip_direct_helper::<_, SignedEncoding>(&encoded)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_direct_i64(values in prop::collection::vec(any::<i64>(), 1..=512)) {
            let encoded = values.iter().map(|v| SignedEncoding::zigzag_encode(*v)).collect::<Vec<_>>();
            let out = roundtrip_direct_helper::<_, SignedEncoding>(&encoded)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_direct_i64_unsigned(values in prop::collection::vec(0..=i64::MAX, 1..=512)) {
            let encoded = values.iter().map(|v| UnsignedEncoding::zigzag_encode(*v)).collect::<Vec<_>>();
            let out = roundtrip_direct_helper::<_, UnsignedEncoding>(&encoded)?;
            prop_assert_eq!(out, values);
        }
    }
}
