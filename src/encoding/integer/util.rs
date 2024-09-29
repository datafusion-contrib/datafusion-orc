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
    encoding::{util::read_u8, NInt, VarintSerde},
    error::Result,
};

/// Extracting run length from first two header bytes.
///
/// Run length encoded as range [0, 511], so adjust to actual
/// value in range [1, 512].
///
/// Used for patched base, delta, and direct sub-encodings.
pub fn extract_run_length_from_header(first_byte: u8, second_byte: u8) -> usize {
    let length = ((first_byte as u16 & 0x01) << 8) | (second_byte as u16);
    (length + 1) as usize
}

/// Read bitpacked integers into provided buffer. `bit_size` can be any value from 1 to 64,
/// inclusive.
pub fn read_ints<N: NInt>(
    buffer: &mut Vec<N>,
    expected_no_of_ints: usize,
    bit_size: usize,
    r: &mut impl Read,
) -> Result<()> {
    debug_assert!(
        (1..=64).contains(&bit_size),
        "bit_size must be in range [1, 64]"
    );
    match bit_size {
        1 => unrolled_unpack_1(buffer, expected_no_of_ints, r),
        2 => unrolled_unpack_2(buffer, expected_no_of_ints, r),
        4 => unrolled_unpack_4(buffer, expected_no_of_ints, r),
        n if n % 8 == 0 => unrolled_unpack_byte_aligned(buffer, expected_no_of_ints, r, n / 8),
        n => unrolled_unpack_unaligned(buffer, expected_no_of_ints, r, n),
    }
}

/// Decode numbers with bit width of 1 from read stream
fn unrolled_unpack_1<N: NInt>(
    buffer: &mut Vec<N>,
    expected_num_of_ints: usize,
    reader: &mut impl Read,
) -> Result<()> {
    for _ in 0..(expected_num_of_ints / 8) {
        let byte = read_u8(reader)?;
        let nums = [
            (byte >> 7) & 1,
            (byte >> 6) & 1,
            (byte >> 5) & 1,
            (byte >> 4) & 1,
            (byte >> 3) & 1,
            (byte >> 2) & 1,
            (byte >> 1) & 1,
            byte & 1,
        ];
        buffer.extend(nums.map(N::from_u8));
    }

    // Less than full byte at end, extract these trailing numbers
    let remainder = expected_num_of_ints % 8;
    if remainder > 0 {
        let byte = read_u8(reader)?;
        for i in 0..remainder {
            let shift = 7 - i;
            let n = N::from_u8((byte >> shift) & 1);
            buffer.push(n);
        }
    }

    Ok(())
}

/// Decode numbers with bit width of 2 from read stream
fn unrolled_unpack_2<N: NInt>(
    buffer: &mut Vec<N>,
    expected_num_of_ints: usize,
    reader: &mut impl Read,
) -> Result<()> {
    for _ in 0..(expected_num_of_ints / 4) {
        let byte = read_u8(reader)?;
        let nums = [(byte >> 6) & 3, (byte >> 4) & 3, (byte >> 2) & 3, byte & 3];
        buffer.extend(nums.map(N::from_u8));
    }

    // Less than full byte at end, extract these trailing numbers
    let remainder = expected_num_of_ints % 4;
    if remainder > 0 {
        let byte = read_u8(reader)?;
        for i in 0..remainder {
            let shift = 6 - (i * 2);
            let n = N::from_u8((byte >> shift) & 3);
            buffer.push(n);
        }
    }

    Ok(())
}

/// Decode numbers with bit width of 4 from read stream
fn unrolled_unpack_4<N: NInt>(
    buffer: &mut Vec<N>,
    expected_num_of_ints: usize,
    reader: &mut impl Read,
) -> Result<()> {
    for _ in 0..(expected_num_of_ints / 2) {
        let byte = read_u8(reader)?;
        let nums = [(byte >> 4) & 15, byte & 15];
        buffer.extend(nums.map(N::from_u8));
    }

    // At worst have 1 trailing 4-bit number
    let remainder = expected_num_of_ints % 2;
    if remainder > 0 {
        let byte = read_u8(reader)?;
        let n = N::from_u8((byte >> 4) & 15);
        buffer.push(n);
    }

    Ok(())
}

/// When the bitpacked integers have a width that isn't byte aligned
fn unrolled_unpack_unaligned<N: NInt>(
    buffer: &mut Vec<N>,
    expected_num_of_ints: usize,
    reader: &mut impl Read,
    bit_size: usize,
) -> Result<()> {
    debug_assert!(
        bit_size <= (N::BYTE_SIZE * 8),
        "bit_size cannot exceed size of N"
    );

    let mut bits_left = 0;
    let mut current_bits = N::zero();
    for _ in 0..expected_num_of_ints {
        let mut result = N::zero();
        let mut bits_left_to_read = bit_size;

        // No bounds check as we rely on caller doing this check
        // (since we know bit_size in advance)

        // bits_left_to_read and bits_left can never exceed 8
        // So safe to convert either to N
        while bits_left_to_read > bits_left {
            // TODO: explain this logic a bit
            result <<= bits_left;
            let mask = ((1_u16 << bits_left) - 1) as u8;
            let mask = N::from_u8(mask);
            result |= current_bits & mask;
            bits_left_to_read -= bits_left;

            let byte = read_u8(reader)?;
            current_bits = N::from_u8(byte);

            bits_left = 8;
        }

        if bits_left_to_read > 0 {
            result <<= bits_left_to_read;
            bits_left -= bits_left_to_read;
            let bits = current_bits >> bits_left;
            let mask = ((1_u16 << bits_left_to_read) - 1) as u8;
            let mask = N::from_u8(mask);
            result |= bits & mask;
        }

        buffer.push(result);
    }

    Ok(())
}

/// Decode bitpacked integers which are byte aligned
#[inline]
fn unrolled_unpack_byte_aligned<N: NInt>(
    buffer: &mut Vec<N>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
    num_bytes: usize,
) -> Result<()> {
    debug_assert!(
        num_bytes <= N::BYTE_SIZE,
        "num_bytes cannot exceed size of integer being decoded into"
    );
    // TODO: can probably read direct into buffer? read_big_endian() decodes
    //       into an intermediary buffer.
    for _ in 0..expected_num_of_ints {
        let num = N::read_big_endian(r, num_bytes)?;
        buffer.push(num);
    }
    Ok(())
}

/// Write bit packed integers, where we expect the `bit_width` to be aligned
/// by [`get_closest_aligned_bit_width`], and we write the bytes as big endian.
pub fn write_aligned_packed_ints<N: NInt>(writer: &mut BytesMut, bit_width: usize, values: &[N]) {
    debug_assert!(
        bit_width == 1 || bit_width == 2 || bit_width == 4 || bit_width % 8 == 0,
        "bit_width must be 1, 2, 4 or a multiple of 8"
    );
    match bit_width {
        1 => unrolled_pack_1(writer, values),
        2 => unrolled_pack_2(writer, values),
        4 => unrolled_pack_4(writer, values),
        n => unrolled_pack_bytes(writer, n / 8, values),
    }
}

/// Similar to [`write_aligned_packed_ints`] but the `bit_width` allows any value
/// in the range `[1, 64]`.
pub fn write_packed_ints<N: NInt>(writer: &mut BytesMut, bit_width: usize, values: &[N]) {
    debug_assert!(
        (1..=64).contains(&bit_width),
        "bit_width must be in the range [1, 64]"
    );
    if bit_width == 1 || bit_width == 2 || bit_width == 4 || bit_width % 8 == 0 {
        write_aligned_packed_ints(writer, bit_width, values);
    } else {
        write_unaligned_packed_ints(writer, bit_width, values)
    }
}

fn write_unaligned_packed_ints<N: NInt>(writer: &mut BytesMut, bit_width: usize, values: &[N]) {
    debug_assert!(
        (1..=64).contains(&bit_width),
        "bit_width must be in the range [1, 64]"
    );
    let mut bits_left = 8;
    let mut current_byte = 0;
    for &value in values {
        let mut bits_to_write = bit_width;
        // This loop will write 8 bits at a time into current_byte, except for the
        // first iteration after a previous value has been written. The previous
        // value may have bits left over, still in current_byte, which is represented
        // by 8 - bits_left (aka bits_left is the amount of space left in current_byte).
        while bits_to_write > bits_left {
            // Writing from most significant bits first.
            let shift = bits_to_write - bits_left;
            // Shift so bits to write are in least significant 8 bits.
            // Masking out higher bits so conversion to u8 is safe.
            let bits = value.unsigned_shr(shift as u32) & N::from_u8(0xFF);
            current_byte |= bits.to_u8().unwrap();
            bits_to_write -= bits_left;

            writer.put_u8(current_byte);
            current_byte = 0;
            bits_left = 8;
        }

        // If there are trailing bits then include these into current_byte.
        bits_left -= bits_to_write;
        let bits = (value << bits_left) & N::from_u8(0xFF);
        current_byte |= bits.to_u8().unwrap();

        if bits_left == 0 {
            writer.put_u8(current_byte);
            current_byte = 0;
            bits_left = 8;
        }
    }
    // Flush any remaining bits
    if bits_left != 8 {
        writer.put_u8(current_byte);
    }
}

fn unrolled_pack_1<N: NInt>(writer: &mut BytesMut, values: &[N]) {
    let mut iter = values.chunks_exact(8);
    for chunk in &mut iter {
        let n1 = chunk[0].to_u8().unwrap() & 0x01;
        let n2 = chunk[1].to_u8().unwrap() & 0x01;
        let n3 = chunk[2].to_u8().unwrap() & 0x01;
        let n4 = chunk[3].to_u8().unwrap() & 0x01;
        let n5 = chunk[4].to_u8().unwrap() & 0x01;
        let n6 = chunk[5].to_u8().unwrap() & 0x01;
        let n7 = chunk[6].to_u8().unwrap() & 0x01;
        let n8 = chunk[7].to_u8().unwrap() & 0x01;
        let byte =
            (n1 << 7) | (n2 << 6) | (n3 << 5) | (n4 << 4) | (n5 << 3) | (n6 << 2) | (n7 << 1) | n8;
        writer.put_u8(byte);
    }
    let remainder = iter.remainder();
    if !remainder.is_empty() {
        let mut byte = 0;
        for (i, n) in remainder.iter().enumerate() {
            let n = n.to_u8().unwrap();
            byte |= (n & 0x03) << (7 - i);
        }
        writer.put_u8(byte);
    }
}

fn unrolled_pack_2<N: NInt>(writer: &mut BytesMut, values: &[N]) {
    let mut iter = values.chunks_exact(4);
    for chunk in &mut iter {
        let n1 = chunk[0].to_u8().unwrap() & 0x03;
        let n2 = chunk[1].to_u8().unwrap() & 0x03;
        let n3 = chunk[2].to_u8().unwrap() & 0x03;
        let n4 = chunk[3].to_u8().unwrap() & 0x03;
        let byte = (n1 << 6) | (n2 << 4) | (n3 << 2) | n4;
        writer.put_u8(byte);
    }
    let remainder = iter.remainder();
    if !remainder.is_empty() {
        let mut byte = 0;
        for (i, n) in remainder.iter().enumerate() {
            let n = n.to_u8().unwrap();
            byte |= (n & 0x03) << (6 - i * 2);
        }
        writer.put_u8(byte);
    }
}

fn unrolled_pack_4<N: NInt>(writer: &mut BytesMut, values: &[N]) {
    let mut iter = values.chunks_exact(2);
    for chunk in &mut iter {
        let n1 = chunk[0].to_u8().unwrap() & 0x0F;
        let n2 = chunk[1].to_u8().unwrap() & 0x0F;
        let byte = (n1 << 4) | n2;
        writer.put_u8(byte);
    }
    let remainder = iter.remainder();
    if !remainder.is_empty() {
        let byte = remainder[0].to_u8().unwrap() & 0x0F;
        let byte = byte << 4;
        writer.put_u8(byte);
    }
}

fn unrolled_pack_bytes<N: NInt>(writer: &mut BytesMut, byte_size: usize, values: &[N]) {
    for num in values {
        let bytes = num.to_be_bytes();
        let bytes = &bytes.as_ref()[N::BYTE_SIZE - byte_size..];
        writer.put_slice(bytes);
    }
}

/// Decoding table for RLEv2 sub-encodings bit width.
///
/// Used by Direct, Patched Base and Delta. By default this assumes non-delta
/// (0 maps to 1), so Delta handles this discrepancy at the caller side.
///
/// Input must be a 5-bit integer (max value is 31).
pub fn rle_v2_decode_bit_width(encoded: u8) -> usize {
    debug_assert!(encoded < 32, "encoded bit width cannot exceed 5 bits");
    match encoded {
        0..=23 => encoded as usize + 1,
        24 => 26,
        25 => 28,
        26 => 30,
        27 => 32,
        28 => 40,
        29 => 48,
        30 => 56,
        31 => 64,
        _ => unreachable!(),
    }
}

/// Inverse of [`rle_v2_decode_bit_width`].
///
/// Assumes supported bit width is passed in. Will panic on invalid
/// inputs that aren't defined in the ORC bit width encoding table
/// (such as 50).
pub fn rle_v2_encode_bit_width(width: usize) -> u8 {
    debug_assert!(width <= 64, "bit width cannot exceed 64");
    match width {
        64 => 31,
        56 => 30,
        48 => 29,
        40 => 28,
        32 => 27,
        30 => 26,
        28 => 25,
        26 => 24,
        1..=24 => width as u8 - 1,
        _ => unreachable!(),
    }
}

pub fn get_closest_fixed_bits(n: usize) -> usize {
    match n {
        0 => 1,
        1..=24 => n,
        25..=26 => 26,
        27..=28 => 28,
        29..=30 => 30,
        31..=32 => 32,
        33..=40 => 40,
        41..=48 => 48,
        49..=56 => 56,
        57..=64 => 64,
        _ => unreachable!(),
    }
}

pub fn encode_bit_width(n: usize) -> usize {
    let n = get_closest_fixed_bits(n);
    match n {
        1..=24 => n - 1,
        25..=26 => 24,
        27..=28 => 25,
        29..=30 => 26,
        31..=32 => 27,
        33..=40 => 28,
        41..=48 => 29,
        49..=56 => 30,
        57..=64 => 31,
        _ => unreachable!(),
    }
}

fn decode_bit_width(n: usize) -> usize {
    match n {
        0..=23 => n + 1,
        24 => 26,
        25 => 28,
        26 => 30,
        27 => 32,
        28 => 40,
        29 => 48,
        30 => 56,
        31 => 64,
        _ => unreachable!(),
    }
}

/// Converts width of 64 bits or less to an aligned width, either rounding
/// up to the nearest multiple of 8, or rounding up to 1, 2 or 4.
pub fn get_closest_aligned_bit_width(width: usize) -> usize {
    debug_assert!(width <= 64, "bit width cannot exceed 64");
    match width {
        0..=1 => 1,
        2 => 2,
        3..=4 => 4,
        5..=8 => 8,
        9..=16 => 16,
        17..=24 => 24,
        25..=32 => 32,
        33..=40 => 40,
        41..=48 => 48,
        49..=54 => 56,
        55..=64 => 64,
        _ => unreachable!(),
    }
}

/// Get the nth percentile, where input percentile must be in range (0.0, 1.0].
pub fn calculate_percentile_bits<N: VarintSerde>(values: &[N], percentile: f32) -> usize {
    debug_assert!(
        percentile > 0.0 && percentile <= 1.0,
        "percentile must be in range (0.0, 1.0]"
    );

    let mut histogram = [0; 32];
    for n in values {
        // Map into range [0, 31]
        let encoded_bit_width = encode_bit_width(n.bits_used());
        histogram[encoded_bit_width] += 1;
    }

    // Then calculate the percentile here
    let count = values.len() as f32;
    let mut per_len = ((1.0 - percentile) * count) as usize;
    for i in (0..32).rev() {
        if let Some(a) = per_len.checked_sub(histogram[i]) {
            per_len = a;
        } else {
            return decode_bit_width(i);
        }
    }

    // If percentile is in correct input range then we should always return above
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use proptest::prelude::*;
    use std::io::Cursor;

    /// Easier to generate values then bound them, instead of generating correctly bounded
    /// values. In this case, bounds are that no value will exceed the `bit_width` in terms
    /// of bit size.
    fn mask_to_bit_width<N: NInt>(values: &[N], bit_width: usize) -> Vec<N> {
        let shift = N::BYTE_SIZE * 8 - bit_width;
        let mask = N::max_value().unsigned_shr(shift as u32);
        values.iter().map(|&v| v & mask).collect()
    }

    fn roundtrip_packed_ints_serde<N: NInt>(values: &[N], bit_width: usize) -> Result<Vec<N>> {
        let mut buf = BytesMut::new();
        let mut out = vec![];
        write_packed_ints(&mut buf, bit_width, values);
        read_ints(&mut out, values.len(), bit_width, &mut Cursor::new(buf))?;
        Ok(out)
    }

    proptest! {
        #[test]
        fn roundtrip_packed_ints_serde_i64(
            values in prop::collection::vec(any::<i64>(), 1..=512),
            bit_width in 1..=64_usize
        ) {
            let values = mask_to_bit_width(&values, bit_width);
            let out = roundtrip_packed_ints_serde(&values, bit_width)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_packed_ints_serde_i32(
            values in prop::collection::vec(any::<i32>(), 1..=512),
            bit_width in 1..=32_usize
        ) {
            let values = mask_to_bit_width(&values, bit_width);
            let out = roundtrip_packed_ints_serde(&values, bit_width)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_packed_ints_serde_i16(
            values in prop::collection::vec(any::<i16>(), 1..=512),
            bit_width in 1..=16_usize
        ) {
            let values = mask_to_bit_width(&values, bit_width);
            let out = roundtrip_packed_ints_serde(&values, bit_width)?;
            prop_assert_eq!(out, values);
        }
    }
}
