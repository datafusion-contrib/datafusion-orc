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
use num::Signed;
use snafu::OptionExt;

use crate::{
    encoding::util::read_u8,
    error::{Result, VarintTooLargeSnafu},
};

use super::{EncodingSign, NInt, VarintSerde};

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

/// Decode Base 128 Unsigned Varint
fn read_varint<N: VarintSerde, R: Read>(reader: &mut R) -> Result<N> {
    // Varints are encoded as sequence of bytes.
    // Where the high bit of a byte is set to 1 if the varint
    // continues into the next byte. Eventually it should terminate
    // with a byte with high bit of 0.
    let mut num = N::zero();
    let mut offset = 0;
    loop {
        let byte = read_u8(reader)?;
        let is_last_byte = byte & 0x80 == 0;
        let without_continuation_bit = byte & 0x7F;
        num |= N::from_u8(without_continuation_bit)
            // Ensure we don't overflow
            .checked_shl(offset)
            .context(VarintTooLargeSnafu)?;
        // Since high bit doesn't contribute to final number,
        // we need to shift in multiples of 7 to account for this.
        offset += 7;
        if is_last_byte {
            break;
        }
    }
    Ok(num)
}

/// Encode Base 128 Unsigned Varint
fn write_varint<N: VarintSerde>(writer: &mut BytesMut, value: N) {
    // Take max in case value = 0.
    // Divide by 7 as high bit is always used as continuation flag.
    let byte_size = value.bits_used().div_ceil(7).max(1);
    // By default we'll have continuation bit set
    // TODO: can probably do without Vec allocation?
    let mut bytes = vec![0x80; byte_size];
    // Then just clear for the last one
    let i = bytes.len() - 1;
    bytes[i] = 0;

    // Encoding 7 bits at a time into bytes
    let mask = N::from_u8(0x7F);
    for (i, b) in bytes.iter_mut().enumerate() {
        let shift = i * 7;
        *b |= ((value >> shift) & mask).to_u8().unwrap();
    }

    writer.put_slice(&bytes);
}

pub fn read_varint_zigzagged<N: VarintSerde, R: Read, S: EncodingSign>(
    reader: &mut R,
) -> Result<N> {
    let unsigned = read_varint::<N, _>(reader)?;
    Ok(S::zigzag_decode(unsigned))
}

pub fn write_varint_zigzagged<N: VarintSerde, S: EncodingSign>(writer: &mut BytesMut, value: N) {
    let value = S::zigzag_encode(value);
    write_varint(writer, value)
}

/// Zigzag encoding stores the sign bit in the least significant bit.
#[inline]
pub fn signed_zigzag_decode<N: VarintSerde + Signed>(encoded: N) -> N {
    let without_sign_bit = encoded.unsigned_shr(1);
    let sign_bit = encoded & N::one();
    // If positive, sign_bit is 0
    //   Negating 0 and doing bitwise XOR will just return without_sign_bit
    //   Since A ^ 0 = A
    // If negative, sign_bit is 1
    //   Negating turns to 11111...11
    //   Then A ^ 1 = ~A (negating every bit in A)
    without_sign_bit ^ -sign_bit
}

/// Opposite of [`signed_zigzag_decode`].
#[inline]
pub fn signed_zigzag_encode<N: VarintSerde + Signed>(value: N) -> N {
    let l = N::BYTE_SIZE * 8 - 1;
    (value << 1_usize) ^ (value >> l)
}

/// MSB indicates if value is negated (1 if negative, else positive). Note we
/// take the MSB of the encoded number which might be smaller than N, hence
/// we need the encoded number byte size to find this MSB.
#[inline]
pub fn signed_msb_decode<N: NInt + Signed>(encoded: N, encoded_byte_size: usize) -> N {
    let msb_mask = N::one() << (encoded_byte_size * 8 - 1);
    let is_positive = (encoded & msb_mask) == N::zero();
    let clean_sign_bit_mask = !msb_mask;
    let encoded = encoded & clean_sign_bit_mask;
    if is_positive {
        encoded
    } else {
        -encoded
    }
}

/// Inverse of [`signed_msb_decode`].
#[inline]
// TODO: bound this to only allow i64 input? might mess up for i32::MIN?
pub fn signed_msb_encode<N: NInt + Signed>(value: N, encoded_byte_size: usize) -> N {
    let is_signed = value.is_negative();
    // 0 if unsigned, 1 if signed
    let sign_bit = N::from_u8(is_signed as u8);
    let value = value.abs();
    let encoded_msb = sign_bit << (encoded_byte_size * 8 - 1);
    encoded_msb | value
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
    use crate::{
        encoding::integer::{SignedEncoding, UnsignedEncoding},
        error::Result,
    };
    use proptest::prelude::*;
    use std::io::Cursor;

    #[test]
    fn test_zigzag_decode() {
        assert_eq!(0, signed_zigzag_decode(0));
        assert_eq!(-1, signed_zigzag_decode(1));
        assert_eq!(1, signed_zigzag_decode(2));
        assert_eq!(-2, signed_zigzag_decode(3));
        assert_eq!(2, signed_zigzag_decode(4));
        assert_eq!(-3, signed_zigzag_decode(5));
        assert_eq!(3, signed_zigzag_decode(6));
        assert_eq!(-4, signed_zigzag_decode(7));
        assert_eq!(4, signed_zigzag_decode(8));
        assert_eq!(-5, signed_zigzag_decode(9));

        assert_eq!(9_223_372_036_854_775_807, signed_zigzag_decode(-2_i64));
        assert_eq!(-9_223_372_036_854_775_808, signed_zigzag_decode(-1_i64));
    }

    #[test]
    fn test_zigzag_encode() {
        assert_eq!(0, signed_zigzag_encode(0));
        assert_eq!(1, signed_zigzag_encode(-1));
        assert_eq!(2, signed_zigzag_encode(1));
        assert_eq!(3, signed_zigzag_encode(-2));
        assert_eq!(4, signed_zigzag_encode(2));
        assert_eq!(5, signed_zigzag_encode(-3));
        assert_eq!(6, signed_zigzag_encode(3));
        assert_eq!(7, signed_zigzag_encode(-4));
        assert_eq!(8, signed_zigzag_encode(4));
        assert_eq!(9, signed_zigzag_encode(-5));

        assert_eq!(-2_i64, signed_zigzag_encode(9_223_372_036_854_775_807));
        assert_eq!(-1_i64, signed_zigzag_encode(-9_223_372_036_854_775_808));
    }

    #[test]
    fn roundtrip_zigzag_edge_cases() {
        let value = 0_i16;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);
        let value = i16::MAX;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);

        let value = 0_i32;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);
        let value = i32::MAX;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);
        let value = i32::MIN;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);

        let value = 0_i64;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);
        let value = i64::MAX;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);
        let value = i64::MIN;
        assert_eq!(signed_zigzag_decode(signed_zigzag_encode(value)), value);
    }

    proptest! {
        #[test]
        fn roundtrip_zigzag_i16(value: i16) {
            let out = signed_zigzag_decode(signed_zigzag_encode(value));
            prop_assert_eq!(value, out);
        }

        #[test]
        fn roundtrip_zigzag_i32(value: i32) {
            let out = signed_zigzag_decode(signed_zigzag_encode(value));
            prop_assert_eq!(value, out);
        }

        #[test]
        fn roundtrip_zigzag_i64(value: i64) {
            let out = signed_zigzag_decode(signed_zigzag_encode(value));
            prop_assert_eq!(value, out);
        }
    }

    fn generate_msb_test_value<N: NInt + Signed>(
        seed_value: N,
        byte_size: usize,
        signed: bool,
    ) -> N {
        // We mask out to values that can fit within the specified byte_size.
        let shift = (N::BYTE_SIZE - byte_size) * 8;
        let mask = N::max_value().unsigned_shr(shift as u32);
        // And remove the msb since we manually set a value to signed based on the signed parameter.
        let mask = mask >> 1;
        let value = seed_value & mask;
        // This guarantees values that can fit within byte_size when they are msb encoded, both
        // signed and unsigned.
        if signed {
            -value
        } else {
            value
        }
    }

    #[test]
    fn roundtrip_msb_edge_cases() {
        // Testing all cases of max values for byte_size + signed combinations
        for byte_size in 1..=2 {
            for signed in [true, false] {
                let value = generate_msb_test_value(i16::MAX, byte_size, signed);
                let out = signed_msb_decode(signed_msb_encode(value, byte_size), byte_size);
                assert_eq!(value, out);
            }
        }

        for byte_size in 1..=4 {
            for signed in [true, false] {
                let value = generate_msb_test_value(i32::MAX, byte_size, signed);
                let out = signed_msb_decode(signed_msb_encode(value, byte_size), byte_size);
                assert_eq!(value, out);
            }
        }

        for byte_size in 1..=8 {
            for signed in [true, false] {
                let value = generate_msb_test_value(i64::MAX, byte_size, signed);
                let out = signed_msb_decode(signed_msb_encode(value, byte_size), byte_size);
                assert_eq!(value, out);
            }
        }
    }

    proptest! {
        #[test]
        fn roundtrip_msb_i16(value: i16, byte_size in 1..=2_usize, signed: bool) {
            let value = generate_msb_test_value(value, byte_size, signed);
            let out = signed_msb_decode(signed_msb_encode(value, byte_size), byte_size);
            prop_assert_eq!(value, out);
        }

        #[test]
        fn roundtrip_msb_i32(value: i32, byte_size in 1..=4_usize, signed: bool) {
            let value = generate_msb_test_value(value, byte_size, signed);
            let out = signed_msb_decode(signed_msb_encode(value, byte_size), byte_size);
            prop_assert_eq!(value, out);
        }

        #[test]
        fn roundtrip_msb_i64(value: i64, byte_size in 1..=8_usize, signed: bool) {
            let value = generate_msb_test_value(value, byte_size, signed);
            let out = signed_msb_decode(signed_msb_encode(value, byte_size), byte_size);
            prop_assert_eq!(value, out);
        }
    }

    #[test]
    fn test_read_varint() -> Result<()> {
        fn test_assert(serialized: &[u8], expected: i64) -> Result<()> {
            let mut reader = Cursor::new(serialized);
            assert_eq!(
                expected,
                read_varint_zigzagged::<i64, _, UnsignedEncoding>(&mut reader)?
            );
            Ok(())
        }

        test_assert(&[0x00], 0)?;
        test_assert(&[0x01], 1)?;
        test_assert(&[0x7f], 127)?;
        test_assert(&[0x80, 0x01], 128)?;
        test_assert(&[0x81, 0x01], 129)?;
        test_assert(&[0xff, 0x7f], 16_383)?;
        test_assert(&[0x80, 0x80, 0x01], 16_384)?;
        test_assert(&[0x81, 0x80, 0x01], 16_385)?;

        // when too large
        let err = read_varint_zigzagged::<i64, _, UnsignedEncoding>(&mut Cursor::new(&[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
        ]));
        assert!(err.is_err());
        assert_eq!(
            "Varint being decoded is too large",
            err.unwrap_err().to_string()
        );

        // when unexpected end to stream
        let err =
            read_varint_zigzagged::<i64, _, UnsignedEncoding>(&mut Cursor::new(&[0x80, 0x80]));
        assert!(err.is_err());
        assert_eq!(
            "Failed to read, source: failed to fill whole buffer",
            err.unwrap_err().to_string()
        );

        Ok(())
    }

    fn roundtrip_varint<N: VarintSerde, S: EncodingSign>(value: N) -> N {
        let mut buf = BytesMut::new();
        write_varint_zigzagged::<N, S>(&mut buf, value);
        read_varint_zigzagged::<N, _, S>(&mut Cursor::new(&buf)).unwrap()
    }

    proptest! {
        #[test]
        fn roundtrip_varint_i16(value: i16) {
            let out = roundtrip_varint::<_, SignedEncoding>(value);
            prop_assert_eq!(out, value);
        }

        #[test]
        fn roundtrip_varint_i32(value: i32) {
            let out = roundtrip_varint::<_, SignedEncoding>(value);
            prop_assert_eq!(out, value);
        }

        #[test]
        fn roundtrip_varint_i64(value: i64) {
            let out = roundtrip_varint::<_, SignedEncoding>(value);
            prop_assert_eq!(out, value);
        }

        #[test]
        fn roundtrip_varint_i128(value: i128) {
            let out = roundtrip_varint::<_, SignedEncoding>(value);
            prop_assert_eq!(out, value);
        }

        #[test]
        fn roundtrip_varint_u64(value in 0..=i64::MAX) {
            let out = roundtrip_varint::<_, UnsignedEncoding>(value);
            prop_assert_eq!(out, value);
        }
    }

    #[test]
    fn roundtrip_varint_edge_cases() {
        let value = 0_i16;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i16::MIN;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i16::MAX;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);

        let value = 0_i32;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i32::MIN;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i32::MAX;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);

        let value = 0_i64;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i64::MIN;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i64::MAX;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);

        let value = 0_i128;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i128::MIN;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
        let value = i128::MAX;
        assert_eq!(roundtrip_varint::<_, SignedEncoding>(value), value);
    }

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
