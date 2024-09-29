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
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result, VarintTooLargeSnafu};

use super::{EncodingSign, NInt, VarintSerde};

/// Read single byte
#[inline]
pub fn read_u8(reader: &mut impl Read) -> Result<u8> {
    let mut byte = [0];
    reader.read_exact(&mut byte).context(error::IoSnafu)?;
    Ok(byte[0])
}

/// Like [`read_u8()`] but returns `Ok(None)` if reader has reached EOF
#[inline]
pub fn try_read_u8(reader: &mut impl Read) -> Result<Option<u8>> {
    let mut byte = [0];
    let length = reader.read(&mut byte).context(error::IoSnafu)?;
    Ok((length > 0).then_some(byte[0]))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        encoding::{SignedEncoding, UnsignedEncoding},
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
}
