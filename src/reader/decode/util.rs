use std::{collections::VecDeque, io::Read};

use snafu::{OptionExt, ResultExt};

use crate::error::{self, IoSnafu, Result, VarintTooLargeSnafu};

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
    if length == 0 {
        Ok(None)
    } else {
        Ok(Some(byte[0]))
    }
}

#[inline]
pub fn bytes_to_long_be<R: Read>(r: &mut R, byte_width: usize) -> Result<i64> {
    let mut buffer = [0; 8];
    // read into back part of buffer since is big endian
    // so if smaller than 8 bytes, most significant bytes will be 0
    r.read_exact(&mut buffer[8 - byte_width..])
        .context(IoSnafu)?;
    Ok(i64::from_be_bytes(buffer))
}

pub fn get_closest_fixed_bits(width: usize) -> usize {
    match width {
        0 => 1,
        1..=24 => width,
        25..=26 => 26,
        27..=28 => 28,
        29..=30 => 30,
        31..=32 => 32,
        33..=40 => 40,
        41..=48 => 48,
        49..=56 => 56,
        _ => 64,
    }
}

pub fn read_ints(
    queue: &mut VecDeque<i64>,
    expected_no_of_ints: usize,
    bit_size: usize,
    r: &mut impl Read,
) -> Result<()> {
    match bit_size {
        1 => unrolled_unpack_1(queue, expected_no_of_ints, r),
        2 => unrolled_unpack_2(queue, expected_no_of_ints, r),
        4 => unrolled_unpack_4(queue, expected_no_of_ints, r),
        8 => unrolled_unpack_8(queue, expected_no_of_ints, r),
        16 => unrolled_unpack_16(queue, expected_no_of_ints, r),
        24 => unrolled_unpack_24(queue, expected_no_of_ints, r),
        32 => unrolled_unpack_32(queue, expected_no_of_ints, r),
        40 => unrolled_unpack_40(queue, expected_no_of_ints, r),
        48 => unrolled_unpack_48(queue, expected_no_of_ints, r),
        56 => unrolled_unpack_56(queue, expected_no_of_ints, r),
        64 => unrolled_unpack_64(queue, expected_no_of_ints, r),
        _ => {
            let mut bits_left = 0;
            let mut current = 0;
            for _ in 0..expected_no_of_ints {
                let mut result: i64 = 0;
                let mut bits_left_to_read = bit_size;

                while bits_left_to_read > bits_left {
                    result <<= bits_left;
                    result |= (current & ((1 << bits_left) - 1)) as i64;
                    bits_left_to_read -= bits_left;

                    current = read_u8(r)? as i32;

                    bits_left = 8;
                }

                if bits_left_to_read > 0 {
                    result <<= bits_left_to_read;
                    bits_left -= bits_left_to_read;
                    result |= ((current >> bits_left) & ((1 << bits_left_to_read) - 1)) as i64;
                }

                queue.push_back(result);
            }

            Ok(())
        }
    }
}

/// Decode numbers with bit width of 1 from read stream
fn unrolled_unpack_1(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    reader: &mut impl Read,
) -> Result<()> {
    for _ in 0..(expected_num_of_ints / 8) {
        let byte = read_u8(reader)? as i64;
        buffer.push_back((byte >> 7) & 1);
        buffer.push_back((byte >> 6) & 1);
        buffer.push_back((byte >> 5) & 1);
        buffer.push_back((byte >> 4) & 1);
        buffer.push_back((byte >> 3) & 1);
        buffer.push_back((byte >> 2) & 1);
        buffer.push_back((byte >> 1) & 1);
        buffer.push_back(byte & 1);
    }

    // less than full byte at end, extract these trailing numbers
    let remainder = expected_num_of_ints % 8;
    if remainder > 0 {
        let mut start_shift = 7;
        let byte = read_u8(reader)? as i64;
        for _ in 0..remainder {
            buffer.push_back((byte >> start_shift) & 1);
            start_shift -= 1;
        }
    }

    Ok(())
}

/// Decode numbers with bit width of 2 from read stream
fn unrolled_unpack_2(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    reader: &mut impl Read,
) -> Result<()> {
    for _ in 0..(expected_num_of_ints / 4) {
        let byte = read_u8(reader)? as i64;
        buffer.push_back((byte >> 6) & 3);
        buffer.push_back((byte >> 4) & 3);
        buffer.push_back((byte >> 2) & 3);
        buffer.push_back(byte & 3);
    }

    // less than full byte at end, extract these trailing numbers
    let remainder = expected_num_of_ints % 4;
    if remainder > 0 {
        let mut start_shift = 6;
        let byte = read_u8(reader)? as i64;
        for _ in 0..remainder {
            buffer.push_back((byte >> start_shift) & 3);
            start_shift -= 2;
        }
    }

    Ok(())
}

/// Decode numbers with bit width of 4 from read stream
fn unrolled_unpack_4(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    reader: &mut impl Read,
) -> Result<()> {
    for _ in 0..(expected_num_of_ints / 2) {
        let byte = read_u8(reader)? as i64;
        buffer.push_back((byte >> 4) & 15);
        buffer.push_back(byte & 15);
    }

    // at worst have 1 trailing 4-bit number
    let remainder = expected_num_of_ints % 2;
    if remainder > 0 {
        let byte = read_u8(reader)? as i64;
        buffer.push_back((byte >> 4) & 15);
    }

    Ok(())
}

fn unrolled_unpack_8(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 1)
}

fn unrolled_unpack_16(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 2)
}

fn unrolled_unpack_24(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 3)
}

fn unrolled_unpack_32(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 4)
}

fn unrolled_unpack_40(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 5)
}

fn unrolled_unpack_48(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 6)
}

fn unrolled_unpack_56(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 7)
}

fn unrolled_unpack_64(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, expected_num_of_ints, r, 8)
}

#[inline]
fn unrolled_unpack_bytes(
    buffer: &mut VecDeque<i64>,
    expected_num_of_ints: usize,
    r: &mut impl Read,
    num_bytes: usize,
) -> Result<()> {
    for _ in 0..expected_num_of_ints {
        let num = bytes_to_long_be(r, num_bytes)?;
        buffer.push_back(num);
    }
    Ok(())
}

pub fn rle_v2_direct_bit_width(value: u8) -> u8 {
    match value {
        0..=23 => value + 1,
        27 => 32,
        28 => 40,
        29 => 48,
        30 => 56,
        31 => 64,
        other => todo!("{other}"),
    }
}

pub fn header_to_rle_v2_direct_bit_width(header: u8) -> u8 {
    let bit_width = (header & 0b00111110) >> 1;
    rle_v2_direct_bit_width(bit_width)
}

/// Decode Base 128 Unsigned Varint
pub fn read_vulong(r: &mut impl Read) -> Result<u64> {
    // Varints are encoded as sequence of bytes.
    // Where the high bit of a byte is set to 1 if the varint
    // continues into the next byte. Eventually it should terminate
    // with a byte with high bit of 0.
    let mut num: u64 = 0;
    let mut offset = 0;
    loop {
        let b = read_u8(r)?;
        let is_end = b & 0x80 == 0;
        // Clear continuation bit
        let b = b & 0x7F;
        // TODO: have check for if larger than u64?
        num |= u64::from(b)
            // Ensure we don't overflow
            .checked_shl(offset)
            .context(VarintTooLargeSnafu)?;
        // Since high bit doesn't contribute to final number
        // We need to shift in multiples of 7 to account for this
        offset += 7;
        // If we've finally reached last byte
        if is_end {
            break;
        }
    }
    Ok(num)
}

pub fn read_vslong<R: Read>(r: &mut R) -> Result<i64> {
    read_vulong(r).map(zigzag_decode)
}

pub fn zigzag_decode(unsigned: u64) -> i64 {
    // Zigzag encoding stores the sign bit in the least significant bit
    let without_sign_bit = (unsigned >> 1) as i64;
    let sign_bit = unsigned & 1;
    // If positive, sign_bit is 0
    //   Negating 0 and doing bitwise XOR will just return without_sign_bit
    //   Since A ^ 0 = A
    // If negative, sign_bit is 1
    //   Converting to i64, and negating ...
    without_sign_bit ^ -(sign_bit as i64)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::error::Result;
    use crate::reader::decode::util::{read_vulong, zigzag_decode};

    #[test]
    fn test_zigzag_decode() {
        assert_eq!(0, zigzag_decode(0));
        assert_eq!(-1, zigzag_decode(1));
        assert_eq!(1, zigzag_decode(2));
        assert_eq!(-2, zigzag_decode(3));
        assert_eq!(2, zigzag_decode(4));
        assert_eq!(-3, zigzag_decode(5));
        assert_eq!(3, zigzag_decode(6));
        assert_eq!(-4, zigzag_decode(7));
        assert_eq!(4, zigzag_decode(8));
        assert_eq!(-5, zigzag_decode(9));
    }

    #[test]
    fn test_read_vulong() -> Result<()> {
        fn test_assert(serialized: &[u8], expected: u64) -> Result<()> {
            let mut reader = Cursor::new(serialized);
            assert_eq!(expected, read_vulong(&mut reader)?);
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
        test_assert(
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
            u64::MAX,
        )?;

        // when too large
        let err = read_vulong(&mut Cursor::new(&[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
        ]));
        assert!(err.is_err());
        assert_eq!(
            "Varint being decoded is too large",
            err.unwrap_err().to_string()
        );

        // when unexpected end to stream
        let err = read_vulong(&mut Cursor::new(&[0x80, 0x80]));
        assert!(err.is_err());
        assert_eq!(
            "Failed to read, source: failed to fill whole buffer",
            err.unwrap_err().to_string()
        );

        Ok(())
    }
}
