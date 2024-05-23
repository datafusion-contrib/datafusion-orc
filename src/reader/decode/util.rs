use std::io::Read;

use num::Signed;
use snafu::{OptionExt, ResultExt};

use crate::error::{self, IoSnafu, Result, VarintTooLargeSnafu};

use super::NInt;

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
    // TODO: we probably don't need this intermediary buffer, just copy bytes directly into Vec
    let mut num_buffer = N::empty_byte_array();
    for _ in 0..expected_num_of_ints {
        // Read into back part of buffer since is big endian.
        // So if smaller than N::BYTE_SIZE bytes, most significant bytes will be 0.
        r.read_exact(&mut num_buffer.as_mut()[N::BYTE_SIZE - num_bytes..])
            .context(IoSnafu)?;
        let num = N::from_be_bytes(num_buffer);
        buffer.push(num);
    }
    Ok(())
}

/// Encoding table for RLEv2 sub-encodings bit width.
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

/// Decode Base 128 Unsigned Varint
fn read_varint_n<N: NInt, R: Read>(r: &mut R) -> Result<N> {
    // Varints are encoded as sequence of bytes.
    // Where the high bit of a byte is set to 1 if the varint
    // continues into the next byte. Eventually it should terminate
    // with a byte with high bit of 0.
    let mut num = N::zero();
    let mut offset = 0;
    loop {
        let byte = read_u8(r)?;
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

pub fn read_varint_zigzagged<N: NInt, R: Read>(r: &mut R) -> Result<N> {
    Ok(read_varint_n::<N, _>(r)?.zigzag_decode())
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum AbsVarint<N: NInt> {
    Negative(N),
    Positive(N),
}

/// This trait (and its implementations) is intended to make generic the
/// behaviour of addition/subtraction over NInt.
// TODO: probably can be done in a cleaner/better way
pub trait AccumulateOp {
    fn acc<N: NInt>(a: N, b: N) -> Option<N>;
}

pub struct AddOp;

impl AccumulateOp for AddOp {
    fn acc<N: NInt>(a: N, b: N) -> Option<N> {
        a.checked_add(&b)
    }
}

pub struct SubOp;

impl AccumulateOp for SubOp {
    fn acc<N: NInt>(a: N, b: N) -> Option<N> {
        a.checked_sub(&b)
    }
}

/// Special case for delta where we need to parse as NInt, but it's signed.
/// So we calculate the absolute value and return the sign via enum variants.
pub fn read_abs_varint<N: NInt, R: Read>(r: &mut R) -> Result<AbsVarint<N>> {
    let num = read_varint_n::<N, _>(r)?;
    let is_negative = (num & N::one()) == N::one();
    // Unsigned >> to ensure new MSB is always 0 and not 1
    let num = num.unsigned_shr(1);
    if is_negative {
        // Because of two's complement
        let num = num + N::one();
        Ok(AbsVarint::Negative(num))
    } else {
        Ok(AbsVarint::Positive(num))
    }
}

/// Zigzag encoding stores the sign bit in the least significant bit.
#[inline]
pub fn signed_zigzag_decode<N: NInt + Signed>(encoded: N) -> N {
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

/// MSB indicates if value is negated (1 if negative, else positive). Note we
/// take the MSB of the encoded number which might be smaller than N, hence
/// we need the encoded number byte size to find this MSB.
#[inline]
pub fn signed_msb_decode<N: NInt + Signed>(encoded: N, encoded_byte_size: usize) -> N {
    let msb_mask = N::one() << (encoded_byte_size * 8 - 1);
    let is_positive = msb_mask & encoded == N::zero();
    let clean_sign_bit_mask = !msb_mask;
    let encoded = encoded & clean_sign_bit_mask;
    if is_positive {
        encoded
    } else {
        -encoded
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::error::Result;
    use crate::reader::decode::util::{read_varint_zigzagged, signed_zigzag_decode};

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
    fn test_read_vulong() -> Result<()> {
        fn test_assert(serialized: &[u8], expected: u64) -> Result<()> {
            let mut reader = Cursor::new(serialized);
            assert_eq!(expected, read_varint_zigzagged::<u64, _>(&mut reader)?);
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
        let err = read_varint_zigzagged::<u64, _>(&mut Cursor::new(&[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
        ]));
        assert!(err.is_err());
        assert_eq!(
            "Varint being decoded is too large",
            err.unwrap_err().to_string()
        );

        // when unexpected end to stream
        let err = read_varint_zigzagged::<u64, _>(&mut Cursor::new(&[0x80, 0x80]));
        assert!(err.is_err());
        assert_eq!(
            "Failed to read, source: failed to fill whole buffer",
            err.unwrap_err().to_string()
        );

        Ok(())
    }
}
