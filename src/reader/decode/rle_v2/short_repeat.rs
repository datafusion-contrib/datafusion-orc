use std::io::Read;

use bytes::{BufMut, BytesMut};
use snafu::ResultExt;

use crate::{
    error::{IoSnafu, OutOfSpecSnafu, Result},
    reader::decode::{rle_v2::EncodingType, EncodingSign},
};

use super::{NInt, SHORT_REPEAT_MIN_LENGTH};

pub fn read_short_repeat_values<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    header: u8,
) -> Result<()> {
    // Header byte:
    //
    // eeww_wccc
    // 7       0 LSB
    //
    // ee  = Sub-encoding bits, always 00
    // www = Value width bits
    // ccc = Repeat count bits

    let byte_width = (header >> 3) & 0x07; // Encoded as 0 to 7
    let byte_width = byte_width as usize + 1; // Decode to 1 to 8 bytes

    if N::BYTE_SIZE < byte_width {
        return OutOfSpecSnafu {
            msg:
                "byte width of short repeat encoding exceeds byte size of integer being decoded to",
        }
        .fail();
    }

    let run_length = (header & 0x07) as usize + SHORT_REPEAT_MIN_LENGTH;

    // Value that is being repeated is encoded as value_byte_width bytes in big endian format
    let mut buffer = N::empty_byte_array();
    // Read into back part of buffer since is big endian.
    // So if smaller than N::BYTE_SIZE bytes, most significant bytes will be 0.
    reader
        .read_exact(&mut buffer.as_mut()[N::BYTE_SIZE - byte_width..])
        .context(IoSnafu)?;
    let val = N::from_be_bytes(buffer);
    let val = S::zigzag_decode(val);

    out_ints.extend(std::iter::repeat(val).take(run_length));

    Ok(())
}

pub fn write_short_repeat<N: NInt, S: EncodingSign>(writer: &mut BytesMut, value: N, count: usize) {
    debug_assert!((SHORT_REPEAT_MIN_LENGTH..=10).contains(&count));

    let value = S::zigzag_encode(value);

    // Take max in case value = 0
    let byte_size = value.bits_used().div_ceil(8).max(1) as u8;
    let encoded_byte_size = byte_size - 1;
    let encoded_count = (count - SHORT_REPEAT_MIN_LENGTH) as u8;

    let header = EncodingType::ShortRepeat.to_header() | (encoded_byte_size << 3) | encoded_count;
    let bytes = value.to_be_bytes();
    let bytes = &bytes.as_ref()[N::BYTE_SIZE - byte_size as usize..];

    writer.put_u8(header);
    writer.put_slice(bytes);
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use proptest::prelude::*;

    use crate::reader::decode::{SignedEncoding, UnsignedEncoding};

    use super::*;

    fn roundtrip_short_repeat_helper<N: NInt, S: EncodingSign>(
        value: N,
        count: usize,
    ) -> Result<Vec<N>> {
        let mut buf = BytesMut::new();
        let mut out = vec![];

        write_short_repeat::<_, S>(&mut buf, value, count);
        let header = buf[0];
        read_short_repeat_values::<_, _, S>(&mut Cursor::new(&buf[1..]), &mut out, header)?;

        Ok(out)
    }

    proptest! {
        #[test]
        fn roundtrip_short_repeat_i16(value: i16, count in 3_usize..=10) {
            let out = roundtrip_short_repeat_helper::<_, SignedEncoding>(value, count)?;
            prop_assert_eq!(out, vec![value; count]);
        }

        #[test]
        fn roundtrip_short_repeat_i32(value: i32, count in 3_usize..=10) {
            let out = roundtrip_short_repeat_helper::<_, SignedEncoding>(value, count)?;
            prop_assert_eq!(out, vec![value; count]);
        }

        #[test]
        fn roundtrip_short_repeat_i64(value: i64, count in 3_usize..=10) {
            let out = roundtrip_short_repeat_helper::<_, SignedEncoding>(value, count)?;
            prop_assert_eq!(out, vec![value; count]);
        }

        #[test]
        fn roundtrip_short_repeat_i64_unsigned(value in 0..=i64::MAX, count in 3_usize..=10) {
            let out = roundtrip_short_repeat_helper::<_, UnsignedEncoding>(value, count)?;
            prop_assert_eq!(out, vec![value; count]);
        }
    }
}
