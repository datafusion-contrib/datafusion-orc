use std::io::Read;

use bytes::{BufMut, BytesMut};

use crate::error::{OutOfSpecSnafu, Result};
use crate::reader::decode::rle_v2::{EncodingType, MAX_RUN_LENGTH};
use crate::reader::decode::util::{
    extract_run_length_from_header, read_ints, read_u8, rle_v2_decode_bit_width,
    rle_v2_encode_bit_width, write_aligned_packed_ints,
};

use super::NInt;

pub fn read_direct_values<N: NInt, R: Read>(
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
        *lit = lit.zigzag_decode();
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
        *values.iter().max().unwrap()
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

    use crate::reader::decode::VarintSerde;

    use super::*;

    fn roundtrip_direct_helper<N: NInt>(values: &[N]) -> Result<Vec<N>> {
        let mut buf = BytesMut::new();
        let mut out = vec![];

        write_direct(&mut buf, values, None);
        let header = buf[0];
        read_direct_values(&mut Cursor::new(&buf[1..]), &mut out, header)?;

        Ok(out)
    }

    proptest! {
        #[test]
        fn roundtrip_direct_i16(values in prop::collection::vec(any::<i16>(), 1..512)) {
            let encoded= values.iter().map(|v| v.zigzag_encode()).collect::<Vec<_>>();
            let out = roundtrip_direct_helper(&encoded)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_direct_i32(values in prop::collection::vec(any::<i32>(), 1..512)) {
            let encoded= values.iter().map(|v| v.zigzag_encode()).collect::<Vec<_>>();
            let out = roundtrip_direct_helper(&encoded)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_direct_i64(values in prop::collection::vec(any::<i64>(), 1..512)) {
            let encoded= values.iter().map(|v| v.zigzag_encode()).collect::<Vec<_>>();
            let out = roundtrip_direct_helper(&encoded)?;
            prop_assert_eq!(out, values);
        }
    }
}
