use std::io::{Read, Write};

use snafu::{OptionExt, ResultExt};

use crate::error::{IoSnafu, OrcError, OutOfSpecSnafu, Result};
use crate::reader::decode::rle_v2::{EncodingType, MAX_RUN_LENGTH};
use crate::reader::decode::util::{
    extract_run_length_from_header, get_closest_aligned_bit_width, read_abs_varint_zigzagged,
    read_ints, read_u8, read_varint_zigzagged, rle_v2_decode_bit_width, rle_v2_encode_bit_width,
    write_abs_varint_zigzagged, write_aligned_packed_ints, write_varint_zigzagged, AbsVarint,
    AccumulateOp, AddOp, SubOp,
};

use super::NInt;

fn fixed_delta<N: NInt, A: AccumulateOp>(
    out_ints: &mut Vec<N>,
    length: usize,
    base_value: N,
    delta: N,
) -> Result<()> {
    // Skip first value since that's base_value
    (1..length).try_fold(base_value, |acc, _| {
        let acc = A::acc(acc, delta).context(OutOfSpecSnafu {
            msg: "over/underflow when decoding delta integer",
        })?;
        out_ints.push(acc);
        Ok::<_, OrcError>(acc)
    })?;
    Ok(())
}

fn varied_deltas<N: NInt, R: Read, A: AccumulateOp>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    length: usize,
    base_value: N,
    delta: N,
    delta_bit_width: usize,
) -> Result<()> {
    // Add delta base and first value
    let second_value = A::acc(base_value, delta).context(OutOfSpecSnafu {
        msg: "over/underflow when decoding delta integer",
    })?;
    out_ints.push(second_value);
    // Run length includes base value and first delta, so skip them
    let length = length - 2;

    // Unpack the delta values
    read_ints(out_ints, length, delta_bit_width, reader)?;
    out_ints
        .iter_mut()
        // Ignore base_value and second_value
        .skip(2)
        // Each element is the delta, so find actual value using running accumulator
        .try_fold(second_value, |acc, delta| {
            let acc = A::acc(acc, *delta).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding delta integer",
            })?;
            *delta = acc;
            Ok::<_, OrcError>(acc)
        })?;
    Ok(())
}

pub fn read_delta_values<N: NInt, R: Read>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
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

    let base_value = read_varint_zigzagged::<N, _>(reader)?;
    out_ints.push(base_value);

    // Always signed since can be decreasing sequence
    let delta_base = read_abs_varint_zigzagged::<N, _>(reader)?;

    match (delta_bit_width, delta_base) {
        // If width is 0 then all values have fixed delta of delta_base
        (0, AbsVarint::Negative(delta)) => {
            fixed_delta::<N, SubOp>(out_ints, length, base_value, delta)?;
        }
        (0, AbsVarint::Positive(delta)) => {
            fixed_delta::<N, AddOp>(out_ints, length, base_value, delta)?;
        }
        (delta_bit_width, AbsVarint::Negative(delta)) => {
            varied_deltas::<N, R, SubOp>(
                reader,
                out_ints,
                length,
                base_value,
                delta,
                delta_bit_width,
            )?;
        }
        (delta_bit_width, AbsVarint::Positive(delta)) => {
            varied_deltas::<N, R, AddOp>(
                reader,
                out_ints,
                length,
                base_value,
                delta,
                delta_bit_width,
            )?;
        }
    }
    Ok(())
}

pub fn write_varying_delta_values<N: NInt, W: Write>(
    writer: &mut W,
    base_value: N,
    first_delta: AbsVarint<N>,
    max_delta: N,
    subsequent_deltas: &[N],
) -> Result<()> {
    debug_assert!(
        max_delta > N::zero(),
        "varying deltas must have at least one non-zero delta"
    );
    let bit_width = N::BYTE_SIZE * 8 - max_delta.leading_zeros() as usize;
    let bit_width = get_closest_aligned_bit_width(bit_width);
    // We can't have bit width of 1 for delta as that would get decoded as
    // 0 bit width on reader, which indicates fixed delta, so bump 1 to 2
    // in this case.
    let bit_width = if bit_width == 1 { 2 } else { bit_width };
    // Add 2 to len for the base_value and first_delta
    let header = derive_delta_header(bit_width, subsequent_deltas.len() + 2);
    writer.write_all(&header).context(IoSnafu)?;

    // Encoding base
    write_varint_zigzagged(writer, base_value)?;

    // Encoding base delta as signed varint
    write_abs_varint_zigzagged(writer, first_delta)?;

    // Bitpacked deltas
    write_aligned_packed_ints(writer, bit_width, subsequent_deltas)?;

    Ok(())
}

pub fn write_fixed_delta_values<N: NInt, W: Write>(
    writer: &mut W,
    base_value: N,
    delta: AbsVarint<N>,
    len: usize,
) -> Result<()> {
    // Assuming len includes base_value and first delta
    let header = derive_delta_header(0, len);
    writer.write_all(&header).context(IoSnafu)?;

    // Encoding base
    write_varint_zigzagged(writer, base_value)?;

    // Encoding base delta as signed varint
    write_abs_varint_zigzagged(writer, delta)?;

    Ok(())
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

    use super::*;

    // TODO: figure out how to write proptests for these

    #[test]
    fn test_fixed_delta_positive() {
        let mut buf = vec![];
        let mut out = vec![];
        write_fixed_delta_values(&mut buf, 0_u64, AbsVarint::Positive(10), 100).unwrap();
        let header = buf[0];
        read_delta_values::<u64, _>(&mut Cursor::new(&buf[1..]), &mut out, header).unwrap();

        let expected = (0..100).map(|i| i * 10).collect::<Vec<u64>>();
        assert_eq!(expected, out);
    }

    #[test]
    fn test_fixed_delta_negative() {
        let mut buf = vec![];
        let mut out = vec![];
        write_fixed_delta_values(&mut buf, 10_000_u64, AbsVarint::Negative(63), 150).unwrap();
        let header = buf[0];
        read_delta_values::<u64, _>(&mut Cursor::new(&buf[1..]), &mut out, header).unwrap();

        let expected = (0..150).map(|i| 10_000_u64 - i * 63).collect::<Vec<u64>>();
        assert_eq!(expected, out);
    }

    #[test]
    fn test_varying_delta_positive() {
        let deltas = [
            1, 6, 98, 12, 65, 9, 0, 0, 1, 128, 643, 129, 469, 123, 4572, 124,
        ];
        let max = *deltas.iter().max().unwrap();

        let mut buf = vec![];
        let mut out = vec![];
        write_varying_delta_values(&mut buf, 0_u64, AbsVarint::Positive(10), max, &deltas).unwrap();
        let header = buf[0];
        read_delta_values::<u64, _>(&mut Cursor::new(&buf[1..]), &mut out, header).unwrap();

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

        let mut buf = vec![];
        let mut out = vec![];
        write_varying_delta_values(&mut buf, 10_000_u64, AbsVarint::Negative(1), max, &deltas)
            .unwrap();
        let header = buf[0];
        read_delta_values::<u64, _>(&mut Cursor::new(&buf[1..]), &mut out, header).unwrap();

        let mut expected = vec![10_000, 9_999];
        let mut i = 1;
        for d in deltas {
            expected.push(expected[i] - d);
            i += 1;
        }
        assert_eq!(expected, out);
    }
}
