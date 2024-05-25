use std::io::Read;

use snafu::OptionExt;

use crate::error::{OrcError, OutOfSpecSnafu, Result};
use crate::reader::decode::util::{
    extract_run_length_from_header, read_abs_varint, read_ints, read_u8, read_varint_zigzagged,
    rle_v2_decode_bit_width, AbsVarint, AccumulateOp, AddOp, SubOp,
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
    let delta_base = read_abs_varint::<N, _>(reader)?;

    // If width is 0 then all values have fixed delta of delta_base
    if delta_bit_width == 0 {
        match delta_base {
            AbsVarint::Negative(delta) => {
                fixed_delta::<N, SubOp>(out_ints, length, base_value, delta)?;
            }
            AbsVarint::Positive(delta) => {
                fixed_delta::<N, AddOp>(out_ints, length, base_value, delta)?;
            }
        };
    } else {
        match delta_base {
            AbsVarint::Negative(delta) => {
                varied_deltas::<N, R, SubOp>(
                    reader,
                    out_ints,
                    length,
                    base_value,
                    delta,
                    delta_bit_width,
                )?;
            }
            AbsVarint::Positive(delta) => {
                varied_deltas::<N, R, AddOp>(
                    reader,
                    out_ints,
                    length,
                    base_value,
                    delta,
                    delta_bit_width,
                )?;
            }
        };
    }
    Ok(())
}
