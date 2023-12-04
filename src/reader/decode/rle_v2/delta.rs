use std::io::Read;

use crate::error::Result;
use crate::reader::decode::rle_v2::RleReaderV2;
use crate::reader::decode::util::{
    read_ints, read_u8, read_vslong, read_vulong, rle_v2_direct_bit_width,
};

impl<R: Read> RleReaderV2<R> {
    pub fn read_delta_values(&mut self, header: u8) -> Result<()> {
        let fb = (header >> 1) & 0x1f;
        let delta_bit_width = if fb != 0 {
            rle_v2_direct_bit_width(fb)
        } else {
            fb
        };
        let reader = &mut self.reader;

        // 9 bits for length (L) (1 to 512 values)
        // Run length encoded as [0, 511]
        // Adjust to actual value [1, 512]
        // Run length includes base value and first delta
        let second_byte = read_u8(reader)? as u16;
        let length = ((header as u16 & 0x01) << 8) | second_byte;
        let mut length = length as usize;
        length += 1;

        // read the first value stored as vint
        let base_value = if self.signed {
            read_vslong(reader)?
        } else {
            read_vulong(reader)? as i64
        };
        self.literals.push_back(base_value);

        // always signed since can be decreasing sequence
        let delta_base = read_vslong(reader)?;

        // if width is 0 then all values have fixed delta of delta_base
        if delta_bit_width == 0 {
            // skip first value since that's base_value
            (1..length).fold(base_value, |acc, _| {
                let acc = acc + delta_base;
                self.literals.push_back(acc);
                acc
            });
        } else {
            // add delta base and first value
            let second_value = base_value + delta_base;
            self.literals.push_back(second_value);
            length -= 2; // base_value and first delta value

            // unpack the delta values
            read_ints(&mut self.literals, length, delta_bit_width as usize, reader)?;
            self.literals
                .iter_mut()
                // ignore base_value and first delta
                .skip(2)
                // each element is the delta, so find actual value using running accumulator
                .fold(second_value, |acc, delta| {
                    let acc = if delta_base < 0 {
                        acc.saturating_sub(*delta)
                    } else {
                        acc.saturating_add(*delta)
                    };
                    *delta = acc;
                    acc
                });
        }
        Ok(())
    }
}
