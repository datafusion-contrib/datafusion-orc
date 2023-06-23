use std::io::Read;

use snafu::ensure;

use crate::error::{self, Result};
use crate::reader::decode::rle_v2::RleReaderV2;
use crate::reader::decode::util::{
    read_ints, read_u8, read_vslong, read_vulong, rle_v2_delta_bit_width,
};

impl<R: Read> RleReaderV2<R> {
    pub fn read_delta_values(&mut self, header: u8) -> Result<()> {
        let mut fb = (header >> 1) & 0x1f;
        if fb != 0 {
            fb = rle_v2_delta_bit_width(fb);
        }
        let reader = &mut self.reader;
        let signed = self.signed;

        // 9 bits for length (L) (1 to 512 values)
        let second_byte = read_u8(reader)?;

        let mut length = ((header as u64 & 0x01) << 8) as usize | second_byte as usize;

        // read the first value stored as vint
        let first_val = if signed {
            read_vslong(reader)?
        } else {
            read_vulong(reader)?
        };

        self.literals[self.num_literals] = first_val;
        self.num_literals += 1;

        // if fixed bits is 0 then all values have fixed delta
        if fb == 0 {
            // read the fixed delta value stored as vint (deltas can be negative even
            // if all number are positive)
            let fd = read_vslong(reader)?;

            if fd == 0 {
                ensure!(
                    self.num_literals == 1,
                    error::UnexpectedSnafu {
                        msg: "expected numLiterals to equal 1"
                    }
                );
                for i in self.num_literals..self.num_literals + length {
                    self.literals[i] = self.literals[0];
                }
                self.num_literals += length;
            } else {
                // add fixed deltas to adjacent values
                for _ in 0..length {
                    self.literals[self.num_literals] = self.literals[self.num_literals - 1] + fd;
                    self.num_literals += 1;
                }
            }
        } else {
            let delta_base = read_vslong(reader)?;
            // add delta base and first value
            self.literals[self.num_literals] = first_val + delta_base;
            self.num_literals += 1;
            let mut prev_val = self.literals[self.num_literals - 1];
            length -= 1;

            // write the unpacked values, add it to previous value and store final
            // value to result buffer. if the delta base value is negative then it
            // is a decreasing sequence else an increasing sequence
            read_ints(
                &mut self.literals,
                self.num_literals,
                length,
                fb as usize,
                reader,
            )?;
            while length > 0 {
                if delta_base < 0 {
                    self.literals[self.num_literals] = prev_val - self.literals[self.num_literals];
                } else {
                    self.literals[self.num_literals] += prev_val
                }
                prev_val = self.literals[self.num_literals];
                length -= 1;
                self.num_literals += 1;
            }
        }
        Ok(())
    }
}
