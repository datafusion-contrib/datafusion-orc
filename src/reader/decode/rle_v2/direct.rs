use std::io::Read;

use crate::error::Result;
use crate::reader::decode::rle_v2::RleReaderV2;
use crate::reader::decode::util::{read_ints, read_u8, rle_v2_direct_bit_width, zigzag_decode};

impl<R: Read> RleReaderV2<R> {
    pub fn read_direct_values(&mut self, header: u8) -> Result<()> {
        let fbo = (header >> 1) & 0x1F;
        let fb = rle_v2_direct_bit_width(fbo);

        // 9 bits for length (L) (1 to 512 values)
        let second_byte = read_u8(&mut self.reader)?;

        let mut length = ((header as u64 & 0x01) << 8) as usize | second_byte as usize;
        // runs are one off
        length += 1;

        // write the unpacked values and zigzag decode to result buffer
        read_ints(&mut self.literals, length, fb as usize, &mut self.reader)?;

        if self.signed {
            for lit in self.literals.iter_mut() {
                *lit = zigzag_decode(*lit as u64);
            }
        }

        Ok(())
    }
}
