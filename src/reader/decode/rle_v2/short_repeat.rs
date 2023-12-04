use std::io::Read;

use crate::error::Result;
use crate::reader::decode::rle_v2::RleReaderV2;
use crate::reader::decode::util::{bytes_to_long_be, zigzag_decode};

/// Minimum number of repeated values required to use run length encoding.
const MIN_REPEAT_SIZE: usize = 3;

impl<R: Read> RleReaderV2<R> {
    pub fn read_short_repeat_values(&mut self, header: u8) -> Result<()> {
        // Header byte:
        //
        // eeww_wccc
        // 7       0 LSB
        //
        // ee  = Sub-encoding bits, always 00
        // www = Value width bits
        // ccc = Repeat count bits

        let value_byte_width = (header >> 3) & 0x07; // Encoded as 0 to 7
        let value_byte_width = value_byte_width as usize + 1; // Decode to 1 to 8 bytes

        let run_length = (header & 0x07) as usize;
        let run_length = run_length + MIN_REPEAT_SIZE;

        // Value that is being repeated is encoded as N bytes in big endian format
        // Where N = value_byte_width
        let val = bytes_to_long_be(&mut self.reader, value_byte_width)?;

        let val = if self.signed {
            zigzag_decode(val as u64)
        } else {
            val
        };

        for _ in 0..run_length {
            self.literals.push_back(val);
        }

        Ok(())
    }
}
