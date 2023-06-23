use std::io::Read;

use snafu::ensure;

use crate::error::{self, Result};
use crate::reader::decode::rle_v2::RleReaderV2;
use crate::reader::decode::util::{bytes_to_long_be, zigzag_decode};

// MIN_REPEAT_SIZE is the minimum number of repeated values required to use run length encoding.
const MIN_REPEAT_SIZE: usize = 3;

impl<R: Read> RleReaderV2<R> {
    pub fn read_short_repeat_values(&mut self, header: u8) -> Result<()> {
        let size = ((header as u64) >> 3) & 0x07;
        let size = size + 1;

        let mut l = (header & 0x07) as usize;
        l += MIN_REPEAT_SIZE;

        let val = bytes_to_long_be(&mut self.reader, size as usize)?;

        let val = if self.signed {
            zigzag_decode(val as u64)
        } else {
            val
        };

        ensure!(
            self.num_literals == 0,
            error::UnexpectedSnafu {
                msg: "readValues called with existing values present"
            }
        );

        for i in 0..l {
            self.literals[i] = val;
        }
        self.num_literals = l;

        Ok(())
    }
}
