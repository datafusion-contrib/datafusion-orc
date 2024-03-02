use std::io::Read;

use snafu::ResultExt;

use crate::error::{IoSnafu, OutOfSpecSnafu, Result};

use super::{NInt, RleReaderV2};

/// Minimum number of repeated values required to use this sub-encoding
const MIN_REPEAT_SIZE: usize = 3;

impl<N: NInt, R: Read> RleReaderV2<N, R> {
    pub fn read_short_repeat_values(&mut self, header: u8) -> Result<()> {
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
                msg: "byte width of short repeat encoding exceeds byte size of integer being decoded to",
            }
            .fail();
        }

        let run_length = (header & 0x07) as usize + MIN_REPEAT_SIZE;

        // Value that is being repeated is encoded as value_byte_width bytes in big endian format
        let mut buffer = N::empty_byte_array();
        // Read into back part of buffer since is big endian.
        // So if smaller than N::BYTE_SIZE bytes, most significant bytes will be 0.
        self.reader
            .read_exact(&mut buffer.as_mut()[N::BYTE_SIZE - byte_width..])
            .context(IoSnafu)?;
        let val = N::from_be_bytes(buffer).zigzag_decode();

        self.decoded_ints
            .extend(std::iter::repeat(val).take(run_length));

        Ok(())
    }
}
