pub mod delta;
pub mod direct;
pub mod patched_base;
pub mod short_repeat;
use std::io::{ErrorKind, Read};

use crate::error::{self, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EncodingTypeV2 {
    ShortRepeat,
    Direct,
    PatchedBase,
    Delta,
}

#[inline]
fn run_encoding(header: u8) -> EncodingTypeV2 {
    match (header & 128 == 128, header & 64 == 64) {
        // 11... = 3
        (true, true) => EncodingTypeV2::Delta,
        // 10... = 2
        (true, false) => EncodingTypeV2::PatchedBase,
        // 01... = 1
        (false, true) => EncodingTypeV2::Direct,
        // 00... = 0
        (false, false) => EncodingTypeV2::ShortRepeat,
    }
}

pub struct RleReaderV2<R> {
    reader: R,
    signed: bool,
    literals: Vec<i64>,
    num_literals: usize,
    used: usize,
    skip_corrupt: bool,
}

const MAX_SCOPE: usize = 512;

impl<R: Read> RleReaderV2<R> {
    pub fn try_new(reader: R, signed: bool, skip_corrupt: bool) -> Self {
        Self {
            reader,
            num_literals: 0,
            signed,
            literals: vec![0; MAX_SCOPE],
            used: 0,
            skip_corrupt,
        }
    }

    // returns false if no more bytes
    pub fn read_values(&mut self, ignore_eof: bool) -> Result<bool> {
        let mut byte = [0u8];

        if let Err(err) = self.reader.read_exact(&mut byte) {
            return match err.kind() {
                ErrorKind::UnexpectedEof => {
                    if !ignore_eof {
                        Ok(false)
                    } else {
                        error::UnexpectedSnafu {
                            msg: "Read past end of RLE integer reader",
                        }
                        .fail()
                    }
                }
                _ => error::UnexpectedSnafu {
                    msg: err.to_string(),
                }
                .fail(),
            };
        }

        let header = byte[0];
        let encoding = run_encoding(header);

        match encoding {
            EncodingTypeV2::ShortRepeat => self.read_short_repeat_values(header)?,
            EncodingTypeV2::Direct => self.read_direct_values(header)?,
            EncodingTypeV2::PatchedBase => self.read_patched_base(header)?,
            EncodingTypeV2::Delta => self.read_delta_values(header)?,
        }

        Ok(true)
    }
}

impl<R: Read> Iterator for RleReaderV2<R> {
    type Item = Result<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.used == self.num_literals {
            self.used = 0;
            self.num_literals = 0;

            match self.read_values(false) {
                Ok(more) => {
                    if !more {
                        return None;
                    }
                }
                Err(err) => {
                    return Some(Err(err));
                }
            }
        }
        let result = self.literals[self.used];
        self.used += 1;

        Some(Ok(result))
    }
}

pub struct UnsignedRleReaderV2<R>(RleReaderV2<R>);

impl<R: Read> UnsignedRleReaderV2<R> {
    pub fn try_new(reader: R, skip_corrupt: bool) -> Self {
        Self(RleReaderV2::try_new(reader, false, skip_corrupt))
    }
}

impl<R: Read> Iterator for UnsignedRleReaderV2<R> {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|v| v.map(|v| v as u64))
    }
}

#[cfg(test)]
mod test {

    use std::io::Cursor;

    use super::*;

    #[test]
    fn reader_test() {
        let data = [2u8, 1, 64, 5, 80, 1, 1];
        let expected = [1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 1];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();
        assert_eq!(a, expected);

        // direct
        let data = [0x5eu8, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];
        let expected = [23713, 43806, 57005, 48879];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();
        assert_eq!(a, expected);

        // patched base
        let data = [
            102u8, 9, 0, 126, 224, 7, 208, 0, 126, 79, 66, 64, 0, 127, 128, 8, 2, 0, 128, 192, 8,
            22, 0, 130, 0, 8, 42,
        ];

        let expected = [
            2030u64, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090,
        ];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();
        assert_eq!(a, expected);

        let data = [196u8, 9, 2, 2, 74, 40, 166];
        let expected = [2u64, 3, 5, 7, 11, 13, 17, 19, 23, 29];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();
        assert_eq!(a, expected);

        let data = [0xc6u8, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];
        let expected = [2u64, 3, 5, 7, 11, 13, 17, 19, 23, 29];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();
        assert_eq!(a, expected);

        let data = [7u8, 1];
        let expected = [1u64, 1, 1, 1, 1, 1, 1, 1, 1, 1];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();
        assert_eq!(a, expected);
    }

    #[test]
    fn short_repeat() {
        // [10000, 10000, 10000, 10000, 10000]
        let data: [u8; 3] = [0x0a, 0x27, 0x10];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();

        assert_eq!(a, vec![10000, 10000, 10000, 10000, 10000]);
    }

    #[test]
    fn direct() {
        // [23713, 43806, 57005, 48879]
        let data: [u8; 10] = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();

        assert_eq!(a, vec![23713, 43806, 57005, 48879]);
    }

    #[test]
    fn direct_signed() {
        // [23713, 43806, 57005, 48879]
        let data = [110u8, 3, 0, 185, 66, 1, 86, 60, 1, 189, 90, 1, 125, 222];

        let cursor = Cursor::new(data);
        let mut reader = RleReaderV2::try_new(cursor, true, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();

        assert_eq!(a, vec![23713, 43806, 57005, 48879]);
    }

    #[test]
    fn delta() {
        // [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
        // 0x22 = 34
        // 0x42 = 66
        // 0x46 = 70
        let data: [u8; 8] = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];

        let cursor = Cursor::new(data);
        let mut reader = UnsignedRleReaderV2::try_new(cursor, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();

        assert_eq!(a, vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);
    }

    #[test]
    fn patched_base() {
        let data = vec![
            0x8eu8, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46,
            0x50, 0x5a, 0xfc, 0xe8,
        ];

        let expected = vec![
            2030u64, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090,
        ];

        let cursor = Cursor::new(data);
        let mut reader = RleReaderV2::try_new(cursor, false, false);
        let a = reader
            .try_collect::<Vec<_>>()
            .unwrap()
            .into_iter()
            .map(|v| v as u64)
            .collect::<Vec<_>>();

        assert_eq!(a, expected);
    }

    #[test]
    fn patched_base_1() {
        let data = vec![
            144u8, 109, 4, 164, 141, 16, 131, 194, 0, 240, 112, 64, 60, 84, 24, 3, 193, 201, 128,
            120, 60, 33, 4, 244, 3, 193, 192, 224, 128, 56, 32, 15, 22, 131, 129, 225, 0, 112, 84,
            86, 14, 8, 106, 193, 192, 228, 160, 64, 32, 14, 213, 131, 193, 192, 240, 121, 124, 30,
            18, 9, 132, 67, 0, 224, 120, 60, 28, 14, 32, 132, 65, 192, 240, 160, 56, 61, 91, 7, 3,
            193, 192, 240, 120, 76, 29, 23, 7, 3, 220, 192, 240, 152, 60, 52, 15, 7, 131, 129, 225,
            0, 144, 56, 30, 14, 44, 140, 129, 194, 224, 120, 0, 28, 15, 8, 6, 129, 198, 144, 128,
            104, 36, 27, 11, 38, 131, 33, 48, 224, 152, 60, 111, 6, 183, 3, 112, 0, 1, 78, 5, 46,
            2, 1, 1, 141, 3, 1, 1, 138, 22, 0, 65, 1, 4, 0, 225, 16, 209, 192, 4, 16, 8, 36, 16, 3,
            48, 1, 3, 13, 33, 0, 176, 0, 1, 94, 18, 0, 68, 0, 33, 1, 143, 0, 1, 7, 93, 0, 25, 0, 5,
            0, 2, 0, 4, 0, 1, 0, 1, 0, 2, 0, 16, 0, 1, 11, 150, 0, 3, 0, 1, 0, 1, 99, 157, 0, 1,
            140, 54, 0, 162, 1, 130, 0, 16, 112, 67, 66, 0, 2, 4, 0, 0, 224, 0, 1, 0, 16, 64, 16,
            91, 198, 1, 2, 0, 32, 144, 64, 0, 12, 2, 8, 24, 0, 64, 0, 1, 0, 0, 8, 48, 51, 128, 0,
            2, 12, 16, 32, 32, 71, 128, 19, 76,
        ];

        let expected = vec![
            20i64, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2, 3, 1783, 475, 2, 1, 1, 3, 1, 3, 2,
            32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1, 1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4,
            11, 1, 2, 2, 1, 1, 52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2,
            6, 2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -13, 1, 2, 3, 13, 1, 92, 3,
            13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1, 141, 3, 1, 1, 1, 1, 2, 1, 4,
            34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4, 13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399,
            1, 1885, 25, 5, 2, 4, 1, 1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131,
            14, 5, 1, 2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1, 1, 13,
            2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2, 2, 16,
        ];

        let cursor = Cursor::new(data);
        let mut reader = RleReaderV2::try_new(cursor, false, false);
        let a = reader.try_collect::<Vec<_>>().unwrap();

        assert_eq!(a.len(), expected.len());

        assert_eq!(a, expected);
    }
}
