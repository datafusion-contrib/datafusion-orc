use std::io::Read;

use snafu::ResultExt;

use crate::error::{self, Result};
use crate::reader::decode::util::read_u8;

#[derive(Debug, Copy, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum BooleanRun {
    Run(u8, u16),
    Literals([u8; 255]),
}

pub struct BooleanRleRunIter<R: Read> {
    reader: R,
}

impl<R: Read> BooleanRleRunIter<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

fn read_literals<R: Read>(reader: &mut R, header: i8) -> Result<[u8; 255]> {
    let length = (-header) as usize;

    let mut literals = [0u8; 255];

    reader
        .take(length as u64)
        .read_exact(&mut literals[..length])
        .context(error::IoSnafu)?;

    Ok(literals)
}

impl<R: Read> Iterator for BooleanRleRunIter<R> {
    type Item = Result<BooleanRun>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let header = match read_u8(&mut self.reader) {
            Ok(header) => header as i8,
            Err(e) => return Some(Err(e)),
        };
        if header < 0 {
            Some(read_literals(&mut self.reader, header).map(BooleanRun::Literals))
        } else {
            let length = header as u16 + 3;
            // this is not ok - it may require more than one byte
            let value = match read_u8(&mut self.reader) {
                Ok(value) => value,
                Err(e) => return Some(Err(e)),
            };
            Some(Ok(BooleanRun::Run(value, length)))
        }
    }
}

pub struct BooleanIter<R: Read> {
    iter: BooleanRleRunIter<R>,
    current: Option<BooleanRun>,
    position: u8,
    byte_position: usize,
    remaining: usize,
}

impl<R: Read> BooleanIter<R> {
    pub fn new(reader: R, length: usize) -> Self {
        Self {
            iter: BooleanRleRunIter::new(reader),
            current: None,
            position: 0,
            byte_position: 0,
            remaining: length,
        }
    }

    pub fn into_inner(self) -> R {
        self.iter.into_inner()
    }
}

impl<R: Read> Iterator for BooleanIter<R> {
    type Item = Result<bool>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(run) = &self.current {
            match run {
                BooleanRun::Run(value, repetitions) => {
                    let repetitions = *repetitions;
                    let mask = 128u8 >> self.position;
                    let result = value & mask == mask;
                    self.position += 1;
                    if self.remaining == 0 {
                        self.current = None;
                        return None;
                    } else {
                        self.remaining -= 1;
                    }
                    if self.position == 8 {
                        if repetitions == 0 {
                            self.current = None;
                        } else {
                            self.current = Some(BooleanRun::Run(*value, repetitions - 1));
                        }
                        self.position = 0;
                    }
                    Some(Ok(result))
                }
                BooleanRun::Literals(bytes) => {
                    let mask = 128u8 >> self.position;
                    let result = bytes[self.byte_position] & mask == mask;
                    self.position += 1;
                    if self.remaining == 0 {
                        self.current = None;
                        return None;
                    } else {
                        self.remaining -= 1;
                    }
                    if self.position == 8 {
                        if bytes.len() == 1 {
                            self.current = None;
                            self.byte_position = 0;
                        } else {
                            self.byte_position += 1;
                        }
                        self.position = 0;
                    }
                    Some(Ok(result))
                }
            }
        } else if self.remaining > 0 {
            match self.iter.next()? {
                Ok(run) => {
                    self.current = Some(run);
                    self.next()
                }
                Err(e) => {
                    self.remaining = 0;
                    Some(Err(e))
                }
            }
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let data = [0x61u8, 0x00];

        let data = &mut data.as_ref();

        let iter = BooleanIter::new(data, 100)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(iter, vec![false; 100])
    }

    #[test]
    fn literals() {
        let data = [0xfeu8, 0b01000100, 0b01000101];

        let data = &mut data.as_ref();

        let iter = BooleanIter::new(data, 16)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            iter,
            vec![
                false, true, false, false, false, true, false, false, // 0b01000100
                false, true, false, false, false, true, false, true, // 0b01000101
            ]
        )
    }

    #[test]
    fn another() {
        // "For example, the byte sequence [0xff, 0x80] would be one true followed by seven false values."
        let data = [0xff, 0x80];

        let data = &mut data.as_ref();

        let iter = BooleanIter::new(data, 8)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            iter,
            vec![true, false, false, false, false, false, false, false,]
        )
    }
}
