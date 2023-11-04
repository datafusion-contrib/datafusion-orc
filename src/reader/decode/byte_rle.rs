use crate::error::Result;
use std::io::Read;

use super::util::read_u8;

const MAX_LITERAL_SIZE: usize = 128;
const MIN_REPEAT_SIZE: usize = 3;

pub struct ByteRleIter<R: Read> {
    reader: R,
    literals: [u8; MAX_LITERAL_SIZE],
    next_byte: Option<u8>,
    num_literals: usize,
    used: usize,
    repeat: bool,
    min_repeat_size: usize,
    remaining: usize,
}

impl<R: Read> ByteRleIter<R> {
    pub fn new(reader: R, length: usize) -> Self {
        Self {
            reader,
            literals: [0u8; MAX_LITERAL_SIZE],
            next_byte: None,
            num_literals: 0,
            used: 0,
            repeat: false,
            min_repeat_size: MIN_REPEAT_SIZE,
            remaining: length,
        }
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    fn read_byte(&mut self) -> Result<u8> {
        if let Some(byt) = self.next_byte.take() {
            Ok(byt)
        } else {
            read_u8(&mut self.reader)
        }
    }

    fn read_values(&mut self) -> Result<()> {
        let control = self.read_byte()?;
        self.used = 0;
        if control < 0x80 {
            self.repeat = true;
            self.num_literals = control as usize + self.min_repeat_size;
            let val = self.read_byte()?;
            self.literals[0] = val;
        } else {
            self.repeat = false;
            self.num_literals = 0x100 - control as usize;
            for i in 0..self.num_literals {
                let result = self.read_byte()?;
                self.literals[i] = result;
            }
        }
        Ok(())
    }
}

impl<R: Read> Iterator for ByteRleIter<R> {
    type Item = Result<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        if self.used == self.num_literals {
            match self.read_values() {
                Ok(_) => {}
                Err(err) => return Some(Err(err)),
            }
        }

        let result = if self.repeat {
            self.literals[0]
        } else {
            self.literals[self.used]
        };
        self.used += 1;
        self.remaining -= 1;
        Some(Ok(result))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn reader_test() {
        let data = [0x61u8, 0x00];

        let data = &mut data.as_ref();

        let iter = ByteRleIter::new(data, 100)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(iter, vec![0; 100]);

        let data = [0x01, 0x01];

        let data = &mut data.as_ref();

        let iter = ByteRleIter::new(data, 4)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(iter, vec![1; 4]);

        let data = [0xfe, 0x44, 0x45];

        let data = &mut data.as_ref();

        let iter = ByteRleIter::new(data, 2)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(iter, vec![0x44, 0x45]);
    }
}
