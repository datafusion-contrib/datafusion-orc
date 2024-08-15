// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::error::Result;
use std::io::Read;

use super::util::read_u8;

const MAX_LITERAL_SIZE: usize = 128;
const MIN_REPEAT_SIZE: usize = 3;

pub struct ByteRleIter<R: Read> {
    reader: R,
    literals: [u8; MAX_LITERAL_SIZE],
    num_literals: usize,
    used: usize,
    repeat: bool,
}

impl<R: Read> ByteRleIter<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            literals: [0u8; MAX_LITERAL_SIZE],
            num_literals: 0,
            used: 0,
            repeat: false,
        }
    }

    fn read_byte(&mut self) -> Result<u8> {
        read_u8(&mut self.reader)
    }

    fn read_values(&mut self) -> Result<()> {
        let control = self.read_byte()?;
        self.used = 0;
        if control < 0x80 {
            self.repeat = true;
            self.num_literals = control as usize + MIN_REPEAT_SIZE;
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
        if self.used == self.num_literals {
            match self.read_values() {
                Ok(_) => {}
                Err(_err) => return None,
            }
        }

        let result = if self.repeat {
            self.literals[0]
        } else {
            self.literals[self.used]
        };
        self.used += 1;
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

        let iter = ByteRleIter::new(data).collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(iter, vec![0; 100]);

        let data = [0x01, 0x01];

        let data = &mut data.as_ref();

        let iter = ByteRleIter::new(data).collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(iter, vec![1; 4]);

        let data = [0xfe, 0x44, 0x45];

        let data = &mut data.as_ref();

        let iter = ByteRleIter::new(data).collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(iter, vec![0x44, 0x45]);
    }
}
