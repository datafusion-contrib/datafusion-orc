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

use std::io::Read;

use crate::error::Result;

use super::byte_rle::ByteRleIter;

pub struct BooleanIter<R: Read> {
    iter: ByteRleIter<R>,
    data: u8,
    bits_in_data: usize,
}

impl<R: Read> BooleanIter<R> {
    pub fn new(reader: R) -> Self {
        Self {
            iter: ByteRleIter::new(reader),
            bits_in_data: 0,
            data: 0,
        }
    }

    pub fn value(&mut self) -> bool {
        let value = (self.data & 0x80) != 0;
        self.data <<= 1;
        self.bits_in_data -= 1;

        value
    }
}

impl<R: Read> Iterator for BooleanIter<R> {
    type Item = Result<bool>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // read more data if necessary
        if self.bits_in_data == 0 {
            match self.iter.next() {
                Some(Ok(data)) => {
                    self.data = data;
                    self.bits_in_data = 8;
                    Some(Ok(self.value()))
                }
                Some(Err(err)) => Some(Err(err)),
                None => None,
            }
        } else {
            Some(Ok(self.value()))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let data = [0x61u8, 0x00];

        let data = &mut data.as_ref();

        let iter = BooleanIter::new(data).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(iter, vec![false; 800])
    }

    #[test]
    fn literals() {
        let data = [0xfeu8, 0b01000100, 0b01000101];

        let data = &mut data.as_ref();

        let iter = BooleanIter::new(data).collect::<Result<Vec<_>>>().unwrap();
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

        let iter = BooleanIter::new(data).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(
            iter,
            vec![true, false, false, false, false, false, false, false]
        )
    }
}
