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

//! Handling decoding of Integer Run Length Encoded V1 data in ORC files

use std::io::Read;

use crate::error::Result;

use super::util::{read_u8, read_vslong, read_vulong, try_read_u8};

/// Hold state of the decoding effort.
enum UnsignedDecodingState {
    /// First created, haven't read anything
    Initial,
    /// Sequence of integers that differ by a fixed delta.
    Run {
        base: u64,
        delta: i64,
        length: usize,
    },
    /// Sequence of `length` varint encoded integers.
    Literals { length: usize },
    /// Exhausted input stream.
    Exhausted,
}

impl UnsignedDecodingState {
    /// Decode header byte to determine sub-encoding.
    /// Runs start with a positive byte, and literals with a negative byte.
    fn get_state<R: Read>(reader: &mut R) -> Result<Self> {
        match try_read_u8(reader)?.map(|byte| byte as i8) {
            Some(byte) if byte < 0 => {
                let length = byte.unsigned_abs() as usize;
                Ok(Self::Literals { length })
            }
            Some(byte) => {
                let byte = byte as u8;
                let length = byte as usize + 3;
                let delta = read_u8(reader)? as i8;
                let delta = delta as i64;
                let base = read_vulong(reader)?;
                Ok(Self::Run {
                    base,
                    delta,
                    length,
                })
            }
            None => Ok(Self::Exhausted),
        }
    }
}

/// Decodes a stream of Integer Run Length Encoded version 1 bytes.
pub struct UnsignedRleReaderV1<R: Read> {
    reader: R,
    state: UnsignedDecodingState,
}

impl<R: Read> UnsignedRleReaderV1<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            state: UnsignedDecodingState::Initial,
        }
    }

    fn iter_helper(&mut self) -> Result<Option<u64>> {
        match self.state {
            UnsignedDecodingState::Initial => {
                self.state = UnsignedDecodingState::get_state(&mut self.reader)?;
                // this is safe as UnsignedDecodingState::Initial is only ever set in new()
                self.iter_helper()
            }
            UnsignedDecodingState::Run {
                base,
                delta,
                length,
            } => {
                let num = base;
                if length == 1 {
                    self.state = UnsignedDecodingState::get_state(&mut self.reader)?;
                } else {
                    self.state = UnsignedDecodingState::Run {
                        base: base.saturating_add_signed(delta),
                        delta,
                        length: length - 1,
                    };
                }
                Ok(Some(num))
            }
            UnsignedDecodingState::Literals { length } => {
                let num = read_vulong(&mut self.reader)?;
                if length == 1 {
                    self.state = UnsignedDecodingState::get_state(&mut self.reader)?;
                } else {
                    self.state = UnsignedDecodingState::Literals { length: length - 1 };
                }
                Ok(Some(num))
            }
            UnsignedDecodingState::Exhausted => Ok(None),
        }
    }
}

impl<R: Read> Iterator for UnsignedRleReaderV1<R> {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_helper().transpose()
    }
}

/// Signed version of [`UnsignedDecodingState`].
enum SignedDecodingState {
    Initial,
    Run {
        base: i64,
        delta: i64,
        length: usize,
    },
    Literals {
        length: usize,
    },
    Exhausted,
}

impl SignedDecodingState {
    fn get_state<R: Read>(reader: &mut R) -> Result<Self> {
        match try_read_u8(reader)?.map(|byte| byte as i8) {
            Some(byte) if byte < 0 => {
                let length = byte.unsigned_abs() as usize;
                Ok(Self::Literals { length })
            }
            Some(byte) => {
                let byte = byte as u8;
                let length = byte as usize + 3;
                let delta = read_u8(reader)? as i8;
                let delta = delta as i64;
                let base = read_vslong(reader)?;
                Ok(Self::Run {
                    base,
                    delta,
                    length,
                })
            }
            None => Ok(Self::Exhausted),
        }
    }
}

/// Signed version of [`UnsignedIntV1RLEDecoder`].
pub struct SignedRleReaderV1<R: Read> {
    reader: R,
    state: SignedDecodingState,
}

impl<R: Read> SignedRleReaderV1<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            state: SignedDecodingState::Initial,
        }
    }

    fn iter_helper(&mut self) -> Result<Option<i64>> {
        match self.state {
            SignedDecodingState::Initial => {
                self.state = SignedDecodingState::get_state(&mut self.reader)?;
                self.iter_helper()
            }
            SignedDecodingState::Run {
                base,
                delta,
                length,
            } => {
                let num = base;
                if length == 1 {
                    self.state = SignedDecodingState::get_state(&mut self.reader)?;
                } else {
                    self.state = SignedDecodingState::Run {
                        base: base.saturating_add(delta),
                        delta,
                        length: length - 1,
                    };
                }
                Ok(Some(num))
            }
            SignedDecodingState::Literals { length } => {
                let num = read_vslong(&mut self.reader)?;
                if length == 1 {
                    self.state = SignedDecodingState::get_state(&mut self.reader)?;
                } else {
                    self.state = SignedDecodingState::Literals { length: length - 1 };
                }
                Ok(Some(num))
            }
            SignedDecodingState::Exhausted => Ok(None),
        }
    }
}

impl<R: Read> Iterator for SignedRleReaderV1<R> {
    type Item = Result<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_helper().transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_run() -> Result<()> {
        let input = [0x61, 0x00, 0x07];
        let decoder = UnsignedRleReaderV1::new(Cursor::new(&input));
        let expected = vec![7; 100];
        let actual = decoder.collect::<Result<Vec<_>>>()?;
        assert_eq!(actual, expected);

        let input = [0x61, 0xff, 0x64];
        let decoder = UnsignedRleReaderV1::new(Cursor::new(&input));
        let expected = (1..=100).rev().collect::<Vec<_>>();
        let actual = decoder.collect::<Result<Vec<_>>>()?;
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_literal() -> Result<()> {
        let input = [0xfb, 0x02, 0x03, 0x06, 0x07, 0xb];
        let decoder = UnsignedRleReaderV1::new(Cursor::new(&input));
        let expected = vec![2, 3, 6, 7, 11];
        let actual = decoder.collect::<Result<Vec<_>>>()?;
        assert_eq!(actual, expected);

        Ok(())
    }
}
