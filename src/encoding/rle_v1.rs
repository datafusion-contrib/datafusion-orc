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

use std::{io::Read, marker::PhantomData};

use snafu::OptionExt;

use crate::error::{OutOfSpecSnafu, Result};

use super::{
    util::{read_u8, read_varint_zigzagged, try_read_u8},
    EncodingSign, NInt, PrimitiveValueDecoder,
};

const MAX_RUN_LENGTH: usize = 130;

/// Decodes a stream of Integer Run Length Encoded version 1 bytes.
pub struct RleReaderV1<N: NInt, R: Read, S: EncodingSign> {
    reader: R,
    decoded_ints: Vec<N>,
    current_head: usize,
    phantom: PhantomData<S>,
}

impl<N: NInt, R: Read, S: EncodingSign> RleReaderV1<N, R, S> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            decoded_ints: Vec::with_capacity(MAX_RUN_LENGTH),
            current_head: 0,
            phantom: Default::default(),
        }
    }

    fn decode_batch(&mut self) -> Result<()> {
        self.current_head = 0;
        self.decoded_ints.clear();
        match try_read_u8(&mut self.reader)?.map(|byte| byte as i8) {
            // Literals
            Some(byte) if byte < 0 => {
                let length = byte.unsigned_abs();
                for _ in 0..length {
                    let lit = read_varint_zigzagged::<_, _, S>(&mut self.reader)?;
                    self.decoded_ints.push(lit);
                }
                Ok(())
            }
            // Run
            Some(byte) => {
                let byte = byte as u8;
                let length = byte + 2; // Technically +3, but we subtract 1 for the base
                let delta = read_u8(&mut self.reader)? as i8;
                let mut base = read_varint_zigzagged::<_, _, S>(&mut self.reader)?;
                self.decoded_ints.push(base);
                if delta < 0 {
                    let delta = delta.unsigned_abs();
                    let delta = N::from_u8(delta);
                    for _ in 0..length {
                        base = base.checked_sub(&delta).context(OutOfSpecSnafu {
                            msg: "over/underflow when decoding patched base integer",
                        })?;
                        self.decoded_ints.push(base);
                    }
                } else {
                    let delta = delta as u8;
                    let delta = N::from_u8(delta);
                    for _ in 0..length {
                        base = base.checked_add(&delta).context(OutOfSpecSnafu {
                            msg: "over/underflow when decoding patched base integer",
                        })?;
                        self.decoded_ints.push(base);
                    }
                }
                Ok(())
            }
            None => Ok(()),
        }
    }
}

impl<N: NInt, R: Read, S: EncodingSign> PrimitiveValueDecoder<N> for RleReaderV1<N, R, S> {
    // TODO: this is exact duplicate from RLEv2 version; deduplicate it
    fn decode(&mut self, out: &mut [N]) -> Result<()> {
        let available = &self.decoded_ints[self.current_head..];
        // If we have enough in buffer to copy over
        if available.len() >= out.len() {
            out.copy_from_slice(&available[..out.len()]);
            self.current_head += out.len();
            return Ok(());
        }

        // Otherwise progressively copy over chunks
        let len_to_copy = out.len();
        let mut copied = 0;
        while copied < len_to_copy {
            let copying = self.decoded_ints.len() - self.current_head;
            // At most, we fill to exact length of output buffer (don't overflow)
            let copying = copying.min(len_to_copy - copied);

            let target_out_slice = &mut out[copied..copied + copying];
            target_out_slice.copy_from_slice(
                &self.decoded_ints[self.current_head..self.current_head + copying],
            );

            copied += copying;
            self.current_head += copying;

            if self.current_head == self.decoded_ints.len() {
                self.decode_batch()?;
            }
        }

        if copied != out.len() {
            // TODO: more descriptive error
            OutOfSpecSnafu {
                msg: "Array length less than expected",
            }
            .fail()
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::encoding::UnsignedEncoding;

    use super::*;

    fn test_helper(data: &[u8], expected: &[i64]) {
        let mut reader = RleReaderV1::<i64, _, UnsignedEncoding>::new(Cursor::new(data));
        let mut actual = vec![0; expected.len()];
        reader.decode(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_run() -> Result<()> {
        let data = [0x61, 0x00, 0x07];
        let expected = [7; 100];
        test_helper(&data, &expected);

        let data = [0x61, 0xff, 0x64];
        let expected = (1..=100).rev().collect::<Vec<_>>();
        test_helper(&data, &expected);

        Ok(())
    }

    #[test]
    fn test_literal() -> Result<()> {
        let data = [0xfb, 0x02, 0x03, 0x06, 0x07, 0xb];
        let expected = vec![2, 3, 6, 7, 11];
        test_helper(&data, &expected);

        Ok(())
    }
}
