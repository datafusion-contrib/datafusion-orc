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

use crate::{
    encoding::{
        rle::GenericRle,
        util::{read_u8, try_read_u8},
    },
    error::{OutOfSpecSnafu, Result},
};

use super::{util::read_varint_zigzagged, EncodingSign, NInt};

const MAX_RUN_LENGTH: usize = 130;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EncodingType {
    Run { length: usize, delta: i8 },
    Literals { length: usize },
}

impl EncodingType {
    /// Decode header byte to determine sub-encoding.
    /// Runs start with a positive byte, and literals with a negative byte.
    fn from_header<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let opt_encoding = match try_read_u8(reader)?.map(|b| b as i8) {
            Some(header) if header < 0 => {
                let length = header.unsigned_abs() as usize;
                Some(Self::Literals { length })
            }
            Some(header) => {
                let length = header as u8 as usize + 3;
                let delta = read_u8(reader)? as i8;
                Some(Self::Run { length, delta })
            }
            None => None,
        };
        Ok(opt_encoding)
    }
}

/// Decodes a stream of Integer Run Length Encoded version 1 bytes.
pub struct RleV1Decoder<N: NInt, R: Read, S: EncodingSign> {
    reader: R,
    decoded_ints: Vec<N>,
    current_head: usize,
    sign: PhantomData<S>,
}

impl<N: NInt, R: Read, S: EncodingSign> RleV1Decoder<N, R, S> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            decoded_ints: Vec::with_capacity(MAX_RUN_LENGTH),
            current_head: 0,
            sign: Default::default(),
        }
    }
}

fn read_literals<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    length: usize,
) -> Result<()> {
    for _ in 0..length {
        let lit = read_varint_zigzagged::<_, _, S>(reader)?;
        out_ints.push(lit);
    }
    Ok(())
}

fn read_run<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    length: usize,
    delta: i8,
) -> Result<()> {
    let mut base = read_varint_zigzagged::<_, _, S>(reader)?;
    // Account for base value
    let length = length - 1;
    out_ints.push(base);
    if delta < 0 {
        let delta = delta.unsigned_abs();
        let delta = N::from_u8(delta);
        for _ in 0..length {
            base = base.checked_sub(&delta).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding patched base integer",
            })?;
            out_ints.push(base);
        }
    } else {
        let delta = delta as u8;
        let delta = N::from_u8(delta);
        for _ in 0..length {
            base = base.checked_add(&delta).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding patched base integer",
            })?;
            out_ints.push(base);
        }
    }
    Ok(())
}

impl<N: NInt, R: Read, S: EncodingSign> GenericRle<N> for RleV1Decoder<N, R, S> {
    fn advance(&mut self, n: usize) {
        self.current_head += n;
    }

    fn available(&self) -> &[N] {
        &self.decoded_ints[self.current_head..]
    }

    fn decode_batch(&mut self) -> Result<()> {
        self.current_head = 0;
        self.decoded_ints.clear();

        match EncodingType::from_header(&mut self.reader)? {
            Some(EncodingType::Literals { length }) => {
                read_literals::<_, _, S>(&mut self.reader, &mut self.decoded_ints, length)
            }
            Some(EncodingType::Run { length, delta }) => {
                read_run::<_, _, S>(&mut self.reader, &mut self.decoded_ints, length, delta)
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::encoding::{integer::UnsignedEncoding, PrimitiveValueDecoder};

    use super::*;

    fn test_helper(data: &[u8], expected: &[i64]) {
        let mut reader = RleV1Decoder::<i64, _, UnsignedEncoding>::new(Cursor::new(data));
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
