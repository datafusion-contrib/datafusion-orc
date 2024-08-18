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

use crate::{error::Result, reader::decode::util::read_varint_zigzagged};

use super::SignedEncoding;

/// Read stream of zigzag encoded varints as i128 (unbound).
pub struct UnboundedVarintStreamDecoder<R: Read> {
    reader: R,
    remaining: usize,
}

impl<R: Read> UnboundedVarintStreamDecoder<R> {
    pub fn new(reader: R, expected_length: usize) -> Self {
        Self {
            reader,
            remaining: expected_length,
        }
    }
}

impl<R: Read> Iterator for UnboundedVarintStreamDecoder<R> {
    type Item = Result<i128>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.remaining > 0).then(|| {
            self.remaining -= 1;
            read_varint_zigzagged::<i128, _, SignedEncoding>(&mut self.reader)
        })
    }
}
