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

use crate::error::{OutOfSpecSnafu, Result};
use crate::reader::decode::util::{
    extract_run_length_from_header, read_ints, read_u8, rle_v2_decode_bit_width,
};

use super::{NInt, RleReaderV2};

impl<N: NInt, R: Read> RleReaderV2<N, R> {
    pub fn read_direct_values(&mut self, header: u8) -> Result<()> {
        let encoded_bit_width = (header >> 1) & 0x1F;
        let bit_width = rle_v2_decode_bit_width(encoded_bit_width);

        if (N::BYTE_SIZE * 8) < bit_width {
            return OutOfSpecSnafu {
                msg: "byte width of direct encoding exceeds byte size of integer being decoded to",
            }
            .fail();
        }

        let second_byte = read_u8(&mut self.reader)?;
        let length = extract_run_length_from_header(header, second_byte);

        // Write the unpacked values and zigzag decode to result buffer
        read_ints(&mut self.decoded_ints, length, bit_width, &mut self.reader)?;

        for lit in self.decoded_ints.iter_mut() {
            *lit = lit.zigzag_decode();
        }

        Ok(())
    }
}
