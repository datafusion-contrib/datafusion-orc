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

use snafu::{OptionExt, ResultExt};

use super::{NInt, RleReaderV2};
use crate::error::{IoSnafu, OutOfSpecSnafu, Result};
use crate::reader::decode::util::{
    extract_run_length_from_header, read_ints, read_u8, rle_v2_decode_bit_width,
};

/// Patches (gap + actual patch bits) width are ceil'd here.
///
/// Not mentioned in ORC specification, but happens in their implementation.
fn get_closest_fixed_bits(width: usize) -> usize {
    match width {
        1..=24 => width,
        25..=26 => 26,
        27..=28 => 28,
        29..=30 => 30,
        31..=32 => 32,
        33..=40 => 40,
        41..=48 => 48,
        49..=56 => 56,
        57..=64 => 64,
        _ => unreachable!(),
    }
}

impl<N: NInt, R: Read> RleReaderV2<N, R> {
    pub fn read_patched_base(&mut self, header: u8) -> Result<()> {
        let encoded_bit_width = (header >> 1) & 0x1F;
        let value_bit_width = rle_v2_decode_bit_width(encoded_bit_width);
        let value_bit_width_u32 = u32::try_from(value_bit_width).or_else(|_| {
            OutOfSpecSnafu {
                msg: "value_bit_width overflows u32",
            }
            .fail()
        })?;

        let second_byte = read_u8(&mut self.reader)?;
        let length = extract_run_length_from_header(header, second_byte);

        let third_byte = read_u8(&mut self.reader)?;
        let fourth_byte = read_u8(&mut self.reader)?;

        // Base width is one off
        let base_byte_width = ((third_byte >> 5) & 0x07) as usize + 1;

        let patch_bit_width = rle_v2_decode_bit_width(third_byte & 0x1f);

        // Patch gap width is one off
        let patch_gap_bit_width = ((fourth_byte >> 5) & 0x07) as usize + 1;

        let patch_total_bit_width = patch_bit_width + patch_gap_bit_width;
        if patch_total_bit_width > 64 {
            return OutOfSpecSnafu {
                msg: "combined patch width and patch gap width cannot be greater than 64 bits",
            }
            .fail();
        }

        let patch_list_length = (fourth_byte & 0x1f) as usize;

        let mut buffer = N::empty_byte_array();
        // Read into back part of buffer since is big endian.
        // So if smaller than N::BYTE_SIZE bytes, most significant bytes will be 0.
        self.reader
            .read_exact(&mut buffer.as_mut()[N::BYTE_SIZE - base_byte_width..])
            .context(IoSnafu)?;
        let base = N::from_be_bytes(buffer).decode_signed_from_msb(base_byte_width);

        // Get data values
        read_ints(
            &mut self.decoded_ints,
            length,
            value_bit_width,
            &mut self.reader,
        )?;

        // Get patches that will be applied to base values.
        // At most they might be u64 in width (because of check above).
        let ceil_patch_total_bit_width = get_closest_fixed_bits(patch_total_bit_width);
        let mut patches: Vec<u64> = Vec::with_capacity(patch_list_length);
        read_ints(
            &mut patches,
            patch_list_length,
            ceil_patch_total_bit_width,
            &mut self.reader,
        )?;

        // TODO: document and explain below logic
        let mut patch_index = 0;
        let patch_mask = (1 << patch_bit_width) - 1;
        let mut current_gap = patches[patch_index] >> patch_bit_width;
        let mut current_patch = patches[patch_index] & patch_mask;
        let mut actual_gap = 0;

        while current_gap == 255 && current_patch == 0 {
            actual_gap += 255;
            patch_index += 1;
            current_gap = patches[patch_index] >> patch_bit_width;
            current_patch = patches[patch_index] & patch_mask;
        }
        actual_gap += current_gap;

        for (idx, value) in self.decoded_ints.iter_mut().enumerate() {
            if idx == actual_gap as usize {
                let patch_bits =
                    current_patch
                        .checked_shl(value_bit_width_u32)
                        .context(OutOfSpecSnafu {
                            msg: "Overflow while shifting patch bits by value_bit_width",
                        })?;
                // Safe conversion without loss as we check the bit width prior
                let patch_bits = N::from_u64(patch_bits);
                let patched_value = *value | patch_bits;

                *value = patched_value.checked_add(&base).context(OutOfSpecSnafu {
                    msg: "over/underflow when decoding patched base integer",
                })?;

                patch_index += 1;

                if patch_index < patches.len() {
                    current_gap = patches[patch_index] >> patch_bit_width;
                    current_patch = patches[patch_index] & patch_mask;
                    actual_gap = 0;

                    while current_gap == 255 && current_patch == 0 {
                        actual_gap += 255;
                        patch_index += 1;
                        current_gap = patches[patch_index] >> patch_bit_width;
                        current_patch = patches[patch_index] & patch_mask;
                    }

                    actual_gap += current_gap;
                    actual_gap += idx as u64;
                }
            } else {
                *value = value.checked_add(&base).context(OutOfSpecSnafu {
                    msg: "over/underflow when decoding patched base integer",
                })?;
            }
        }

        Ok(())
    }
}
