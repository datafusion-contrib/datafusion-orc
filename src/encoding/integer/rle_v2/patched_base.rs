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

use bytes::{BufMut, BytesMut};
use snafu::OptionExt;

use super::{EncodingType, NInt};
use crate::{
    encoding::{
        integer::{
            util::{
                encode_bit_width, extract_run_length_from_header, get_closest_fixed_bits,
                read_ints, rle_v2_decode_bit_width, signed_msb_encode, write_packed_ints,
            },
            EncodingSign, VarintSerde,
        },
        util::read_u8,
    },
    error::{OutOfSpecSnafu, Result},
};

pub fn read_patched_base<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    header: u8,
) -> Result<()> {
    let encoded_bit_width = (header >> 1) & 0x1F;
    let value_bit_width = rle_v2_decode_bit_width(encoded_bit_width);
    // Bit width derived from u8 above, so impossible to overflow u32
    let value_bit_width_u32 = u32::try_from(value_bit_width).unwrap();

    let second_byte = read_u8(reader)?;
    let length = extract_run_length_from_header(header, second_byte);

    let third_byte = read_u8(reader)?;
    let fourth_byte = read_u8(reader)?;

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

    let base = N::read_big_endian(reader, base_byte_width)?;
    let base = S::decode_signed_msb(base, base_byte_width);

    // Get data values
    // TODO: this should read into Vec<i64>
    //       as base reduced values can exceed N::max()
    //       (e.g. if base is N::min() and this is signed type)
    read_ints(out_ints, length, value_bit_width, reader)?;

    // Get patches that will be applied to base values.
    // At most they might be u64 in width (because of check above).
    let ceil_patch_total_bit_width = get_closest_fixed_bits(patch_total_bit_width);
    let mut patches: Vec<i64> = Vec::with_capacity(patch_list_length);
    read_ints(
        &mut patches,
        patch_list_length,
        ceil_patch_total_bit_width,
        reader,
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

    for (idx, value) in out_ints.iter_mut().enumerate() {
        if idx == actual_gap as usize {
            let patch_bits =
                current_patch
                    .checked_shl(value_bit_width_u32)
                    .context(OutOfSpecSnafu {
                        msg: "Overflow while shifting patch bits by value_bit_width",
                    })?;
            // Safe conversion without loss as we check the bit width prior
            let patch_bits = N::from_i64(patch_bits);
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
                actual_gap += idx as i64;
            }
        } else {
            *value = value.checked_add(&base).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding patched base integer",
            })?;
        }
    }

    Ok(())
}

fn derive_patches(
    base_reduced_literals: &mut [i64],
    patch_bits_width: usize,
    max_base_value_bit_width: usize,
) -> (Vec<i64>, usize) {
    // Values with bits exceeding this mask will be patched.
    let max_base_value_mask = (1 << max_base_value_bit_width) - 1;
    // Used to encode gaps greater than 255 (no patch bits, just used for gap).
    let jump_patch = 255 << patch_bits_width;

    // At most 5% of values that must be patched.
    // (Since max buffer length is 512, at most this can be 26)
    let mut patches: Vec<i64> = Vec::with_capacity(26);
    let mut last_patch_index = 0;
    // Needed to determine bit width of patch gaps to encode in header.
    let mut max_gap = 0;
    for (idx, lit) in base_reduced_literals
        .iter_mut()
        .enumerate()
        // Find all values which need to be patched (the 5% of values larger than the others)
        .filter(|(_, &mut lit)| lit > max_base_value_mask)
    {
        // Convert to unsigned to ensure leftmost bits are 0
        let patch_bits = (*lit as u64) >> max_base_value_bit_width;

        // Gaps can at most be 255 (since gap bit width cannot exceed 8; in spec it states
        // the header has only 3 bits to encode the size of the patch gap, so 8 is the largest
        // value).
        //
        // Therefore if gap is found greater than 255 then we insert an empty patch with gap of 255
        // (and the empty patch will have no effect when reading as patching using empty bits will
        // be a no-op).
        //
        // Extra special case if gap is 511, we unroll into inserting two empty patches (instead of
        // relying on a loop). Max buffer size cannot exceed 512 so this is the largest possible gap.
        let gap = idx - last_patch_index;
        let gap = if gap == 511 {
            max_gap = 255;
            patches.push(jump_patch);
            patches.push(jump_patch);
            1
        } else if gap > 255 {
            max_gap = 255;
            patches.push(jump_patch);
            gap - 255
        } else {
            max_gap = max_gap.max(gap);
            gap
        };
        let patch = patch_bits | (gap << patch_bits_width) as u64;
        patches.push(patch as i64);

        last_patch_index = idx;

        // Stripping patch bits
        *lit &= max_base_value_mask;
    }

    // If only one element to be patched, and is the very first one.
    // Patch gap width minimum is 1.
    let patch_gap_width = if max_gap == 0 {
        1
    } else {
        (max_gap as i16).bits_used()
    };

    (patches, patch_gap_width)
}

pub fn write_patched_base(
    writer: &mut BytesMut,
    base_reduced_literals: &mut [i64],
    base: i64,
    brl_100p_bit_width: usize,
    brl_95p_bit_width: usize,
) {
    let patch_bits_width = brl_100p_bit_width - brl_95p_bit_width;
    let patch_bits_width = get_closest_fixed_bits(patch_bits_width);
    // According to spec, each patch (patch bits + gap) must be <= 64 bits.
    // So we adjust accordingly here if we hit this edge case where patch_width
    // is 64 bits (which would have no space for gap).
    let (patch_bits_width, brl_95p_bit_width) = if patch_bits_width == 64 {
        (56, 8)
    } else {
        (patch_bits_width, brl_95p_bit_width)
    };

    let (patches, patch_gap_width) =
        derive_patches(base_reduced_literals, patch_bits_width, brl_95p_bit_width);

    let encoded_bit_width = encode_bit_width(brl_95p_bit_width) as u8;

    // [1, 512] to [0, 511]
    let run_length = base_reduced_literals.len() as u16 - 1;

    // No need to mask as we guarantee max length is 512
    let encoded_length_high_bit = (run_length >> 8) as u8;
    let encoded_length_low_bits = (run_length & 0xFF) as u8;

    // +1 to account for sign bit
    let base_bit_width = get_closest_fixed_bits(base.abs().bits_used() + 1);
    let base_byte_width = base_bit_width.div_ceil(8).max(1);
    let msb_encoded_min = signed_msb_encode(base, base_byte_width);
    // [1, 8] to [0, 7]
    let encoded_base_width = base_byte_width - 1;
    let encoded_patch_bits_width = encode_bit_width(patch_bits_width);
    let encoded_patch_gap_width = patch_gap_width - 1;

    let header1 =
        EncodingType::PatchedBase.to_header() | encoded_bit_width << 1 | encoded_length_high_bit;
    let header2 = encoded_length_low_bits;
    let header3 = (encoded_base_width as u8) << 5 | encoded_patch_bits_width as u8;
    let header4 = (encoded_patch_gap_width as u8) << 5 | patches.len() as u8;
    writer.put_slice(&[header1, header2, header3, header4]);

    // Write out base value as big endian bytes
    let base_bytes = msb_encoded_min.to_be_bytes();
    // 8 since i64
    let base_bytes = &base_bytes.as_ref()[8 - base_byte_width..];
    writer.put_slice(base_bytes);

    // Writing base reduced literals followed by patch list
    let bit_width = get_closest_fixed_bits(brl_95p_bit_width);
    write_packed_ints(writer, bit_width, base_reduced_literals);
    let bit_width = get_closest_fixed_bits(patch_gap_width + patch_bits_width);
    write_packed_ints(writer, bit_width, &patches);
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use proptest::prelude::*;

    use crate::encoding::integer::{util::calculate_percentile_bits, SignedEncoding};

    use super::*;

    #[derive(Debug)]
    struct PatchesStrategy {
        base: i64,
        base_reduced_values: Vec<i64>,
        patches: Vec<i64>,
        patch_indices: Vec<usize>,
        base_index: usize,
    }

    fn patches_strategy() -> impl Strategy<Value = PatchesStrategy> {
        // TODO: clean this up a bit
        prop::collection::vec(0..1_000_000_i64, 20..=512)
            .prop_flat_map(|base_reduced_values| {
                let base_strategy = -1_000_000_000..1_000_000_000_i64;
                let max_patches_length = (base_reduced_values.len() as f32 * 0.05).ceil() as usize;
                let base_reduced_values_strategy = Just(base_reduced_values);
                let patches_strategy = prop::collection::vec(
                    1_000_000_000_000_000..1_000_000_000_000_000_000_i64,
                    1..=max_patches_length,
                );
                (
                    base_strategy,
                    base_reduced_values_strategy,
                    patches_strategy,
                )
            })
            .prop_flat_map(|(base, base_reduced_values, patches)| {
                let base_strategy = Just(base);
                // +1 for the base index, so we don't have to deduplicate separately
                let patch_indices_strategy =
                    prop::collection::hash_set(0..base_reduced_values.len(), patches.len() + 1);
                let base_reduced_values_strategy = Just(base_reduced_values);
                let patches_strategy = Just(patches);
                (
                    base_strategy,
                    base_reduced_values_strategy,
                    patches_strategy,
                    patch_indices_strategy,
                )
            })
            .prop_map(|(base, base_reduced_values, patches, patch_indices)| {
                let mut patch_indices = patch_indices.into_iter().collect::<Vec<_>>();
                let base_index = patch_indices.pop().unwrap();
                PatchesStrategy {
                    base,
                    base_reduced_values,
                    patches,
                    patch_indices,
                    base_index,
                }
            })
    }

    fn roundtrip_patched_base_helper(
        base_reduced_literals: &[i64],
        base: i64,
        brl_95p_bit_width: usize,
        brl_100p_bit_width: usize,
    ) -> Result<Vec<i64>> {
        let mut base_reduced_literals = base_reduced_literals.to_vec();

        let mut buf = BytesMut::new();
        let mut out = vec![];

        write_patched_base(
            &mut buf,
            &mut base_reduced_literals,
            base,
            brl_100p_bit_width,
            brl_95p_bit_width,
        );
        let header = buf[0];
        read_patched_base::<i64, _, SignedEncoding>(&mut Cursor::new(&buf[1..]), &mut out, header)?;

        Ok(out)
    }

    fn form_patched_base_values(
        base_reduced_values: &[i64],
        patches: &[i64],
        patch_indices: &[usize],
        base_index: usize,
    ) -> Vec<i64> {
        let mut base_reduced_values = base_reduced_values.to_vec();
        for (&patch, &index) in patches.iter().zip(patch_indices) {
            base_reduced_values[index] = patch;
        }
        // Need at least one zero to represent the base
        base_reduced_values[base_index] = 0;
        base_reduced_values
    }

    fn form_expected_values(base: i64, base_reduced_values: &[i64]) -> Vec<i64> {
        base_reduced_values.iter().map(|&v| base + v).collect()
    }

    proptest! {
        #[test]
        fn roundtrip_patched_base_i64(patches_strategy in patches_strategy()) {
            let PatchesStrategy {
                base,
                base_reduced_values,
                patches,
                patch_indices,
                base_index
            } = patches_strategy;
            let base_reduced_values = form_patched_base_values(
                &base_reduced_values,
                &patches,
                &patch_indices,
                base_index
            );
            let expected = form_expected_values(base, &base_reduced_values);
            let brl_95p_bit_width = calculate_percentile_bits(&base_reduced_values, 0.95);
            let brl_100p_bit_width = calculate_percentile_bits(&base_reduced_values, 1.0);
            // Need enough outliers to require patching
            prop_assume!(brl_95p_bit_width != brl_100p_bit_width);
            let actual = roundtrip_patched_base_helper(
                &base_reduced_values,
                base,
                brl_95p_bit_width,
                brl_100p_bit_width
            )?;
            prop_assert_eq!(actual, expected);
        }
    }
}
