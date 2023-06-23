use std::io::Read;

use super::RleReaderV2;
use crate::error::Result;
use crate::reader::decode::util::{
    bytes_to_long_be, get_closest_fixed_bits, header_to_rle_v2_direct_bit_width, read_ints,
    read_u8, rle_v2_direct_bit_width,
};

impl<R: Read> RleReaderV2<R> {
    pub fn read_patched_base(&mut self, header: u8) -> Result<()> {
        let bit_width = header_to_rle_v2_direct_bit_width(header);

        let reader = &mut self.reader;

        // 9 bits for length (L) (1 to 512 values)
        let second_byte = read_u8(reader)?;

        let mut length = ((header as u64 & 0x01) << 8) as usize | second_byte as usize;
        // runs are one off
        length += 1;

        let third_byte = read_u8(reader)?;

        let mut base_width = third_byte as u64 >> 5 & 0x07;
        // base width is one off
        base_width += 1;

        let patch_width = rle_v2_direct_bit_width(third_byte & 0x1f);

        // reads next
        let fourth_byte = read_u8(reader)?;

        let mut patch_gap_width = fourth_byte as u64 >> 5 & 0x07;
        // patch gap width is one off
        patch_gap_width += 1;

        // extracts the length of the patch list
        let patch_list_length = fourth_byte & 0x1f;

        let mut base = bytes_to_long_be(reader, base_width as usize)?;

        let mask = 1i64 << ((base_width * 8) - 1);
        // if MSB of base value is 1 then base is negative value else positive
        if base & mask != 0 {
            base &= !mask;
            base = -base
        }

        let mut unpacked = vec![0i64; length];

        read_ints(&mut unpacked, 0, length, bit_width as usize, reader)?;

        let mut unpacked_patch = vec![0i64; patch_list_length as usize];

        let width = patch_width as usize + patch_gap_width as usize;

        if width > 64 && !self.skip_corrupt {
            // TODO: throw error
        }

        let bit_size = get_closest_fixed_bits(width);

        read_ints(
            &mut unpacked_patch,
            0,
            patch_list_length as usize,
            bit_size,
            reader,
        )?;

        let mut patch_index = 0;
        let patch_mask = (1 << patch_width) - 1;
        let mut current_gap = ((unpacked_patch[patch_index] as u64) >> patch_width) as i64;
        let mut current_patch = unpacked_patch[patch_index] & patch_mask;
        let mut actual_gap = 0i64;

        while current_gap == 255 && current_patch == 0 {
            actual_gap += 255;
            patch_index += 1;
            current_gap = ((unpacked_patch[patch_index] as u64) >> patch_width) as i64;
            current_patch = unpacked_patch[patch_index] & patch_mask;
        }
        actual_gap += current_gap;

        for (i, item) in unpacked.iter().enumerate() {
            if i == actual_gap as usize {
                let patched_value = item | (current_patch << bit_width);

                self.literals[self.num_literals] = base + patched_value;
                self.num_literals += 1;

                patch_index += 1;

                if patch_index < unpacked_patch.len() {
                    current_gap = ((unpacked_patch[patch_index] as u64) >> patch_width) as i64;
                    current_patch = unpacked_patch[patch_index] & patch_mask;
                    actual_gap = 0;

                    while current_gap == 255 && current_patch == 0 {
                        actual_gap += 255;
                        patch_index += 1;
                        current_gap = ((unpacked_patch[patch_index] as u64) >> patch_width) as i64;
                        current_patch = unpacked_patch[patch_index] & patch_mask;
                    }

                    actual_gap += current_gap;
                    actual_gap += i as i64;
                }
            } else {
                self.literals[self.num_literals] = base + item;
                self.num_literals += 1;
            }
        }

        Ok(())
    }
}
