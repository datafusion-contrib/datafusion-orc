use std::io::Read;

use snafu::ResultExt;

use super::util::{bytes_to_long_be, read_u8};
use crate::error::{self, Result};
use crate::reader::decode::util::{get_closest_fixed_bits, read_ints};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EncodingTypeV2 {
    ShortRepeat,
    Direct,
    PatchedBase,
    Delta,
}

fn header_to_rle_v2_short_repeated_width(header: u8) -> u8 {
    (header & 0b00111000) >> 3
}

fn header_to_rle_v2_short_repeated_count(header: u8) -> u8 {
    header & 0b00000111
}

fn rle_v2_direct_bit_width(value: u8) -> u8 {
    match value {
        0..=23 => value + 1,
        27 => 32,
        28 => 40,
        29 => 48,
        30 => 56,
        31 => 64,
        other => todo!("{other}"),
    }
}

fn header_to_rle_v2_direct_bit_width(header: u8) -> u8 {
    let bit_width = (header & 0b00111110) >> 1;
    rle_v2_direct_bit_width(bit_width)
}

fn rle_v2_delta_bit_width(value: u8) -> u8 {
    match value {
        0 => 0,
        1 => 2,
        3 => 4,
        7 => 8,
        15 => 16,
        23 => 24,
        27 => 32,
        28 => 40,
        29 => 48,
        30 => 56,
        31 => 64,
        other => todo!("{other}"),
    }
}

fn header_to_rle_v2_delta_bit_width(header: u8) -> u8 {
    let bit_width = (header & 0b00111110) >> 1;
    rle_v2_delta_bit_width(bit_width)
}

fn header_to_rle_v2_direct_length(header: u8, header1: u8) -> u16 {
    let bit = header & 0b00000001;
    let r = u16::from_be_bytes([bit, header1]);
    1 + r
}

fn unsigned_varint<R: Read>(reader: &mut R) -> Result<u64> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];
    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return error::OutOfSpecSnafu {
                msg: "Failed to unsigned_varint",
            }
            .fail();
        }
        reader.read_exact(&mut buf[..]).context(error::IoSnafu)?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }
    Ok(i)
}

#[inline]
fn zigzag(z: u64) -> i64 {
    if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    }
}

fn signed_varint<R: Read>(reader: &mut R) -> Result<i64> {
    unsigned_varint(reader).map(zigzag)
}

#[inline]
fn unpack(bytes: &[u8], num_bits: u8, index: usize) -> u64 {
    if num_bits == 0 {
        return 0;
    };
    let num_bits = num_bits as usize;
    let start: usize = num_bits * index; // in bits
    let length = num_bits; // in bits
    let byte_start = start / 8;
    let byte_end = (start + length + 7) / 8;
    // copy swapped
    let slice = &bytes[byte_start..byte_end];
    let mut a = [0u8; 8];
    for (i, item) in slice.iter().rev().enumerate() {
        a[i] = *item;
    }
    let bits = u64::from_le_bytes(a);
    let offset = (slice.len() * 8 - num_bits) % 8 - start % 8;
    (bits >> offset) & (!0u64 >> (64 - num_bits))
}

#[derive(Debug)]
pub struct UnsignedPatchedBaseRun {
    length: usize,
    index: usize,
    data: Vec<i64>,
}

impl UnsignedPatchedBaseRun {
    #[inline]
    pub fn try_new<R: Read>(header: u8, reader: &mut R, skip_corrupt: bool) -> Result<Self> {
        let bit_width = header_to_rle_v2_direct_bit_width(header);

        // 9 bits for length (L) (1 to 512 values)
        let second_byte = read_u8(reader)?;

        let mut length = (header as i32 & 0x01 << 8) as usize | second_byte as usize;
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

        if width > 64 && !skip_corrupt {
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

        let mut literals = vec![];
        let mut num_literals = 0;

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

                literals.push(base + patched_value);
                num_literals += 1;

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
                literals.push(base + item);
                num_literals += 1;
            }
        }

        Ok(Self {
            length: num_literals,
            index: 0,
            data: literals,
        })
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.length - self.index
    }
}

impl Iterator for UnsignedPatchedBaseRun {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            self.index += 1;
            self.data[index] as u64
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

#[derive(Debug)]
pub struct UnsignedDirectRun {
    data: Vec<u8>,
    bit_width: u8,
    index: usize,
    length: usize,
}

impl UnsignedDirectRun {
    #[inline]
    pub fn try_new<R: Read>(header: u8, reader: &mut R, mut scratch: Vec<u8>) -> Result<Self> {
        let mut header1 = [0u8];
        reader.read_exact(&mut header1).context(error::IoSnafu)?;

        let bit_width = header_to_rle_v2_direct_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, header1[0]);

        let additional = ((bit_width as usize) * (length as usize) + 7) / 8;
        scratch.clear();
        scratch.reserve(additional);
        reader
            .take(additional as u64)
            .read_to_end(&mut scratch)
            .context(error::IoSnafu)?;

        Ok(Self {
            data: scratch,
            bit_width,
            index: 0,
            length: length as usize,
        })
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.length - self.index
    }
}

impl Iterator for UnsignedDirectRun {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            self.index += 1;
            unpack(&self.data, self.bit_width, index)
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

pub struct UnsignedDeltaRun {
    encoded_deltas: Vec<u8>,
    bit_width: u8,
    index: usize,
    length: usize,
    base: u64,
    delta_base: i64,
}

impl UnsignedDeltaRun {
    #[inline]
    pub fn try_new<R: Read>(header: u8, reader: &mut R, mut scratch: Vec<u8>) -> Result<Self> {
        let mut header1 = [0u8];
        reader.read_exact(&mut header1).context(error::IoSnafu)?;
        let bit_width = header_to_rle_v2_delta_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, header1[0]);

        let base = unsigned_varint(reader)?;
        let delta_base = signed_varint(reader)?;
        let additional = ((length as usize - 2) * bit_width as usize + 7) / 8;

        scratch.clear();
        scratch.reserve(additional);
        reader
            .take(additional as u64)
            .read_to_end(&mut scratch)
            .context(error::IoSnafu)?;

        Ok(Self {
            base,
            encoded_deltas: scratch,
            bit_width,
            index: 0,
            length: length as usize,
            delta_base,
        })
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.length - self.index
    }

    #[inline]
    pub fn into_inner(mut self) -> Vec<u8> {
        self.encoded_deltas.clear();
        self.encoded_deltas
    }
}

impl Iterator for UnsignedDeltaRun {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            if index == 0 {
                self.index += 1;
                return self.base;
            }
            if index == 1 || self.bit_width == 0 {
                self.index += 1;
                if self.delta_base > 0 {
                    self.base += self.delta_base as u64;
                } else {
                    self.base -= (-self.delta_base) as u64;
                }
                return self.base;
            }
            self.index += 1;
            let delta = unpack(&self.encoded_deltas, self.bit_width, index - 2);
            if self.delta_base > 0 {
                self.base += delta;
            } else {
                self.base -= delta;
            }
            self.base
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

#[derive(Debug)]
pub struct UnsignedShortRepeat {
    value: u64,
    remaining: usize,
    scratch: Vec<u8>,
}

impl UnsignedShortRepeat {
    #[inline]
    fn try_new<R: Read>(header: u8, reader: &mut R, mut scratch: Vec<u8>) -> Result<Self> {
        let width = 1 + header_to_rle_v2_short_repeated_width(header);
        let count = 3 + header_to_rle_v2_short_repeated_count(header);

        scratch.clear();
        scratch.reserve(width as usize);
        reader
            .take(width as u64)
            .read_to_end(&mut scratch)
            .context(error::IoSnafu)?;

        let mut a = [0u8; 8];
        a[8 - scratch.len()..].copy_from_slice(&scratch);
        let value = u64::from_be_bytes(a);
        scratch.clear();

        Ok(Self {
            value,
            remaining: count as usize,
            scratch,
        })
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.remaining
    }

    #[inline]
    pub fn into_inner(self) -> Vec<u8> {
        self.scratch
    }
}

impl Iterator for UnsignedShortRepeat {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.remaining != 0).then(|| {
            self.remaining -= 1;
            self.value
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

#[derive(Debug)]
pub struct SignedDeltaRun {
    encoded_deltas: Vec<u8>,
    bit_width: u8,
    index: usize,
    length: usize,
    base: i64,
    delta_base: i64,
}

impl SignedDeltaRun {
    #[inline]
    fn try_new<R: Read>(header: u8, reader: &mut R, mut scratch: Vec<u8>) -> Result<Self> {
        let mut header1 = [0u8];
        reader.read_exact(&mut header1).context(error::IoSnafu)?;
        let bit_width = header_to_rle_v2_delta_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, header1[0]);

        let base = unsigned_varint(reader).map(zigzag)?;
        let delta_base = signed_varint(reader)?;
        let additional = ((length as usize - 2) * bit_width as usize + 7) / 8;

        scratch.clear();
        scratch.reserve(additional);
        reader
            .take(additional as u64)
            .read_to_end(&mut scratch)
            .context(error::IoSnafu)?;

        Ok(Self {
            base,
            encoded_deltas: scratch,
            bit_width,
            index: 0,
            length: length as usize,
            delta_base,
        })
    }

    pub fn len(&self) -> usize {
        self.length - self.index
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for SignedDeltaRun {
    type Item = i64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            if index == 0 {
                self.index += 1;
                return self.base;
            }
            if index == 1 || self.bit_width == 0 {
                self.index += 1;
                if self.delta_base > 0 {
                    self.base += self.delta_base;
                } else {
                    self.base -= -self.delta_base;
                }
                return self.base;
            }
            self.index += 1;
            // edge case where `bit_width == 0`, where deltas are equal to base delta
            let delta = unpack(&self.encoded_deltas, self.bit_width, index - 2);
            if self.delta_base > 0 {
                self.base += delta as i64;
            } else {
                self.base -= delta as i64;
            }
            self.base
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }
}

#[inline]
fn run_encoding(header: u8) -> EncodingTypeV2 {
    match (header & 128 == 128, header & 64 == 64) {
        // 11... = 3
        (true, true) => EncodingTypeV2::Delta,
        // 10... = 2
        (true, false) => EncodingTypeV2::PatchedBase,
        // 01... = 1
        (false, true) => EncodingTypeV2::Direct,
        // 00... = 0
        (false, false) => EncodingTypeV2::ShortRepeat,
    }
}

/// An enum describing one of the RLE v2 runs for unsigned integers
pub enum UnsignedRleV2Run {
    /// Direct
    Direct(UnsignedDirectRun),
    /// Delta
    Delta(UnsignedDeltaRun),
    /// Short repeat
    ShortRepeat(UnsignedShortRepeat),
    /// Patched base
    PatchedBase(UnsignedPatchedBaseRun),
}

impl UnsignedRleV2Run {
    /// Returns a new [`UnsignedRleV2Run`] owning `scratch`.
    pub fn try_new<R: Read>(reader: &mut R, scratch: Vec<u8>) -> Result<Self> {
        let mut header = [0u8];
        reader.read_exact(&mut header).context(error::IoSnafu)?;
        let header = header[0];
        let encoding = run_encoding(header);

        match encoding {
            EncodingTypeV2::Direct => {
                UnsignedDirectRun::try_new(header, reader, scratch).map(Self::Direct)
            }
            EncodingTypeV2::Delta => {
                UnsignedDeltaRun::try_new(header, reader, scratch).map(Self::Delta)
            }
            EncodingTypeV2::ShortRepeat => {
                UnsignedShortRepeat::try_new(header, reader, scratch).map(Self::ShortRepeat)
            }
            EncodingTypeV2::PatchedBase => {
                UnsignedPatchedBaseRun::try_new(header, reader, true).map(Self::PatchedBase)
            }
        }
    }

    /// The number of items remaining
    pub fn len(&self) -> usize {
        match self {
            Self::Direct(run) => run.len(),
            Self::Delta(run) => run.len(),
            Self::ShortRepeat(run) => run.len(),
            Self::PatchedBase(run) => run.len(),
        }
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A fallible [`Iterator`] of [`UnsignedRleV2Run`].
pub struct UnsignedRleV2RunIter<R: Read> {
    reader: R,
    scratch: Vec<u8>,
    length: usize,
}

impl<R: Read> UnsignedRleV2RunIter<R> {
    /// Returns a new [`UnsignedRleV2RunIter`].
    pub fn new(reader: R, length: usize, scratch: Vec<u8>) -> Self {
        Self {
            reader,
            scratch,
            length,
        }
    }

    /// Returns its internal buffer
    pub fn into_inner(mut self) -> (R, Vec<u8>) {
        self.scratch.clear();
        (self.reader, self.scratch)
    }
}

impl<R: Read> Iterator for UnsignedRleV2RunIter<R> {
    type Item = Result<UnsignedRleV2Run>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.length != 0).then(|| {
            let run =
                UnsignedRleV2Run::try_new(&mut self.reader, std::mem::take(&mut self.scratch))?;
            self.length -= run.len();
            Ok(run)
        })
    }
}

/// A fallible [`Iterator`] of [`i64`].
pub struct UnsignedRleV2Iter<R: Read> {
    current: Option<UnsignedRleV2Run>,
    runs: UnsignedRleV2RunIter<R>,
}

impl<R: Read> UnsignedRleV2Iter<R> {
    /// Returns a new [`SignedRleV2Iter`].
    pub fn new(reader: R, length: usize, scratch: Vec<u8>) -> Self {
        Self {
            runs: UnsignedRleV2RunIter::new(reader, length, scratch),
            current: None,
        }
    }

    /// Returns its internal buffer
    pub fn into_inner(self) -> (R, Vec<u8>) {
        self.runs.into_inner()
    }
}

impl<R: Read> Iterator for UnsignedRleV2Iter<R> {
    type Item = Result<u64>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let next = if let Some(run) = &mut self.current {
            match run {
                UnsignedRleV2Run::Direct(values_iter) => values_iter.next(),
                UnsignedRleV2Run::Delta(values_iter) => values_iter.next(),
                UnsignedRleV2Run::ShortRepeat(values_iter) => values_iter.next(),
                UnsignedRleV2Run::PatchedBase(values_iter) => values_iter.next(),
            }
        } else {
            None
        };

        if next.is_none() {
            match self.runs.next()? {
                Ok(run) => self.current = Some(run),
                Err(e) => return Some(Err(e)),
            }
            self.next()
        } else {
            next.map(Ok)
        }
    }
}

#[derive(Debug)]
pub struct SignedDirectRun(UnsignedDirectRun);

impl SignedDirectRun {
    pub fn try_new<R: Read>(header: u8, reader: &mut R, scratch: Vec<u8>) -> Result<Self> {
        UnsignedDirectRun::try_new(header, reader, scratch).map(Self)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for SignedDirectRun {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(zigzag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[derive(Debug)]
pub struct SignedShortRepeat(UnsignedShortRepeat);

impl SignedShortRepeat {
    pub fn try_new<R: Read>(header: u8, reader: &mut R, scratch: Vec<u8>) -> Result<Self> {
        UnsignedShortRepeat::try_new(header, reader, scratch).map(Self)
    }

    /// The number of items remaining
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for SignedShortRepeat {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(zigzag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[derive(Debug)]
pub struct SignedPatchedBaseRun(UnsignedPatchedBaseRun);

impl SignedPatchedBaseRun {
    pub fn try_new<R: Read>(header: u8, reader: &mut R, skip_corrupt: bool) -> Result<Self> {
        UnsignedPatchedBaseRun::try_new(header, reader, skip_corrupt).map(Self)
    }

    /// The number of items remaining
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for SignedPatchedBaseRun {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|v| v as i64)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// An enum describing one of the RLE v2 runs for signed integers
#[derive(Debug)]
pub enum SignedRleV2Run {
    /// Direct
    Direct(SignedDirectRun),
    /// Delta
    Delta(SignedDeltaRun),
    /// Short repeat
    ShortRepeat(SignedShortRepeat),
    /// PatchedBase
    PatchedBase(SignedPatchedBaseRun),
}

impl SignedRleV2Run {
    /// Returns a new [`SignedRleV2Run`], moving `scratch` to itself
    pub fn try_new<R: Read>(reader: &mut R, scratch: Vec<u8>) -> Result<Self> {
        let mut header = [0u8];
        reader.read_exact(&mut header).context(error::IoSnafu)?;
        let header = header[0];
        let encoding = run_encoding(header);

        match encoding {
            EncodingTypeV2::Direct => {
                SignedDirectRun::try_new(header, reader, scratch).map(Self::Direct)
            }
            EncodingTypeV2::Delta => {
                SignedDeltaRun::try_new(header, reader, scratch).map(Self::Delta)
            }
            EncodingTypeV2::ShortRepeat => {
                SignedShortRepeat::try_new(header, reader, scratch).map(Self::ShortRepeat)
            }
            EncodingTypeV2::PatchedBase => {
                SignedPatchedBaseRun::try_new(header, reader, true).map(Self::PatchedBase)
            }
        }
    }

    /// The number of items remaining
    pub fn len(&self) -> usize {
        match self {
            Self::Direct(run) => run.len(),
            Self::Delta(run) => run.len(),
            Self::ShortRepeat(run) => run.len(),
            Self::PatchedBase(run) => run.len(),
        }
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A fallible [`Iterator`] of [`SignedRleV2Run`].
pub struct SignedRleV2RunIter<R: Read> {
    reader: R,
    scratch: Vec<u8>,
    length: usize,
}

impl<R: Read> SignedRleV2RunIter<R> {
    /// Returns a new [`SignedRleV2RunIter`].
    pub fn new(reader: R, length: usize, scratch: Vec<u8>) -> Self {
        Self {
            reader,
            scratch,
            length,
        }
    }

    pub fn into_inner(mut self) -> (R, Vec<u8>) {
        self.scratch.clear();
        (self.reader, self.scratch)
    }
}

impl<R: Read> Iterator for SignedRleV2RunIter<R> {
    type Item = Result<SignedRleV2Run>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.length != 0).then(|| {
            let run = SignedRleV2Run::try_new(&mut self.reader, std::mem::take(&mut self.scratch))?;
            self.length -= run.len();
            Ok(run)
        })
    }
}

/// A fallible [`Iterator`] of [`i64`].
pub struct SignedRleV2Iter<R: Read> {
    current: Option<SignedRleV2Run>,
    runs: SignedRleV2RunIter<R>,
}

impl<R: Read> SignedRleV2Iter<R> {
    /// Returns a new [`SignedRleV2Iter`].
    pub fn new(reader: R, length: usize, scratch: Vec<u8>) -> Self {
        Self {
            runs: SignedRleV2RunIter::new(reader, length, scratch),
            current: None,
        }
    }

    /// Returns its internal buffer
    pub fn into_inner(self) -> (R, Vec<u8>) {
        self.runs.into_inner()
    }
}

impl<R: Read> Iterator for SignedRleV2Iter<R> {
    type Item = Result<i64>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let next = if let Some(run) = &mut self.current {
            match run {
                SignedRleV2Run::Direct(values_iter) => values_iter.next(),
                SignedRleV2Run::Delta(values_iter) => values_iter.next(),
                SignedRleV2Run::ShortRepeat(values_iter) => values_iter.next(),
                SignedRleV2Run::PatchedBase(values_iter) => values_iter.next(),
            }
        } else {
            None
        };

        if next.is_none() {
            match self.runs.next()? {
                Ok(run) => self.current = Some(run),
                Err(e) => return Some(Err(e)),
            }
            self.next()
        } else {
            next.map(Ok)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag(2), 1);
        assert_eq!(zigzag(4), 2);
    }

    #[test]
    fn unpacking() {
        let bytes = [0b01000000u8];
        assert_eq!(unpack(&bytes, 2, 0), 1);
        assert_eq!(unpack(&bytes, 2, 1), 0);
    }

    #[test]
    fn short_repeat() {
        // [10000, 10000, 10000, 10000, 10000]
        let data: [u8; 3] = [0x0a, 0x27, 0x10];

        let a = UnsignedShortRepeat::try_new(data[0], &mut &data[1..], vec![])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(a, vec![10000, 10000, 10000, 10000, 10000]);
    }

    #[test]
    fn direct() {
        // [23713, 43806, 57005, 48879]
        let data: [u8; 10] = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];

        let data = &mut data.as_ref();

        let a = UnsignedDirectRun::try_new(data[0], &mut &data[1..], vec![])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(a, vec![23713, 43806, 57005, 48879]);
    }

    #[test]
    fn delta() {
        // [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
        // 0x22 = 34
        // 0x42 = 66
        // 0x46 = 70
        let data: [u8; 8] = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];

        let data = &mut data.as_ref();

        let a = UnsignedDeltaRun::try_new(data[0], &mut &data[1..], vec![])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(a, vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);
    }

    #[test]
    fn patched_base() {
        let data = vec![
            0x8eu8, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46,
            0x50, 0x5a, 0xfc, 0xe8,
        ];

        let expected = vec![
            2030u64, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090,
        ];

        let a = UnsignedPatchedBaseRun::try_new(data[0], &mut &data[1..], false)
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(a, expected);
    }
}
