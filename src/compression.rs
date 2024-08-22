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

// Modified from https://github.com/DataEngineeringLabs/orc-format/blob/416490db0214fc51d53289253c0ee91f7fc9bc17/src/read/decompress/mod.rs
//! Related code for handling decompression of ORC files.

use std::io::Read;

use bytes::{Bytes, BytesMut};
use fallible_streaming_iterator::FallibleStreamingIterator;
use snafu::ResultExt;

use crate::error::{self, OrcError, Result};
use crate::proto::{self, CompressionKind};

// Spec states default is 256K
const DEFAULT_COMPRESSION_BLOCK_SIZE: u64 = 256 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct Compression {
    compression_type: CompressionType,
    /// No compression chunk will decompress to larger than this size.
    /// Use to size the scratch buffer appropriately.
    max_decompressed_block_size: usize,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({} byte max block size)",
            self.compression_type, self.max_decompressed_block_size
        )
    }
}

impl Compression {
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    pub(crate) fn from_proto(
        kind: proto::CompressionKind,
        compression_block_size: Option<u64>,
    ) -> Option<Self> {
        let max_decompressed_block_size =
            compression_block_size.unwrap_or(DEFAULT_COMPRESSION_BLOCK_SIZE) as usize;
        match kind {
            CompressionKind::None => None,
            CompressionKind::Zlib => Some(Self {
                compression_type: CompressionType::Zlib,
                max_decompressed_block_size,
            }),
            CompressionKind::Snappy => Some(Self {
                compression_type: CompressionType::Snappy,
                max_decompressed_block_size,
            }),
            CompressionKind::Lzo => Some(Self {
                compression_type: CompressionType::Lzo,
                max_decompressed_block_size,
            }),
            CompressionKind::Lz4 => Some(Self {
                compression_type: CompressionType::Lz4,
                max_decompressed_block_size,
            }),
            CompressionKind::Zstd => Some(Self {
                compression_type: CompressionType::Zstd,
                max_decompressed_block_size,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CompressionType {
    Zlib,
    Snappy,
    Lzo,
    Lz4,
    Zstd,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Indicates length of block and whether it's compressed or not.
#[derive(Debug, PartialEq, Eq)]
enum CompressionHeader {
    Original(u32),
    Compressed(u32),
}

/// ORC files are compressed in blocks, with a 3 byte header at the start
/// of these blocks indicating the length of the block and whether it's
/// compressed or not.
fn decode_header(bytes: [u8; 3]) -> CompressionHeader {
    let bytes = [bytes[0], bytes[1], bytes[2], 0];
    let length_and_flag = u32::from_le_bytes(bytes);
    let is_original = length_and_flag & 1 == 1;
    let length = length_and_flag >> 1;
    if is_original {
        CompressionHeader::Original(length)
    } else {
        CompressionHeader::Compressed(length)
    }
}

pub(crate) trait DecompressorVariant: Send {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
struct Zlib;
#[derive(Debug, Clone, Copy)]
struct Zstd;
#[derive(Debug, Clone, Copy)]
struct Snappy;
#[derive(Debug, Clone, Copy)]
struct Lzo;
#[derive(Debug, Clone, Copy)]
struct Lz4 {
    max_decompressed_block_size: usize,
}

impl DecompressorVariant for Zlib {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let mut gz = flate2::read::DeflateDecoder::new(compressed_bytes);
        scratch.clear();
        gz.read_to_end(scratch).context(error::IoSnafu)?;
        Ok(())
    }
}

impl DecompressorVariant for Zstd {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let mut reader =
            zstd::Decoder::new(compressed_bytes).context(error::BuildZstdDecoderSnafu)?;
        scratch.clear();
        reader.read_to_end(scratch).context(error::IoSnafu)?;
        Ok(())
    }
}

impl DecompressorVariant for Snappy {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let len =
            snap::raw::decompress_len(compressed_bytes).context(error::BuildSnappyDecoderSnafu)?;
        scratch.resize(len, 0);
        let mut decoder = snap::raw::Decoder::new();
        decoder
            .decompress(compressed_bytes, scratch)
            .context(error::BuildSnappyDecoderSnafu)?;
        Ok(())
    }
}

impl DecompressorVariant for Lzo {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let decompressed = lzokay_native::decompress_all(compressed_bytes, None)
            .context(error::BuildLzoDecoderSnafu)?;
        // TODO: better way to utilize scratch here
        scratch.clear();
        scratch.extend(decompressed);
        Ok(())
    }
}

impl DecompressorVariant for Lz4 {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let decompressed =
            lz4_flex::block::decompress(compressed_bytes, self.max_decompressed_block_size)
                .context(error::BuildLz4DecoderSnafu)?;
        // TODO: better way to utilize scratch here
        scratch.clear();
        scratch.extend(decompressed);
        Ok(())
    }
}

// TODO: push this earlier so we don't check this variant each time
fn get_decompressor_variant(
    Compression {
        compression_type,
        max_decompressed_block_size,
    }: Compression,
) -> Box<dyn DecompressorVariant> {
    match compression_type {
        CompressionType::Zlib => Box::new(Zlib),
        CompressionType::Snappy => Box::new(Snappy),
        CompressionType::Lzo => Box::new(Lzo),
        CompressionType::Lz4 => Box::new(Lz4 {
            max_decompressed_block_size,
        }),
        CompressionType::Zstd => Box::new(Zstd),
    }
}

enum State {
    Original(Bytes),
    Compressed(Vec<u8>),
}

struct DecompressorIter {
    stream: BytesMut,
    current: Option<State>, // when we have compression but the value is original
    compression: Option<Box<dyn DecompressorVariant>>,
    scratch: Vec<u8>,
}

impl DecompressorIter {
    fn new(stream: Bytes, compression: Option<Compression>, scratch: Vec<u8>) -> Self {
        Self {
            stream: BytesMut::from(stream.as_ref()),
            current: None,
            compression: compression.map(get_decompressor_variant),
            scratch,
        }
    }
}

impl FallibleStreamingIterator for DecompressorIter {
    type Item = [u8];

    type Error = OrcError;

    #[inline]
    fn advance(&mut self) -> Result<(), Self::Error> {
        if self.stream.is_empty() {
            self.current = None;
            return Ok(());
        }

        match &self.compression {
            Some(compression) => {
                // TODO: take stratch from current State::Compressed for re-use
                let header = self.stream.split_to(3);
                let header = [header[0], header[1], header[2]];
                match decode_header(header) {
                    CompressionHeader::Original(length) => {
                        let original = self.stream.split_to(length as usize);
                        self.current = Some(State::Original(original.into()));
                    }
                    CompressionHeader::Compressed(length) => {
                        let compressed = self.stream.split_to(length as usize);
                        compression.decompress_block(&compressed, &mut self.scratch)?;
                        self.current = Some(State::Compressed(std::mem::take(&mut self.scratch)));
                    }
                };
                Ok(())
            }
            None => {
                // TODO: take stratch from current State::Compressed for re-use
                self.current = Some(State::Original(self.stream.clone().into()));
                self.stream.clear();
                Ok(())
            }
        }
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref().map(|x| match x {
            State::Original(x) => x.as_ref(),
            State::Compressed(x) => x.as_ref(),
        })
    }
}

/// A [`Read`]er fulfilling the ORC specification of reading compressed data.
pub struct Decompressor {
    decompressor: DecompressorIter,
    offset: usize,
    is_first: bool,
}

impl Decompressor {
    /// Creates a new [`Decompressor`] that will use `scratch` as a temporary region.
    pub fn new(stream: Bytes, compression: Option<Compression>, scratch: Vec<u8>) -> Self {
        Self {
            decompressor: DecompressorIter::new(stream, compression, scratch),
            offset: 0,
            is_first: true,
        }
    }

    // TODO: remove need for this upstream
    pub fn empty() -> Self {
        Self {
            decompressor: DecompressorIter::new(Bytes::new(), None, vec![]),
            offset: 0,
            is_first: true,
        }
    }
}

impl std::io::Read for Decompressor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.is_first {
            self.is_first = false;
            self.decompressor.advance().unwrap();
        }
        let current = self.decompressor.get();
        let current = if let Some(current) = current {
            if current.len() == self.offset {
                self.decompressor.advance().unwrap();
                self.offset = 0;
                let current = self.decompressor.get();
                if let Some(current) = current {
                    current
                } else {
                    return Ok(0);
                }
            } else {
                &current[self.offset..]
            }
        } else {
            return Ok(0);
        };

        if current.len() >= buf.len() {
            buf.copy_from_slice(&current[..buf.len()]);
            self.offset += buf.len();
            Ok(buf.len())
        } else {
            buf[..current.len()].copy_from_slice(current);
            self.offset += current.len();
            Ok(current.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_uncompressed() {
        // 5 uncompressed = [0x0b, 0x00, 0x00] = [0b1011, 0, 0]
        let bytes = [0b1011, 0, 0];

        let expected = CompressionHeader::Original(5);
        let actual = decode_header(bytes);
        assert_eq!(expected, actual);
    }

    #[test]
    fn decode_compressed() {
        // 100_000 compressed = [0x40, 0x0d, 0x03] = [0b01000000, 0b00001101, 0b00000011]
        let bytes = [0b0100_0000, 0b0000_1101, 0b0000_0011];
        let expected = CompressionHeader::Compressed(100_000);
        let actual = decode_header(bytes);
        assert_eq!(expected, actual);
    }
}
