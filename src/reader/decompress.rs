// Modified from https://github.com/DataEngineeringLabs/orc-format/blob/416490db0214fc51d53289253c0ee91f7fc9bc17/src/read/decompress/mod.rs
//! Contains [`Decompressor`]

use std::io::Read;

use bytes::{Bytes, BytesMut};
use fallible_streaming_iterator::FallibleStreamingIterator;
use snafu::ResultExt;

use crate::error::{self, Error};
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

impl Compression {
    pub fn from_proto(
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

fn decode_header(bytes: &[u8]) -> (bool, usize) {
    let a: [u8; 3] = (&bytes[..3]).try_into().unwrap();
    let a = [0, a[0], a[1], a[2]];
    let length = u32::from_le_bytes(a);
    let is_original = a[1] & 1 == 1;
    let length = (length >> (8 + 1)) as usize;

    (is_original, length)
}

fn decompress_block(
    compression: Compression,
    compressed_bytes: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<(), Error> {
    match compression.compression_type {
        CompressionType::Zlib => {
            let mut gz = flate2::read::DeflateDecoder::new(compressed_bytes);
            scratch.clear();
            gz.read_to_end(scratch).context(error::IoSnafu)?;
        }
        CompressionType::Zstd => {
            let mut reader =
                zstd::Decoder::new(compressed_bytes).context(error::BuildZstdDecoderSnafu)?;
            scratch.clear();
            reader.read_to_end(scratch).context(error::IoSnafu)?;
        }
        CompressionType::Snappy => {
            let len = snap::raw::decompress_len(compressed_bytes)
                .context(error::BuildSnappyDecoderSnafu)?;
            scratch.resize(len, 0);
            let mut decoder = snap::raw::Decoder::new();
            decoder
                .decompress(compressed_bytes, scratch)
                .context(error::BuildSnappyDecoderSnafu)?;
        }
        CompressionType::Lzo => {
            let decompressed = lzokay_native::decompress_all(compressed_bytes, None)
                .context(error::BuildLzoDecoderSnafu)?;
            // TODO: better way to utilize scratch here
            scratch.clear();
            scratch.extend(decompressed);
        }
        CompressionType::Lz4 => {
            let decompressed = lz4_flex::block::decompress(
                compressed_bytes,
                compression.max_decompressed_block_size,
            )
            .context(error::BuildLz4DecoderSnafu)?;
            // TODO: better way to utilize scratch here
            scratch.clear();
            scratch.extend(decompressed);
        }
    };
    Ok(())
}

enum State {
    Original(Bytes),
    Compressed(Vec<u8>),
}

struct DecompressorIter {
    stream: BytesMut,
    current: Option<State>, // when we have compression but the value is original
    compression: Option<Compression>,
    scratch: Vec<u8>,
}

impl DecompressorIter {
    pub fn new(stream: Bytes, compression: Option<Compression>, scratch: Vec<u8>) -> Self {
        Self {
            stream: BytesMut::from(stream.as_ref()),
            current: None,
            compression,
            scratch,
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        match self.current {
            Some(State::Compressed(some)) => some,
            _ => self.scratch,
        }
    }
}

impl FallibleStreamingIterator for DecompressorIter {
    type Item = [u8];

    type Error = Error;

    #[inline]
    fn advance(&mut self) -> Result<(), Self::Error> {
        if self.stream.is_empty() {
            self.current = None;
            return Ok(());
        }

        match self.compression {
            Some(compression) => {
                // todo: take stratch from current State::Compressed for re-use
                let (is_original, length) = decode_header(&self.stream);
                let _ = self.stream.split_to(3);
                let maybe_compressed = self.stream.split_to(length);

                if is_original {
                    self.current = Some(State::Original(maybe_compressed.into()));
                } else {
                    decompress_block(compression, &maybe_compressed, &mut self.scratch)?;
                    self.current = Some(State::Compressed(std::mem::take(&mut self.scratch)));
                }
                Ok(())
            }
            None => {
                // todo: take stratch from current State::Compressed for re-use
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

    /// Returns the internal memory region, so it can be re-used
    pub fn into_inner(self) -> Vec<u8> {
        self.decompressor.into_inner()
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
        let bytes = &[0b1011, 0, 0, 0];

        let (is_original, length) = decode_header(bytes);
        assert!(is_original);
        assert_eq!(length, 5);
    }

    #[test]
    fn decode_compressed() {
        // 100_000 compressed = [0x40, 0x0d, 0x03] = [0b01000000, 0b00001101, 0b00000011]
        let bytes = &[0b01000000, 0b00001101, 0b00000011, 0];

        let (is_original, length) = decode_header(bytes);
        assert!(!is_original);
        assert_eq!(length, 100_000);
    }
}
