use std::fmt::Debug;

use arrow::{array::BooleanBufferBuilder, buffer::NullBuffer};
use bytes::Bytes;

use crate::proto;

pub mod column;
pub mod stripe;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StreamType {
    Present,
    Data,
    Length,
    DictionaryData,
    Secondary,
}

impl From<StreamType> for proto::stream::Kind {
    fn from(value: StreamType) -> Self {
        match value {
            StreamType::Present => proto::stream::Kind::Present,
            StreamType::Data => proto::stream::Kind::Data,
            StreamType::Length => proto::stream::Kind::Length,
            StreamType::DictionaryData => proto::stream::Kind::DictionaryData,
            StreamType::Secondary => proto::stream::Kind::Secondary,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    s_type: StreamType,
    bytes: Bytes,
}

impl Stream {
    pub fn into_parts(self) -> (StreamType, Bytes) {
        (self.s_type, self.bytes)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ColumnEncoding {
    Direct,
    DirectV2,
    Dictionary { size: usize },
    DictionaryV2 { size: usize },
}

impl From<&ColumnEncoding> for proto::ColumnEncoding {
    fn from(value: &ColumnEncoding) -> Self {
        match value {
            ColumnEncoding::Direct => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::Direct.into()),
                dictionary_size: None,
                bloom_encoding: None,
            },
            ColumnEncoding::DirectV2 => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::DirectV2.into()),
                dictionary_size: None,
                bloom_encoding: None,
            },
            ColumnEncoding::Dictionary { size } => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::Dictionary.into()),
                dictionary_size: Some(*size as u32),
                bloom_encoding: None,
            },
            ColumnEncoding::DictionaryV2 { size } => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::DictionaryV2.into()),
                dictionary_size: Some(*size as u32),
                bloom_encoding: None,
            },
        }
    }
}

/// ORC encodes validity starting from MSB, whilst Arrow encodes it
/// from LSB.
struct PresentStreamEncoder {
    builder: BooleanBufferBuilder,
}

impl PresentStreamEncoder {
    pub fn new() -> Self {
        Self {
            builder: BooleanBufferBuilder::new(8),
        }
    }

    pub fn extend(&mut self, null_buffer: &NullBuffer) {
        let bb = null_buffer.inner();
        self.builder.append_buffer(bb);
    }

    /// Extend with n true bits.
    pub fn extend_present(&mut self, n: usize) {
        self.builder.append_n(n, true);
    }

    pub fn estimate_memory_size(&self) -> usize {
        self.builder.len() / 8
    }

    /// Produce ORC present stream bytes and reset internal builder.
    pub fn finish(&mut self) -> Bytes {
        let bb = self.builder.finish();
        // We use BooleanBufferBuilder so offset is 0
        let bytes = bb.values();
        // Reverse bits as ORC stores from MSB
        let bytes = bytes.iter().map(|b| b.reverse_bits()).collect::<Vec<_>>();
        bytes.into()
    }
}