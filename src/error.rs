use std::io;

use arrow::datatypes::DataType as ArrowDataType;
use arrow::error::ArrowError;
use snafu::prelude::*;
use snafu::Location;

use crate::proto;
use crate::schema::DataType;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum OrcError {
    #[snafu(display("Failed to read, source: {}", source))]
    IoError {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("Empty file"))]
    EmptyFile { location: Location },

    #[snafu(display("Out of spec, message: {}", msg))]
    OutOfSpec { msg: String, location: Location },

    #[snafu(display("Failed to decode float, source: {}", source))]
    DecodeFloat {
        location: Location,
        source: std::io::Error,
    },

    #[snafu(display(
        "Overflow while decoding timestamp (seconds={}, nanoseconds={}) to nanoseconds",
        seconds,
        nanoseconds
    ))]
    DecodeTimestamp {
        location: Location,
        seconds: i64,
        nanoseconds: u64,
    },

    #[snafu(display("Failed to decode proto, source: {}", source))]
    DecodeProto {
        location: Location,
        source: prost::DecodeError,
    },

    #[snafu(display("No types found"))]
    NoTypes { location: Location },

    #[snafu(display("unsupported type variant: {}", msg))]
    UnsupportedTypeVariant {
        location: Location,
        msg: &'static str,
    },

    #[snafu(display(
        "Cannot decode ORC type {:?} into Arrow type {:?}",
        orc_type,
        arrow_type,
    ))]
    MismatchedSchema {
        location: Location,
        orc_type: DataType,
        arrow_type: ArrowDataType,
    },

    #[snafu(display("Invalid encoding for column '{}': {:?}", name, encoding))]
    InvalidColumnEncoding {
        location: Location,
        name: String,
        encoding: proto::column_encoding::Kind,
    },

    #[snafu(display("Failed to convert to record batch: {}", source))]
    ConvertRecordBatch {
        location: Location,
        source: ArrowError,
    },

    #[snafu(display("Varint being decoded is too large"))]
    VarintTooLarge { location: Location },

    #[snafu(display("unexpected: {}", msg))]
    Unexpected { location: Location, msg: String },

    #[snafu(display("Failed to build zstd decoder: {}", source))]
    BuildZstdDecoder {
        location: Location,
        source: io::Error,
    },

    #[snafu(display("Failed to build snappy decoder: {}", source))]
    BuildSnappyDecoder {
        location: Location,
        source: snap::Error,
    },

    #[snafu(display("Failed to build lzo decoder: {}", source))]
    BuildLzoDecoder {
        location: Location,
        source: lzokay_native::Error,
    },

    #[snafu(display("Failed to build lz4 decoder: {}", source))]
    BuildLz4Decoder {
        location: Location,
        source: lz4_flex::block::DecompressError,
    },

    #[snafu(display("Arrow error: {}", source))]
    Arrow {
        source: arrow::error::ArrowError,
        location: Location,
    },
}

pub type Result<T, E = OrcError> = std::result::Result<T, E>;

impl From<OrcError> for ArrowError {
    fn from(value: OrcError) -> Self {
        ArrowError::ExternalError(Box::new(value))
    }
}
