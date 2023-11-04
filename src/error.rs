use std::io;
use std::str::Utf8Error;

use arrow::error::ArrowError;
pub use snafu::prelude::*;
use snafu::Location;

use crate::proto::r#type::Kind;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to seek, source: {}", source))]
    SeekError {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("Failed to read, source: {}", source))]
    IoError {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("Invalid input, message: {}", msg))]
    InvalidInput { msg: String, location: Location },

    #[snafu(display("Out of sepc, message: {}", msg))]
    OutOfSpec { msg: String, location: Location },

    #[snafu(display("Failed to decode float, source: {}", source))]
    DecodeFloat {
        location: Location,
        source: std::io::Error,
    },

    #[snafu(display("Failed to decode proto, source: {}", source))]
    DecodeProto {
        location: Location,
        source: prost::DecodeError,
    },

    #[snafu(display("No types found"))]
    NoTypes { location: Location },

    #[snafu(display("unsupported type: {:?}", kind))]
    UnsupportedType { location: Location, kind: Kind },

    #[snafu(display("Field not found: {:?}", name))]
    FieldNotFound { location: Location, name: String },

    #[snafu(display("Invalid column : {:?}", name))]
    InvalidColumn { location: Location, name: String },

    #[snafu(display("Failed to convert to timestamp"))]
    InvalidTimestamp { location: Location },

    #[snafu(display("Failed to convert to date"))]
    InvalidDate { location: Location },

    #[snafu(display("Failed to add day to a date"))]
    AddDays { location: Location },

    #[snafu(display("Invalid utf8, source: {}", source))]
    InvalidUft8 {
        location: Location,
        source: Utf8Error,
    },

    #[snafu(display("Out of bound at: {}", index))]
    OutOfBound { location: Location, index: usize },

    #[snafu(display("Failed to convert to record batch: {}", source))]
    ConvertRecordBatch {
        location: Location,
        source: ArrowError,
    },

    #[snafu(display("Unsigned VInt"))]
    EofUnsignedVInt { location: Location },

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
}

pub type Result<T> = std::result::Result<T, Error>;
