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

use std::io;

use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit;
use arrow::error::ArrowError;
use snafu::prelude::*;
use snafu::Location;

use crate::proto;
use crate::schema::DataType;

// TODO: consolidate error types? better to have a smaller set?
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum OrcError {
    #[snafu(display("Failed to read, source: {}", source))]
    IoError {
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty file"))]
    EmptyFile {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Out of spec, message: {}", msg))]
    OutOfSpec {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode float, source: {}", source))]
    DecodeFloat {
        #[snafu(implicit)]
        location: Location,
        source: std::io::Error,
    },

    #[snafu(display(
        "Overflow while decoding timestamp (seconds={}, nanoseconds={}) to {:?}",
        seconds,
        nanoseconds,
        to_time_unit,
    ))]
    DecodeTimestamp {
        #[snafu(implicit)]
        location: Location,
        seconds: i64,
        nanoseconds: u64,
        to_time_unit: TimeUnit,
    },

    #[snafu(display("Failed to decode proto, source: {}", source))]
    DecodeProto {
        #[snafu(implicit)]
        location: Location,
        source: prost::DecodeError,
    },

    #[snafu(display("No types found"))]
    NoTypes {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("unsupported type variant: {}", msg))]
    UnsupportedTypeVariant {
        #[snafu(implicit)]
        location: Location,
        msg: &'static str,
    },

    #[snafu(display(
        "Cannot decode ORC type {:?} into Arrow type {:?}",
        orc_type,
        arrow_type,
    ))]
    MismatchedSchema {
        #[snafu(implicit)]
        location: Location,
        orc_type: DataType,
        arrow_type: ArrowDataType,
    },

    #[snafu(display("Invalid encoding for column '{}': {:?}", name, encoding))]
    InvalidColumnEncoding {
        #[snafu(implicit)]
        location: Location,
        name: String,
        encoding: proto::column_encoding::Kind,
    },

    #[snafu(display("Failed to convert to record batch: {}", source))]
    ConvertRecordBatch {
        #[snafu(implicit)]
        location: Location,
        source: ArrowError,
    },

    #[snafu(display("Varint being decoded is too large"))]
    VarintTooLarge {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("unexpected: {}", msg))]
    Unexpected {
        #[snafu(implicit)]
        location: Location,
        msg: String,
    },

    #[snafu(display("Failed to build zstd decoder: {}", source))]
    BuildZstdDecoder {
        #[snafu(implicit)]
        location: Location,
        source: io::Error,
    },

    #[snafu(display("Failed to build snappy decoder: {}", source))]
    BuildSnappyDecoder {
        #[snafu(implicit)]
        location: Location,
        source: snap::Error,
    },

    #[snafu(display("Failed to build lzo decoder: {}", source))]
    BuildLzoDecoder {
        #[snafu(implicit)]
        location: Location,
        source: lzokay_native::Error,
    },

    #[snafu(display("Failed to build lz4 decoder: {}", source))]
    BuildLz4Decoder {
        #[snafu(implicit)]
        location: Location,
        source: lz4_flex::block::DecompressError,
    },

    #[snafu(display("Arrow error: {}", source))]
    Arrow {
        source: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T, E = OrcError> = std::result::Result<T, E>;

impl From<OrcError> for ArrowError {
    fn from(value: OrcError) -> Self {
        ArrowError::ExternalError(Box::new(value))
    }
}
