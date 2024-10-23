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

//! Parse ORC file tail metadata structures from file.
//!
//! File tail structure:
//!
//! ```text
//! ------------------
//! |    Metadata    |
//! |                |
//! ------------------
//! |     Footer     |
//! |                |
//! ------------------
//! |  Postscript  |X|
//! ------------------
//! ```
//!
//! Where X is last byte in file indicating
//! Postscript length in bytes.
//!
//! Footer and Metadata lengths are encoded in Postscript.
//! Postscript is never compressed, Footer and Metadata
//! may be compressed depending Postscript config value.
//!
//! If they are compressed then their lengths indicate their
//! compressed lengths.

use std::collections::HashMap;
use std::io::Read;

use bytes::{Bytes, BytesMut};
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};

use crate::compression::{Compression, Decompressor};
use crate::error::{self, EmptyFileSnafu, OutOfSpecSnafu, Result};
use crate::proto::{self, Footer, Metadata, PostScript};
use crate::schema::RootDataType;
use crate::statistics::ColumnStatistics;
use crate::stripe::StripeMetadata;

use crate::reader::ChunkReader;

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

/// The file's metadata.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    compression: Option<Compression>,
    root_data_type: RootDataType,
    number_of_rows: u64,
    file_format_version: String,
    /// Statistics of columns across entire file
    column_statistics: Vec<ColumnStatistics>,
    stripes: Vec<StripeMetadata>,
    user_custom_metadata: HashMap<String, Vec<u8>>,
}

impl FileMetadata {
    fn from_proto(
        postscript: &proto::PostScript,
        footer: &proto::Footer,
        metadata: &proto::Metadata,
    ) -> Result<Self> {
        let compression =
            Compression::from_proto(postscript.compression(), postscript.compression_block_size);
        let root_data_type = RootDataType::from_proto(&footer.types)?;
        let number_of_rows = footer.number_of_rows();
        let column_statistics = footer
            .statistics
            .iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            metadata.stripe_stats.is_empty() || metadata.stripe_stats.len() == footer.stripes.len(),
            OutOfSpecSnafu {
                msg: "stripe stats length must equal the number of stripes"
            }
        );
        // TODO: confirm if this is valid
        let stripes = if metadata.stripe_stats.is_empty() {
            footer
                .stripes
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        } else {
            footer
                .stripes
                .iter()
                .zip(metadata.stripe_stats.iter())
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        };
        let user_custom_metadata = footer
            .metadata
            .iter()
            .map(|kv| (kv.name().to_owned(), kv.value().to_vec()))
            .collect::<HashMap<_, _>>();

        let file_format_version = postscript
            .version
            .iter()
            .map(|v| v.to_string() + ".")
            .collect::<String>();
        let file_format_version = file_format_version
            .strip_suffix('.')
            .unwrap_or("")
            .to_string();

        Ok(Self {
            compression,
            root_data_type,
            number_of_rows,
            file_format_version,
            column_statistics,
            stripes,
            user_custom_metadata,
        })
    }

    pub fn number_of_rows(&self) -> u64 {
        self.number_of_rows
    }

    pub fn compression(&self) -> Option<Compression> {
        self.compression
    }

    pub fn root_data_type(&self) -> &RootDataType {
        &self.root_data_type
    }

    pub fn column_file_statistics(&self) -> &[ColumnStatistics] {
        &self.column_statistics
    }

    pub fn stripe_metadatas(&self) -> &[StripeMetadata] {
        &self.stripes
    }

    pub fn user_custom_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.user_custom_metadata
    }

    pub fn file_format_version(&self) -> &str {
        &self.file_format_version
    }
}

pub fn read_metadata<R: ChunkReader>(reader: &mut R) -> Result<FileMetadata> {
    let file_len = reader.len();
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }

    // Initial read of the file tail
    // Use a default size for first read in hopes of capturing all sections with one read
    // At worst need two reads to get all necessary bytes
    let assume_footer_len = file_len.min(DEFAULT_FOOTER_SIZE);
    let mut tail_bytes = reader
        .get_bytes(file_len - assume_footer_len, assume_footer_len)
        .context(error::IoSnafu)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as u64;
    tail_bytes.truncate(tail_bytes.len() - 1);

    if tail_bytes.len() < postscript_len as usize {
        return OutOfSpecSnafu {
            msg: "File too small for given postscript length",
        }
        .fail();
    }
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len as usize..])
        .context(error::DecodeProtoSnafu)?;
    let compression =
        Compression::from_proto(postscript.compression(), postscript.compression_block_size);
    tail_bytes.truncate(tail_bytes.len() - postscript_len as usize);

    let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
        msg: "Footer length is empty",
    })?;
    let metadata_length = postscript.metadata_length.context(error::OutOfSpecSnafu {
        msg: "Metadata length is empty",
    })?;

    // Ensure we have enough bytes for Footer and Metadata
    let mut tail_bytes = if footer_length + metadata_length > tail_bytes.len() as u64 {
        // Need second read
        // -1 is the postscript length byte
        let offset = file_len - 1 - postscript_len - footer_length - metadata_length;
        let bytes_to_read = (footer_length + metadata_length) - tail_bytes.len() as u64;
        let prepend_bytes = reader
            .get_bytes(offset, bytes_to_read)
            .context(error::IoSnafu)?;
        let mut all_bytes = BytesMut::with_capacity(prepend_bytes.len() + tail_bytes.len());
        all_bytes.extend_from_slice(&prepend_bytes);
        all_bytes.extend_from_slice(&tail_bytes);
        all_bytes.into()
    } else {
        tail_bytes
    };

    let footer = deserialize_footer(
        tail_bytes.slice(tail_bytes.len() - footer_length as usize..),
        compression,
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let metadata = deserialize_footer_metadata(
        tail_bytes.slice(tail_bytes.len() - metadata_length as usize..),
        compression,
    )?;

    FileMetadata::from_proto(&postscript, &footer, &metadata)
}

#[cfg(feature = "async")]
pub async fn read_metadata_async<R: super::AsyncChunkReader>(
    reader: &mut R,
) -> Result<FileMetadata> {
    let file_len = reader.len().await.context(error::IoSnafu)?;
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }

    // Initial read of the file tail
    // Use a default size for first read in hopes of capturing all sections with one read
    // At worst need two reads to get all necessary bytes
    let assume_footer_len = file_len.min(DEFAULT_FOOTER_SIZE);
    let mut tail_bytes = reader
        .get_bytes(file_len - assume_footer_len, assume_footer_len)
        .await
        .context(error::IoSnafu)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as u64;
    tail_bytes.truncate(tail_bytes.len() - 1);

    if tail_bytes.len() < postscript_len as usize {
        return OutOfSpecSnafu {
            msg: "File too small for given postscript length",
        }
        .fail();
    }
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len as usize..])
        .context(error::DecodeProtoSnafu)?;
    let compression =
        Compression::from_proto(postscript.compression(), postscript.compression_block_size);
    tail_bytes.truncate(tail_bytes.len() - postscript_len as usize);

    let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
        msg: "Footer length is empty",
    })?;
    let metadata_length = postscript.metadata_length.context(error::OutOfSpecSnafu {
        msg: "Metadata length is empty",
    })?;

    // Ensure we have enough bytes for Footer and Metadata
    let mut tail_bytes = if footer_length + metadata_length > tail_bytes.len() as u64 {
        // Need second read
        // -1 is the postscript length byte
        let offset = file_len - 1 - postscript_len - footer_length - metadata_length;
        let bytes_to_read = (footer_length + metadata_length) - tail_bytes.len() as u64;
        let prepend_bytes = reader
            .get_bytes(offset, bytes_to_read)
            .await
            .context(error::IoSnafu)?;
        let mut all_bytes = BytesMut::with_capacity(prepend_bytes.len() + tail_bytes.len());
        all_bytes.extend_from_slice(&prepend_bytes);
        all_bytes.extend_from_slice(&tail_bytes);
        all_bytes.into()
    } else {
        tail_bytes
    };

    let footer = deserialize_footer(
        tail_bytes.slice(tail_bytes.len() - footer_length as usize..),
        compression,
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let metadata = deserialize_footer_metadata(
        tail_bytes.slice(tail_bytes.len() - metadata_length as usize..),
        compression,
    )?;

    FileMetadata::from_proto(&postscript, &footer, &metadata)
}

fn deserialize_footer(bytes: Bytes, compression: Option<Compression>) -> Result<Footer> {
    let mut buffer = vec![];
    Decompressor::new(bytes, compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Footer::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

fn deserialize_footer_metadata(bytes: Bytes, compression: Option<Compression>) -> Result<Metadata> {
    let mut buffer = vec![];
    Decompressor::new(bytes, compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Metadata::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}
