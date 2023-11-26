//! Parse ORC file tail metadata structures from file.
//!
//! File tail structure:
//!
//! ------------------
//! |    Metadata    |
//! |                |
//! ------------------
//! |     Footer     |
//! |                |
//! ------------------
//! |  Postscript  |X|
//! ------------------
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
use snafu::{OptionExt, ResultExt};

use crate::error::{self, EmptyFileSnafu, OutOfSpecSnafu, Result};
use crate::proto::{self, Footer, Metadata, PostScript, StripeFooter};
use crate::reader::decompress::Decompressor;
use crate::schema::RootDataType;
use crate::statistics::ColumnStatistics;
use crate::stripe::StripeMetadata;

use super::decompress::Compression;
use super::{AsyncChunkReader, ChunkReader};

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

/// The file's metadata.
#[derive(Debug)]
pub struct FileMetadata {
    compression: Option<Compression>,
    root_data_type: RootDataType,
    number_of_rows: u64,
    /// Statistics of columns across entire file
    column_statistics: Vec<ColumnStatistics>,
    stripes: Vec<StripeMetadata>,
    user_custom_metadata: HashMap<String, Vec<u8>>,
    // TODO: for now keeping this, but ideally won't want all stripe footers here
    //       since don't want to require parsing all stripe footers in file unless actually required
    stripe_footers: Vec<StripeFooter>,
}

impl FileMetadata {
    fn from_proto(
        postscript: &proto::PostScript,
        footer: &proto::Footer,
        metadata: &proto::Metadata,
        stripe_footers: Vec<StripeFooter>,
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
        let stripes = footer
            .stripes
            .iter()
            .zip(metadata.stripe_stats.iter())
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;
        let user_custom_metadata = footer
            .metadata
            .iter()
            .map(|kv| (kv.name().to_owned(), kv.value().to_vec()))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            compression,
            root_data_type,
            number_of_rows,
            column_statistics,
            stripes,
            user_custom_metadata,
            stripe_footers,
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

    pub fn stripe_footers(&self) -> &[StripeFooter] {
        &self.stripe_footers
    }

    pub fn user_custom_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.user_custom_metadata
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
        &tail_bytes[tail_bytes.len() - footer_length as usize..],
        compression,
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let metadata = deserialize_footer_metadata(
        &tail_bytes[tail_bytes.len() - metadata_length as usize..],
        compression,
    )?;

    let mut stripe_footers = Vec::with_capacity(footer.stripes.len());

    // TODO: move out of here
    // clippy read_zero_byte_vec lint causing issues so init to non-zero length
    let mut scratch = vec![0];
    for stripe in &footer.stripes {
        let offset = stripe.offset() + stripe.index_length() + stripe.data_length();
        let len = stripe.footer_length();

        let mut read = reader.get_read(offset).context(error::IoSnafu)?;
        scratch.resize(len as usize, 0);
        read.read_exact(&mut scratch).context(error::IoSnafu)?;
        stripe_footers.push(deserialize_stripe_footer(&scratch, compression)?);
    }

    FileMetadata::from_proto(&postscript, &footer, &metadata, stripe_footers)
}

pub async fn read_metadata_async<R: AsyncChunkReader>(reader: &mut R) -> Result<FileMetadata> {
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
        &tail_bytes[tail_bytes.len() - footer_length as usize..],
        compression,
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let metadata = deserialize_footer_metadata(
        &tail_bytes[tail_bytes.len() - metadata_length as usize..],
        compression,
    )?;

    let mut stripe_footers = Vec::with_capacity(footer.stripes.len());

    // TODO: move out of here
    for stripe in &footer.stripes {
        let offset = stripe.offset() + stripe.index_length() + stripe.data_length();
        let len = stripe.footer_length();

        let bytes = reader
            .get_bytes(offset, len)
            .await
            .context(error::IoSnafu)?;
        stripe_footers.push(deserialize_stripe_footer(&bytes, compression)?);
    }

    FileMetadata::from_proto(&postscript, &footer, &metadata, stripe_footers)
}

fn deserialize_footer(bytes: &[u8], compression: Option<Compression>) -> Result<Footer> {
    let mut buffer = vec![];
    // TODO: refactor to not need Bytes::copy_from_slice
    Decompressor::new(Bytes::copy_from_slice(bytes), compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Footer::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

fn deserialize_footer_metadata(bytes: &[u8], compression: Option<Compression>) -> Result<Metadata> {
    let mut buffer = vec![];
    // TODO: refactor to not need Bytes::copy_from_slice
    Decompressor::new(Bytes::copy_from_slice(bytes), compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Metadata::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

fn deserialize_stripe_footer(
    bytes: &[u8],
    compression: Option<Compression>,
) -> Result<StripeFooter> {
    let mut buffer = vec![];
    // TODO: refactor to not need Bytes::copy_from_slice
    Decompressor::new(Bytes::copy_from_slice(bytes), compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    StripeFooter::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}
