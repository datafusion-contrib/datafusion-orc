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
use std::io::{Read, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use snafu::{OptionExt, ResultExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::error::{self, Result};
use crate::proto::{self, Footer, Metadata, PostScript, StripeFooter};
use crate::reader::decompress::Decompressor;
use crate::statistics::ColumnStatistics;
use crate::stripe::StripeMetadata;

use super::decompress::Compression;
use super::schema::{create_schema, TypeDescription};
use super::ChunkReader;

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

/// The file's metadata.
#[derive(Debug)]
pub struct FileMetadata {
    compression: Option<Compression>,
    type_description: Arc<TypeDescription>,
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
        let type_description = create_schema(&footer.types, 0)?;
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
            type_description,
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

    pub fn type_description(&self) -> &Arc<TypeDescription> {
        &self.type_description
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

pub fn read_metadata<R>(reader: &mut R) -> Result<FileMetadata>
where
    R: ChunkReader,
{
    let file_len = reader.len();
    // TODO: return error if empty

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

    // TODO: slice here could panic if file too small
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
        let mut prepend_bytes = reader
            .get_bytes(offset, bytes_to_read)
            .context(error::IoSnafu)?;
        prepend_bytes.extend(tail_bytes);
        prepend_bytes
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

// TODO: refactor like for sync
pub async fn read_metadata_async<R>(reader: &mut R) -> Result<FileMetadata>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    let file_len = {
        let old_pos = reader.stream_position().await.context(error::SeekSnafu)?;
        let len = reader
            .seek(SeekFrom::End(0))
            .await
            .context(error::SeekSnafu)?;

        // Avoid seeking a third time when we were already at the end of the
        // stream. The branch is usually way cheaper than a seek operation.
        if old_pos != len {
            reader
                .seek(SeekFrom::Start(old_pos))
                .await
                .context(error::SeekSnafu)?;
        }
        len
    };

    // initial read of the footer
    let assume_footer_len = if file_len < DEFAULT_FOOTER_SIZE {
        file_len
    } else {
        DEFAULT_FOOTER_SIZE
    };

    reader
        .seek(SeekFrom::End(-(assume_footer_len as i64)))
        .await
        .context(error::SeekSnafu)?;
    let mut tail_bytes = Vec::with_capacity(assume_footer_len as usize);
    reader
        .take(assume_footer_len)
        .read_to_end(&mut tail_bytes)
        .await
        .context(error::IoSnafu)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as usize;
    tail_bytes.truncate(tail_bytes.len() - 1);

    // next is the postscript
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len..])
        .context(error::DecodeProtoSnafu)?;
    let compression =
        Compression::from_proto(postscript.compression(), postscript.compression_block_size);
    tail_bytes.truncate(tail_bytes.len() - postscript_len);

    // next is the footer
    let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
        msg: "Footer length is empty",
    })? as usize; // todo: throw error

    let footer_offset = file_len - footer_length as u64 - postscript_len as u64 - 1;

    reader
        .seek(SeekFrom::Start(footer_offset))
        .await
        .context(error::SeekSnafu)?;
    let mut footer = vec![0; footer_length];
    reader
        .read_exact(&mut footer)
        .await
        .context(error::SeekSnafu)?;
    let footer = deserialize_footer(&footer, compression)?;

    // finally the metadata
    let metadata_length = postscript.metadata_length.context(error::OutOfSpecSnafu {
        msg: "Metadata length is empty",
    })? as usize;
    let metadata_offset =
        file_len - metadata_length as u64 - footer_length as u64 - postscript_len as u64 - 1;

    reader
        .seek(SeekFrom::Start(metadata_offset))
        .await
        .context(error::SeekSnafu)?;
    let mut metadata = vec![0; metadata_length];
    reader
        .read_exact(&mut metadata)
        .await
        .context(error::IoSnafu)?;

    let metadata = deserialize_footer_metadata(&metadata, compression)?;

    let mut stripe_footers = Vec::with_capacity(footer.stripes.len());

    let mut scratch = Vec::<u8>::new();

    for stripe in &footer.stripes {
        let start = stripe.offset() + stripe.index_length() + stripe.data_length();
        let len = stripe.footer_length();
        reader
            .seek(SeekFrom::Start(start))
            .await
            .context(error::SeekSnafu)?;

        scratch.clear();
        scratch.reserve(len as usize);
        reader
            .take(len)
            .read_to_end(&mut scratch)
            .await
            .context(error::IoSnafu)?;
        stripe_footers.push(deserialize_stripe_footer(&scratch, compression)?);
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
