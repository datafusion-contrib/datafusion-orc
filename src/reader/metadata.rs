use std::io::{Read, Seek, SeekFrom};

use bytes::Bytes;
use prost::Message;
use snafu::{OptionExt, ResultExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::error::{self, Result};
use crate::proto::{CompressionKind, Footer, Metadata, PostScript, StripeFooter};
use crate::reader::decompress::Decompressor;

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

/// The file's metadata.
#[derive(Debug)]
pub struct FileMetadata {
    pub postscript: PostScript,
    pub footer: Footer,
    pub metadata: Metadata,
    pub stripe_footers: Vec<StripeFooter>,
}

macro_rules! impl_read_metadata {
    ($reader:ident $($_await:tt)*) => {
        {
            let file_len = {
                let old_pos = $reader.stream_position()$($_await)*.context(error::SeekSnafu)?;
                let len = $reader.seek(SeekFrom::End(0))$($_await)*.context(error::SeekSnafu)?;

                // Avoid seeking a third time when we were already at the end of the
                // stream. The branch is usually way cheaper than a seek operation.
                if old_pos != len {
                    $reader.seek(SeekFrom::Start(old_pos))$($_await)*
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

            $reader
                .seek(SeekFrom::End(-(assume_footer_len as i64)))$($_await)*
                .context(error::SeekSnafu)?;
            let mut tail_bytes = Vec::with_capacity(assume_footer_len as usize);
            $reader
                .take(assume_footer_len)
                .read_to_end(&mut tail_bytes)$($_await)*
                .context(error::IoSnafu)?;

            // The final byte of the file contains the serialized length of the Postscript,
            // which must be less than 256 bytes.
            let postscript_len = tail_bytes[tail_bytes.len() - 1] as usize;
            tail_bytes.truncate(tail_bytes.len() - 1);

            // next is the postscript
            let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len..])
                .context(error::DecodeProtoSnafu)?;
            tail_bytes.truncate(tail_bytes.len() - postscript_len);

            // next is the footer
            let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
                msg: "Footer length is empty",
            })? as usize; // todo: throw error

            let footer_offset = file_len - footer_length as u64 - postscript_len as u64 - 1;

            $reader
                .seek(SeekFrom::Start(footer_offset))$($_await)*
                .context(error::SeekSnafu)?;
            let mut footer = vec![0; footer_length];
            $reader
                .read_exact(&mut footer)$($_await)*
                .context(error::SeekSnafu)?;
            let footer = deserialize_footer(&footer, postscript.compression())?;

            // finally the metadata
            let metadata_length = postscript.metadata_length.context(error::OutOfSpecSnafu {
                msg: "Metadata length is empty",
            })? as usize;
            let metadata_offset =
                file_len - metadata_length as u64 - footer_length as u64 - postscript_len as u64 - 1;

            $reader
                .seek(SeekFrom::Start(metadata_offset))$($_await)*
                .context(error::SeekSnafu)?;
            let mut metadata = vec![0; metadata_length];
            $reader.read_exact(&mut metadata)$($_await)*.context(error::IoSnafu)?;

            let metadata = deserialize_footer_metadata(&metadata, postscript.compression())?;

            let mut stripe_footers = Vec::with_capacity(footer.stripes.len());

            let mut scratch = Vec::<u8>::new();

            for stripe in &footer.stripes {
                let start = stripe.offset() + stripe.index_length() + stripe.data_length();
                let len = stripe.footer_length();
                $reader
                    .seek(SeekFrom::Start(start))$($_await)*
                    .context(error::SeekSnafu)?;

                scratch.clear();
                scratch.reserve(len as usize);
                $reader
                    .take(len)
                    .read_to_end(&mut scratch)$($_await)*
                    .context(error::IoSnafu)?;
                stripe_footers.push(deserialize_stripe_footer(
                    &scratch,
                    postscript.compression(),
                )?);
            }

            Ok(FileMetadata {
                postscript,
                footer,
                metadata,
                stripe_footers,
            })
        }
    };
}

pub fn read_metadata<R>(reader: &mut R) -> Result<FileMetadata>
where
    R: Read + Seek,
{
    impl_read_metadata!(reader)
}

pub async fn read_metadata_async<R>(reader: &mut R) -> Result<FileMetadata>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    impl_read_metadata!(reader.await)
}

fn deserialize_footer(bytes: &[u8], compression: CompressionKind) -> Result<Footer> {
    let mut buffer = vec![];
    Decompressor::new(Bytes::copy_from_slice(bytes), compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Footer::decode(&*buffer).context(error::DecodeProtoSnafu)
}

fn deserialize_footer_metadata(bytes: &[u8], compression: CompressionKind) -> Result<Metadata> {
    let mut buffer = vec![];
    Decompressor::new(Bytes::copy_from_slice(bytes), compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    Metadata::decode(&*buffer).context(error::DecodeProtoSnafu)
}

fn deserialize_stripe_footer(bytes: &[u8], compression: CompressionKind) -> Result<StripeFooter> {
    let mut buffer = vec![];
    Decompressor::new(Bytes::copy_from_slice(bytes), compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    StripeFooter::decode(&*buffer).context(error::DecodeProtoSnafu)
}
