pub mod decode;
pub mod decompress;
pub mod metadata;
pub mod schema;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncSeek};

use self::metadata::{read_metadata, FileMetadata};
use self::schema::{create_schema, TypeDescription};
use crate::arrow_reader::Cursor;
use crate::error::Result;
use crate::proto::{StripeFooter, StripeInformation};
use crate::reader::metadata::read_metadata_async;

pub struct Reader<R> {
    pub(crate) inner: R,
    metadata: Box<FileMetadata>,
    pub(crate) schema: Arc<TypeDescription>,
}

impl<R: ChunkReader> Reader<R> {
    pub fn new(mut r: R) -> Result<Self> {
        let metadata = Box::new(read_metadata(&mut r)?);
        let schema = create_schema(&metadata.footer.types, 0)?;

        Ok(Self {
            inner: r,
            metadata,
            schema,
        })
    }
}

impl<R: Read> Reader<R> {
    pub fn new_with_metadata(r: R, metadata: FileMetadata) -> Result<Self> {
        let schema = create_schema(&metadata.footer.types, 0)?;

        Ok(Self {
            inner: r,
            metadata: Box::new(metadata),
            schema,
        })
    }

    pub fn select(self, fields: &[&str]) -> Result<Cursor<R>> {
        Cursor::new(self, fields)
    }
}

impl<R> Reader<R> {
    pub fn metadata(&self) -> &FileMetadata {
        &self.metadata
    }

    pub fn schema(&self) -> &TypeDescription {
        &self.schema
    }

    pub fn stripe(&self, index: usize) -> Option<StripeInformation> {
        self.metadata.footer.stripes.get(index).cloned()
    }

    pub fn stripe_footer(&mut self, stripe: usize) -> &StripeFooter {
        &self.metadata.stripe_footers[stripe]
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin + Send> Reader<R> {
    pub async fn new_async(mut r: R) -> Result<Self> {
        let metadata = Box::new(read_metadata_async(&mut r).await?);
        let schema = create_schema(&metadata.footer.types, 0)?;

        Ok(Self {
            inner: r,
            metadata,
            schema,
        })
    }
}

/// Primary source used for reading required bytes for operations.
#[allow(clippy::len_without_is_empty)]
// TODO: async version
pub trait ChunkReader {
    type T: Read;

    /// Get total length of bytes. Useful for parsing the metadata located at
    /// the end of the file.
    fn len(&self) -> u64;

    /// Get a reader starting at a specific offset.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T>;

    /// Read bytes from an offset with specific length.
    fn get_bytes(&self, offset_from_start: u64, length: u64) -> std::io::Result<Vec<u8>> {
        let mut bytes = vec![0; length as usize];
        self.get_read(offset_from_start)?
            .take(length)
            .read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

impl ChunkReader for File {
    type T = BufReader<File>;

    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }

    /// Care needs to be taken when using this simulatenously as underlying
    /// file descriptor is the same and will be affected by other invocations.
    ///
    /// See [`File::try_clone()`] for more details.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T> {
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(offset_from_start))?;
        Ok(BufReader::new(self.try_clone()?))
    }
}
