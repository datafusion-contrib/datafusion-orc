pub mod decode;
pub mod decompress;
pub mod metadata;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use tokio::io::{AsyncRead, AsyncSeek};

use self::metadata::{read_metadata, FileMetadata};
use crate::error::Result;
use crate::proto::StripeFooter;
use crate::reader::metadata::read_metadata_async;
use crate::schema::RootDataType;
use crate::stripe::StripeMetadata;

pub struct Reader<R> {
    pub(crate) inner: R,
    metadata: Box<FileMetadata>,
}

impl<R: ChunkReader> Reader<R> {
    pub fn new(mut r: R) -> Result<Self> {
        let metadata = Box::new(read_metadata(&mut r)?);
        Ok(Self { inner: r, metadata })
    }
}

impl<R> Reader<R> {
    pub fn metadata(&self) -> &FileMetadata {
        &self.metadata
    }

    pub fn schema(&self) -> &RootDataType {
        self.metadata.root_data_type()
    }

    pub fn stripe(&self, index: usize) -> Option<&StripeMetadata> {
        self.metadata.stripe_metadatas().get(index)
    }

    pub fn stripe_footer(&mut self, stripe: usize) -> &StripeFooter {
        &self.metadata.stripe_footers()[stripe]
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin + Send> Reader<R> {
    pub async fn new_async(mut r: R) -> Result<Self> {
        let metadata = Box::new(read_metadata_async(&mut r).await?);
        Ok(Self { inner: r, metadata })
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

    /// Care needs to be taken when using this simultaneously as underlying
    /// file descriptor is the same and will be affected by other invocations.
    ///
    /// See [`File::try_clone()`] for more details.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T> {
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(offset_from_start))?;
        Ok(BufReader::new(self.try_clone()?))
    }
}
