pub mod decode;
pub mod decompress;
pub mod metadata;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

/// Primary source used for reading required bytes for operations.
#[allow(clippy::len_without_is_empty)]
pub trait ChunkReader {
    type T: Read;

    /// Get total length of bytes. Useful for parsing the metadata located at
    /// the end of the file.
    fn len(&self) -> u64;

    /// Get a reader starting at a specific offset.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T>;

    /// Read bytes from an offset with specific length.
    fn get_bytes(&self, offset_from_start: u64, length: u64) -> std::io::Result<Bytes> {
        let mut bytes = vec![0; length as usize];
        self.get_read(offset_from_start)?
            .take(length)
            .read_exact(&mut bytes)?;
        Ok(bytes.into())
    }
}

impl ChunkReader for File {
    type T = BufReader<File>;

    // TODO: this is only used for file tail, so replace with load_metadata?
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

#[allow(clippy::len_without_is_empty)]
pub trait AsyncChunkReader: Send {
    // TODO: this is only used for file tail, so replace with load_metadata?
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>>;

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>>;
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncChunkReader for T {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        async move { self.seek(SeekFrom::End(0)).await }.boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        async move {
            self.seek(SeekFrom::Start(offset_from_start)).await?;
            let mut buffer = vec![0; length as usize];
            self.read_exact(&mut buffer).await?;
            Ok(buffer.into())
        }
        .boxed()
    }
}

impl AsyncChunkReader for Box<dyn AsyncChunkReader> {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        self.as_mut().len()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        self.as_mut().get_bytes(offset_from_start, length)
    }
}
