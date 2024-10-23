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

pub mod metadata;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use bytes::{Buf, Bytes};

/// Primary source used for reading required bytes for operations.
#[allow(clippy::len_without_is_empty)]
pub trait ChunkReader {
    type T: Read;

    /// Get total length of bytes. Useful for parsing the metadata located at
    /// the end of the file.
    // TODO: this is only used for file tail, so replace with load_metadata?
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

impl ChunkReader for Bytes {
    type T = bytes::buf::Reader<Bytes>;

    fn len(&self) -> u64 {
        self.len() as u64
    }

    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T> {
        Ok(self.slice(offset_from_start as usize..).reader())
    }
}

#[cfg(feature = "async")]
mod async_chunk_reader {
    use super::*;

    use futures_util::future::BoxFuture;
    use futures_util::FutureExt;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

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
}

#[cfg(feature = "async")]
pub use async_chunk_reader::AsyncChunkReader;

#[cfg(all(feature = "async", feature = "opendal"))]
mod async_opendal_reader {
    use crate::reader::AsyncChunkReader;
    use bytes::Bytes;
    use futures_util::future::BoxFuture;
    use opendal::Operator;
    use std::sync::Arc;

    /// AsyncOpendalReader provides native support for [`opendal`]
    ///
    /// ```
    /// use opendal::Operator;
    /// use std::io::Result;
    /// use orc_rust::reader::AsyncOpendalReader;
    /// use orc_rust::reader::AsyncChunkReader;
    /// use opendal::services::MemoryConfig;
    ///
    /// # async fn test() -> Result<()> {
    /// let op = Operator::from_config(MemoryConfig::default())?.finish();
    /// op.write("test", "Hello, world!").await?;
    ///
    /// let mut reader = AsyncOpendalReader::new(op, "test");
    /// let len = reader.len().await?;
    /// let data = reader.get_bytes(0, len).await?;
    /// #    Ok(())
    /// # }
    /// ```
    pub struct AsyncOpendalReader {
        op: Operator,
        path: Arc<String>,
    }

    impl AsyncOpendalReader {
        /// Create a new async opendal reader.
        pub fn new(op: Operator, path: &str) -> Self {
            Self {
                op,
                path: Arc::new(path.to_string()),
            }
        }
    }

    impl AsyncChunkReader for AsyncOpendalReader {
        fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
            let path = self.path.clone();
            Box::pin(async move {
                let meta = self.op.stat(&path).await?;
                Ok(meta.content_length())
            })
        }

        fn get_bytes(
            &mut self,
            offset_from_start: u64,
            length: u64,
        ) -> BoxFuture<'_, std::io::Result<Bytes>> {
            let path = self.path.clone();

            Box::pin(async move {
                let reader = self
                    .op
                    .read_with(&path)
                    .range(offset_from_start..offset_from_start + length)
                    .await?;
                Ok(reader.to_bytes())
            })
        }
    }
}

#[cfg(all(feature = "async", feature = "opendal"))]
pub use async_opendal_reader::AsyncOpendalReader;
