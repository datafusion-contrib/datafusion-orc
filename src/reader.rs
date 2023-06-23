pub mod decode;
pub mod decompress;
pub mod metadata;
pub mod schema;

use std::io::{Read, Seek};
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

impl<R: Read + Seek> Reader<R> {
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
