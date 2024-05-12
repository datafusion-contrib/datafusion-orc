use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::error::Result;
use crate::projection::ProjectionMask;
#[cfg(feature = "async")]
use crate::reader::metadata::read_metadata_async;
use crate::reader::metadata::{read_metadata, FileMetadata};
#[cfg(feature = "async")]
use crate::reader::AsyncChunkReader;
use crate::reader::ChunkReader;
use crate::schema::RootDataType;
use crate::stripe::Stripe;
#[cfg(feature = "async")]
use crate::ArrowStreamReader;

use self::decoder::NaiveStripeDecoder;

pub(crate) mod column;
pub(crate) mod decoder;

const DEFAULT_BATCH_SIZE: usize = 8192;

pub struct ArrowReaderBuilder<R> {
    reader: R,
    file_metadata: Arc<FileMetadata>,
    batch_size: usize,
    projection: ProjectionMask,
}

impl<R> ArrowReaderBuilder<R> {
    fn new(reader: R, file_metadata: Arc<FileMetadata>) -> Self {
        Self {
            reader,
            file_metadata,
            batch_size: DEFAULT_BATCH_SIZE,
            projection: ProjectionMask::all(),
        }
    }

    pub fn file_metadata(&self) -> &FileMetadata {
        &self.file_metadata
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_projection(mut self, projection: ProjectionMask) -> Self {
        self.projection = projection;
        self
    }
}

impl<R: ChunkReader> ArrowReaderBuilder<R> {
    pub fn try_new(mut reader: R) -> Result<Self> {
        let file_metadata = Arc::new(read_metadata(&mut reader)?);
        Ok(Self::new(reader, file_metadata))
    }

    pub fn build(self) -> ArrowReader<R> {
        let projected_data_type = self
            .file_metadata
            .root_data_type()
            .project(&self.projection);
        let cursor = Cursor {
            reader: self.reader,
            file_metadata: self.file_metadata,
            projected_data_type,
            stripe_index: 0,
        };
        let schema_ref = Arc::new(create_arrow_schema(&cursor));
        ArrowReader {
            cursor,
            schema_ref,
            current_stripe: None,
            batch_size: self.batch_size,
        }
    }
}

#[cfg(feature = "async")]
impl<R: AsyncChunkReader + 'static> ArrowReaderBuilder<R> {
    pub async fn try_new_async(mut reader: R) -> Result<Self> {
        let file_metadata = Arc::new(read_metadata_async(&mut reader).await?);
        Ok(Self::new(reader, file_metadata))
    }

    pub fn build_async(self) -> ArrowStreamReader<R> {
        let projected_data_type = self
            .file_metadata
            .root_data_type()
            .project(&self.projection);
        let cursor = Cursor {
            reader: self.reader,
            file_metadata: self.file_metadata,
            projected_data_type,
            stripe_index: 0,
        };
        let schema_ref = Arc::new(create_arrow_schema(&cursor));
        ArrowStreamReader::new(cursor, self.batch_size, schema_ref)
    }
}

pub struct ArrowReader<R> {
    cursor: Cursor<R>,
    schema_ref: SchemaRef,
    current_stripe: Option<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>>,
    batch_size: usize,
}

impl<R> ArrowReader<R> {
    pub fn total_row_count(&self) -> u64 {
        self.cursor.file_metadata.number_of_rows()
    }
}

impl<R: ChunkReader> ArrowReader<R> {
    fn try_advance_stripe(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let stripe = self.cursor.next().transpose()?;
        match stripe {
            Some(stripe) => {
                let decoder =
                    NaiveStripeDecoder::new(stripe, self.schema_ref.clone(), self.batch_size)?;
                self.current_stripe = Some(Box::new(decoder));
                self.next().transpose()
            }
            None => Ok(None),
        }
    }
}

fn create_arrow_schema<R>(cursor: &Cursor<R>) -> Schema {
    let metadata = cursor
        .file_metadata
        .user_custom_metadata()
        .iter()
        .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).to_string()))
        .collect::<HashMap<_, _>>();
    cursor.projected_data_type.create_arrow_schema(&metadata)
}

impl<R: ChunkReader> RecordBatchReader for ArrowReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }
}

impl<R: ChunkReader> Iterator for ArrowReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_stripe.as_mut() {
            Some(stripe) => {
                match stripe
                    .next()
                    .map(|batch| batch.map_err(|err| ArrowError::ExternalError(Box::new(err))))
                {
                    Some(rb) => Some(rb),
                    None => self.try_advance_stripe().transpose(),
                }
            }
            None => self.try_advance_stripe().transpose(),
        }
    }
}

pub(crate) struct Cursor<R> {
    pub reader: R,
    pub file_metadata: Arc<FileMetadata>,
    pub projected_data_type: RootDataType,
    pub stripe_index: usize,
}

impl<R: ChunkReader> Iterator for Cursor<R> {
    type Item = Result<Stripe>;

    fn next(&mut self) -> Option<Self::Item> {
        self.file_metadata
            .stripe_metadatas()
            .get(self.stripe_index)
            .map(|info| {
                let stripe = Stripe::new(
                    &mut self.reader,
                    &self.file_metadata,
                    &self.projected_data_type.clone(),
                    info,
                );
                self.stripe_index += 1;
                stripe
            })
    }
}
