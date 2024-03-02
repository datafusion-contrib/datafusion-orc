use std::collections::HashMap;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, Stream};
use futures_util::FutureExt;
use snafu::ResultExt;

use crate::arrow_reader::column::Column;
use crate::arrow_reader::{Cursor, NaiveStripeDecoder};
use crate::error::{IoSnafu, Result};
use crate::reader::metadata::FileMetadata;
use crate::reader::AsyncChunkReader;
use crate::schema::RootDataType;
use crate::stripe::{deserialize_stripe_footer, StreamMap, Stripe, StripeMetadata};

pub type BoxedDecoder = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

enum StreamState<T> {
    /// At the start of a new row group, or the end of the file stream
    Init,
    /// Decoding a batch
    Decoding(BoxedDecoder),
    /// Reading data from input
    Reading(BoxFuture<'static, Result<(StripeFactory<T>, Option<Stripe>)>>),
    /// Error
    Error,
}

impl<T> std::fmt::Debug for StreamState<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamState::Init => write!(f, "StreamState::Init"),
            StreamState::Decoding(_) => write!(f, "StreamState::Decoding"),
            StreamState::Reading(_) => write!(f, "StreamState::Reading"),
            StreamState::Error => write!(f, "StreamState::Error"),
        }
    }
}

impl<R: Send> From<Cursor<R>> for StripeFactory<R> {
    fn from(c: Cursor<R>) -> Self {
        Self {
            inner: c,
            is_end: false,
        }
    }
}

pub struct StripeFactory<R> {
    inner: Cursor<R>,
    is_end: bool,
}

pub struct ArrowStreamReader<R: AsyncChunkReader> {
    factory: Option<Box<StripeFactory<R>>>,
    batch_size: usize,
    schema_ref: SchemaRef,
    state: StreamState<R>,
}

impl<R: AsyncChunkReader + 'static> StripeFactory<R> {
    pub async fn read_next_stripe_inner(&mut self, info: &StripeMetadata) -> Result<Stripe> {
        let inner = &mut self.inner;

        let stripe_offset = inner.stripe_offset;
        inner.stripe_offset += 1;

        Stripe::new_async(
            &mut inner.reader,
            &inner.file_metadata,
            &inner.projected_data_type,
            stripe_offset,
            info,
        )
        .await
    }

    pub async fn read_next_stripe(mut self) -> Result<(Self, Option<Stripe>)> {
        let info = self
            .inner
            .file_metadata
            .stripe_metadatas()
            .get(self.inner.stripe_offset)
            .cloned();

        if let Some(info) = info {
            match self.read_next_stripe_inner(&info).await {
                Ok(stripe) => Ok((self, Some(stripe))),
                Err(err) => Err(err),
            }
        } else {
            self.is_end = true;
            Ok((self, None))
        }
    }

    pub async fn is_end(&self) -> bool {
        self.is_end
    }
}

impl<R: AsyncChunkReader + 'static> ArrowStreamReader<R> {
    pub fn new(cursor: Cursor<R>, batch_size: usize, schema_ref: SchemaRef) -> Self {
        Self {
            factory: Some(Box::new(cursor.into())),
            batch_size,
            schema_ref,
            state: StreamState::Init,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }

    fn poll_next_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Decoding(decoder) => match decoder.next() {
                    Some(Ok(batch)) => {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Some(Err(e)) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                    None => self.state = StreamState::Init,
                },
                StreamState::Init => {
                    let factory = self.factory.take().expect("lost factory");
                    if factory.is_end {
                        return Poll::Ready(None);
                    }

                    let fut = factory.read_next_stripe().boxed();

                    self.state = StreamState::Reading(fut)
                }
                StreamState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((factory, Some(stripe))) => {
                        self.factory = Some(Box::new(factory));
                        match NaiveStripeDecoder::new(
                            stripe,
                            self.schema_ref.clone(),
                            self.batch_size,
                        ) {
                            Ok(decoder) => {
                                self.state = StreamState::Decoding(Box::new(decoder));
                            }
                            Err(e) => {
                                self.state = StreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    Ok((factory, None)) => {
                        self.factory = Some(Box::new(factory));
                        // All rows skipped, read next row group
                        self.state = StreamState::Init;
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                StreamState::Error => return Poll::Ready(None), // Ends the stream as error happens.
            }
        }
    }
}

impl<R: AsyncChunkReader + 'static> Stream for ArrowStreamReader<R> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    }
}

impl Stripe {
    // TODO: reduce duplication with sync version in arrow_reader.rs
    pub async fn new_async<R: AsyncChunkReader>(
        reader: &mut R,
        file_metadata: &Arc<FileMetadata>,
        projected_data_type: &RootDataType,
        stripe: usize,
        info: &StripeMetadata,
    ) -> Result<Self> {
        let compression = file_metadata.compression();

        let footer = reader
            .get_bytes(info.footer_offset(), info.footer_length())
            .await
            .context(IoSnafu)?;
        let footer = Arc::new(deserialize_stripe_footer(&footer, compression)?);

        //TODO(weny): add tz
        let columns = projected_data_type
            .children()
            .iter()
            .map(|col| Column::new(col.name(), col.data_type(), &footer, info.number_of_rows()))
            .collect();

        let mut stream_map = HashMap::new();
        let mut stream_offset = info.offset();
        for stream in &footer.streams {
            let length = stream.length();
            let column_id = stream.column();
            let kind = stream.kind();
            let data = Column::read_stream_async(reader, stream_offset, length).await?;

            // TODO(weny): filter out unused streams.
            stream_map.insert((column_id, kind), data);

            stream_offset += length;
        }

        Ok(Stripe {
            footer,
            columns,
            stripe_offset: stripe,
            stream_map: Arc::new(StreamMap {
                inner: stream_map,
                compression,
            }),
        })
    }
}
