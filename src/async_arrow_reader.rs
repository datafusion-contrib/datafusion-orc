use std::fmt::Formatter;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, Stream};
use futures_util::FutureExt;

use crate::arrow_reader::{decoder::NaiveStripeDecoder, Cursor};
use crate::error::Result;
use crate::reader::AsyncChunkReader;
use crate::stripe::{Stripe, StripeMetadata};

type BoxedDecoder = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

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

struct StripeFactory<R> {
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
    async fn read_next_stripe_inner(&mut self, info: &StripeMetadata) -> Result<Stripe> {
        let inner = &mut self.inner;

        inner.stripe_index += 1;

        Stripe::new_async(
            &mut inner.reader,
            &inner.file_metadata,
            &inner.projected_data_type,
            info,
        )
        .await
    }

    async fn read_next_stripe(mut self) -> Result<(Self, Option<Stripe>)> {
        let info = self
            .inner
            .file_metadata
            .stripe_metadatas()
            .get(self.inner.stripe_index)
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
}

impl<R: AsyncChunkReader + 'static> ArrowStreamReader<R> {
    pub(crate) fn new(cursor: Cursor<R>, batch_size: usize, schema_ref: SchemaRef) -> Self {
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
