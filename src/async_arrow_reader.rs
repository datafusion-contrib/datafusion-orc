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
use tokio::io::{AsyncRead, AsyncSeek};

use crate::arrow_reader::column::Column;
use crate::arrow_reader::{
    create_arrow_schema, Cursor, NaiveStripeDecoder, Stripe, DEFAULT_BATCH_SIZE,
};
use crate::error::Result;
use crate::proto::StripeInformation;
use crate::reader::schema::TypeDescription;
use crate::reader::Reader;

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

pub struct ArrowStreamReader<R: AsyncRead + AsyncSeek + Unpin + Send> {
    factory: Option<StripeFactory<R>>,
    batch_size: usize,
    schema_ref: SchemaRef,
    state: StreamState<R>,
}

impl<R: AsyncRead + AsyncSeek + Unpin + Send + 'static> StripeFactory<R> {
    pub async fn read_next_stripe_inner(&mut self, info: StripeInformation) -> Result<Stripe> {
        let inner = &mut self.inner;

        let column_defs = inner.columns.clone();
        let stripe_offset = inner.stripe_offset;
        inner.stripe_offset += 1;

        Stripe::new_async(&mut inner.reader, column_defs, stripe_offset, info).await
    }

    pub async fn read_next_stripe(mut self) -> Result<(Self, Option<Stripe>)> {
        let info = self.inner.reader.stripe(self.inner.stripe_offset);

        if let Some(info) = info {
            match self.read_next_stripe_inner(info).await {
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

impl<R: AsyncRead + AsyncSeek + Unpin + Send + 'static> ArrowStreamReader<R> {
    pub fn new(c: Cursor<R>, batch_size: Option<usize>) -> Self {
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let schema = Arc::new(create_arrow_schema(&c));
        Self {
            factory: Some(c.into()),
            batch_size,
            schema_ref: schema,
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
                        self.factory = Some(factory);
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
                        self.factory = Some(factory);
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

impl<R: AsyncRead + AsyncSeek + Unpin + Send + 'static> Stream for ArrowStreamReader<R> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    }
}

impl Stripe {
    pub async fn new_async<R: AsyncRead + AsyncSeek + Unpin + Send>(
        r: &mut Reader<R>,
        column_defs: Arc<Vec<(String, Arc<TypeDescription>)>>,
        stripe: usize,
        info: StripeInformation,
    ) -> Result<Self> {
        let footer = Arc::new(r.stripe_footer(stripe).clone());

        let compression = r.metadata().postscript.compression();
        //TODO(weny): add tz
        let mut columns = Vec::with_capacity(column_defs.len());
        for (name, typ) in column_defs.iter() {
            columns.push(Column::new_async(r, compression, name, typ, &footer, &info).await?);
        }

        Ok(Stripe {
            footer,
            columns,
            stripe_offset: stripe,
            info,
        })
    }
}
