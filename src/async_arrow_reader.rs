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

use crate::array_decoder::NaiveStripeDecoder;
use crate::arrow_reader::Cursor;
use crate::error::Result;
use crate::reader::metadata::read_metadata_async;
use crate::reader::AsyncChunkReader;
use crate::stripe::{Stripe, StripeMetadata};
use crate::ArrowReaderBuilder;

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
            if let Some(range) = self.inner.file_byte_range.clone() {
                let offset = info.offset() as usize;
                if !range.contains(&offset) {
                    self.inner.stripe_index += 1;
                    return Ok((self, None));
                }
            }
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
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    }
}

impl<R: AsyncChunkReader + 'static> ArrowReaderBuilder<R> {
    pub async fn try_new_async(mut reader: R) -> Result<Self> {
        let file_metadata = Arc::new(read_metadata_async(&mut reader).await?);
        Ok(Self::new(reader, file_metadata))
    }

    pub fn build_async(self) -> ArrowStreamReader<R> {
        let projected_data_type = self
            .file_metadata()
            .root_data_type()
            .project(&self.projection);
        let schema_ref = self.schema();
        let cursor = Cursor {
            reader: self.reader,
            file_metadata: self.file_metadata,
            projected_data_type,
            stripe_index: 0,
            file_byte_range: self.file_byte_range,
        };
        ArrowStreamReader::new(cursor, self.batch_size, schema_ref)
    }
}
