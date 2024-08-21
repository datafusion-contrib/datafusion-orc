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

use std::marker::PhantomData;

use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{
        ArrowPrimitiveType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    },
};
use bytes::Bytes;

use crate::{
    encoding::{byte::ByteRleWriter, float::FloatValueEncoder},
    error::Result,
    reader::decode::{rle_v2::RleWriterV2, SignedEncoding},
    writer::StreamType,
};

use super::{ColumnEncoding, PresentStreamEncoder, Stream};

/// Used to help determine when to finish writing a stripe once a certain
/// size threshold has been reached.
pub trait EstimateMemory {
    /// Approximate current memory usage in bytes.
    fn estimate_memory_size(&self) -> usize;
}

/// Encodes a specific column for a stripe. Will encode to an internal memory
/// buffer until it is finished, in which case it returns the stream bytes to
/// be serialized to a writer.
pub trait ColumnStripeEncoder: EstimateMemory {
    /// Encode entire provided [`ArrayRef`] to internal buffer.
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()>;

    /// Column encoding used for streams.
    fn column_encoding(&self) -> ColumnEncoding;

    /// Emit buffered streams to be written to the writer, and reset state
    /// in preparation for next stripe.
    fn finish(&mut self) -> Vec<Stream>;
}

/// Encodes primitive values into an internal buffer, usually with a specialized run length
/// encoding for better compression.
pub trait PrimitiveValueEncoder<V>: EstimateMemory
where
    V: Copy,
{
    fn new() -> Self;

    fn write_one(&mut self, value: V);

    fn write_slice(&mut self, values: &[V]) {
        for &value in values {
            self.write_one(value);
        }
    }

    /// Take the encoded bytes, replacing it with an empty buffer.
    // TODO: Figure out how to retain the allocation instead of handing
    //       it off each time.
    fn take_inner(&mut self) -> Bytes;
}

// TODO: simplify these generics, probably overcomplicating things here

/// Encoder for primitive ORC types (e.g. int, float). Uses a specific [`PrimitiveValueEncoder`] to
/// encode the primitive values into internal memory. When finished, outputs a DATA stream and
/// optionally a PRESENT stream.
pub struct PrimitiveStripeEncoder<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> {
    encoder: E,
    column_encoding: ColumnEncoding,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<PresentStreamEncoder>,
    encoded_count: usize,
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> PrimitiveStripeEncoder<T, E> {
    // TODO: encode knowledge of the ColumnEncoding as part of the type, instead of requiring it
    //       to be passed at runtime
    pub fn new(column_encoding: ColumnEncoding) -> Self {
        Self {
            encoder: E::new(),
            column_encoding,
            present: None,
            encoded_count: 0,
            _phantom: Default::default(),
        }
    }
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> EstimateMemory
    for PrimitiveStripeEncoder<T, E>
{
    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> ColumnStripeEncoder
    for PrimitiveStripeEncoder<T, E>
{
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        // TODO: return as result instead of panicking here?
        let array = array.as_primitive::<T>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.write_one(v);
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = PresentStreamEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.write_one(v);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, Some(present)) => {
                let values = array.values();
                self.encoder.write_slice(values);
                present.extend_present(array.len());
            }
            (None, None) => {
                let values = array.values();
                self.encoder.write_slice(values);
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        self.column_encoding
    }

    fn finish(&mut self) -> Vec<Stream> {
        let bytes = self.encoder.take_inner();
        // Return mandatory Data stream and optional Present stream
        let data = Stream {
            kind: StreamType::Data,
            bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, present]
            }
            None => vec![data],
        }
    }
}

pub type FloatStripeEncoder = PrimitiveStripeEncoder<Float32Type, FloatValueEncoder<Float32Type>>;
pub type DoubleStripeEncoder = PrimitiveStripeEncoder<Float64Type, FloatValueEncoder<Float64Type>>;
pub type ByteStripeEncoder = PrimitiveStripeEncoder<Int8Type, ByteRleWriter>;
pub type Int16StripeEncoder = PrimitiveStripeEncoder<Int16Type, RleWriterV2<i16, SignedEncoding>>;
pub type Int32StripeEncoder = PrimitiveStripeEncoder<Int32Type, RleWriterV2<i32, SignedEncoding>>;
pub type Int64StripeEncoder = PrimitiveStripeEncoder<Int64Type, RleWriterV2<i64, SignedEncoding>>;
