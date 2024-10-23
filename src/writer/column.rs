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
        ArrowPrimitiveType, ByteArrayType, Float32Type, Float64Type, GenericBinaryType,
        GenericStringType, Int16Type, Int32Type, Int64Type, Int8Type,
    },
};
use bytes::{BufMut, BytesMut};

use crate::{
    encoding::{
        boolean::BooleanEncoder,
        byte::ByteRleEncoder,
        float::FloatEncoder,
        integer::{rle_v2::RleV2Encoder, NInt, SignedEncoding, UnsignedEncoding},
        PrimitiveValueEncoder,
    },
    error::Result,
    memory::EstimateMemory,
    writer::StreamType,
};

use super::{ColumnEncoding, Stream};

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

// TODO: simplify these generics, probably overcomplicating things here

/// Encoder for primitive ORC types (e.g. int, float). Uses a specific [`PrimitiveValueEncoder`] to
/// encode the primitive values into internal memory. When finished, outputs a DATA stream and
/// optionally a PRESENT stream.
pub struct PrimitiveColumnEncoder<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> {
    encoder: E,
    column_encoding: ColumnEncoding,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<BooleanEncoder>,
    encoded_count: usize,
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> PrimitiveColumnEncoder<T, E> {
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
    for PrimitiveColumnEncoder<T, E>
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
    for PrimitiveColumnEncoder<T, E>
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
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.write_one(v);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let values = array.values();
                self.encoder.write_slice(values);
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
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

pub struct BooleanColumnEncoder {
    encoder: BooleanEncoder,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl BooleanColumnEncoder {
    pub fn new() -> Self {
        Self {
            encoder: BooleanEncoder::new(),
            present: None,
            encoded_count: 0,
        }
    }
}

impl EstimateMemory for BooleanColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl ColumnStripeEncoder for BooleanColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        // TODO: return as result instead of panicking here?
        let array = array.as_boolean();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.extend_boolean(v);
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.extend_boolean(v);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let values = array.values();
                self.encoder.extend_bb(values);
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::Direct
    }

    fn finish(&mut self) -> Vec<Stream> {
        let bytes = self.encoder.finish();
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

/// Direct encodes binary/strings.
pub struct GenericBinaryColumnEncoder<T: ByteArrayType>
where
    T::Offset: NInt,
{
    string_bytes: BytesMut,
    length_encoder: RleV2Encoder<T::Offset, UnsignedEncoding>,
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl<T: ByteArrayType> GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    pub fn new() -> Self {
        Self {
            string_bytes: BytesMut::new(),
            length_encoder: RleV2Encoder::new(),
            present: None,
            encoded_count: 0,
        }
    }
}

impl<T: ByteArrayType> EstimateMemory for GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    fn estimate_memory_size(&self) -> usize {
        self.string_bytes.len()
            + self.length_encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ByteArrayType> ColumnStripeEncoder for GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }
        // TODO: return as result instead of panicking here?
        let array = array.as_bytes::<T>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    self.length_encoder.write_one(array.value_length(index));
                    self.string_bytes.put_slice(array.value(index).as_ref());
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    self.length_encoder.write_one(array.value_length(index));
                    self.string_bytes.put_slice(array.value(index).as_ref());
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let offsets = array.offsets();
                let first_offset = offsets[0];

                let mut length_to_copy = <T::Offset as num::Zero>::zero();
                let mut prev_offset = first_offset;
                // Derive lengths from offsets then encode them as ints
                for &offset in offsets.iter().skip(1) {
                    let length = offset - prev_offset;
                    self.length_encoder.write_one(length);
                    length_to_copy += length;
                    prev_offset = offset;
                }
                // Copy all string bytes in a single go
                // TODO: this cast to i64 to usize can be cleaned up?
                let first_offset = first_offset.as_i64() as usize;
                let end_offset = first_offset + length_to_copy.as_i64() as usize;
                let string_bytes = &array.value_data()[first_offset..end_offset];
                self.string_bytes.put_slice(string_bytes);

                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        // TODO: throwing away allocations here
        let data_bytes = std::mem::take(&mut self.string_bytes);
        let length_bytes = self.length_encoder.take_inner();
        let data = Stream {
            kind: StreamType::Data,
            bytes: data_bytes.into(),
        };
        let length = Stream {
            kind: StreamType::Length,
            bytes: length_bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, length, present]
            }
            None => vec![data, length],
        }
    }
}

pub type FloatColumnEncoder = PrimitiveColumnEncoder<Float32Type, FloatEncoder<f32>>;
pub type DoubleColumnEncoder = PrimitiveColumnEncoder<Float64Type, FloatEncoder<f64>>;
pub type ByteColumnEncoder = PrimitiveColumnEncoder<Int8Type, ByteRleEncoder>;
pub type Int16ColumnEncoder = PrimitiveColumnEncoder<Int16Type, RleV2Encoder<i16, SignedEncoding>>;
pub type Int32ColumnEncoder = PrimitiveColumnEncoder<Int32Type, RleV2Encoder<i32, SignedEncoding>>;
pub type Int64ColumnEncoder = PrimitiveColumnEncoder<Int64Type, RleV2Encoder<i64, SignedEncoding>>;
pub type StringColumnEncoder = GenericBinaryColumnEncoder<GenericStringType<i32>>;
pub type LargeStringColumnEncoder = GenericBinaryColumnEncoder<GenericStringType<i64>>;
pub type BinaryColumnEncoder = GenericBinaryColumnEncoder<GenericBinaryType<i32>>;
pub type LargeBinaryColumnEncoder = GenericBinaryColumnEncoder<GenericBinaryType<i64>>;
