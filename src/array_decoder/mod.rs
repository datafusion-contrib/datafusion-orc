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

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::ArrowNativeTypeOp;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::{DataType as ArrowDataType, Field};
use arrow::datatypes::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef,
};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use snafu::{ensure, ResultExt};

use crate::column::Column;
use crate::encoding::boolean::BooleanDecoder;
use crate::encoding::byte::ByteRleDecoder;
use crate::encoding::float::FloatDecoder;
use crate::encoding::integer::get_rle_reader;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{
    self, MismatchedSchemaSnafu, Result, UnexpectedSnafu, UnsupportedTypeVariantSnafu,
};
use crate::proto::stream::Kind;
use crate::schema::DataType;
use crate::stripe::Stripe;

use self::decimal::new_decimal_decoder;
use self::list::ListArrayDecoder;
use self::map::MapArrayDecoder;
use self::string::{new_binary_decoder, new_string_decoder};
use self::struct_decoder::StructArrayDecoder;
use self::timestamp::{new_timestamp_decoder, new_timestamp_instant_decoder};
use self::union::UnionArrayDecoder;

mod decimal;
mod list;
mod map;
mod string;
mod struct_decoder;
mod timestamp;
mod union;

pub trait ArrayBatchDecoder: Send {
    /// Used as base for decoding ORC columns into Arrow arrays. Provide an input `batch_size`
    /// which specifies the upper limit of the number of values returned in the output array.
    ///
    /// If parent nested type (e.g. Struct) indicates a null in it's PRESENT stream,
    /// then the child doesn't have a value (similar to other nullability). So we need
    /// to take care to insert these null values as Arrow requires the child to hold
    /// data in the null slot of the child.
    // TODO: encode nullability in generic -> for a given column in a stripe, we will always know
    //       upfront if we need to bother with nulls or not, so we don't need to keep checking this
    //       for every invocation of next_batch
    // NOTE: null parent may have non-null child, so would still have to account for this
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef>;
}

struct PrimitiveArrayDecoder<T: ArrowPrimitiveType> {
    iter: Box<dyn PrimitiveValueDecoder<T::Native> + Send>,
    present: Option<PresentDecoder>,
}

impl<T: ArrowPrimitiveType> PrimitiveArrayDecoder<T> {
    pub fn new(
        iter: Box<dyn PrimitiveValueDecoder<T::Native> + Send>,
        present: Option<PresentDecoder>,
    ) -> Self {
        Self { iter, present }
    }

    fn next_primitive_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<PrimitiveArray<T>> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;
        let mut data = vec![T::Native::ZERO; batch_size];
        match present {
            Some(present) => {
                self.iter.decode_spaced(data.as_mut_slice(), &present)?;
                let array = PrimitiveArray::<T>::new(data.into(), Some(present));
                Ok(array)
            }
            None => {
                self.iter.decode(data.as_mut_slice())?;
                let array = PrimitiveArray::<T>::from_iter_values(data);
                Ok(array)
            }
        }
    }
}

impl<T: ArrowPrimitiveType> ArrayBatchDecoder for PrimitiveArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self.next_primitive_batch(batch_size, parent_present)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

type Int64ArrayDecoder = PrimitiveArrayDecoder<Int64Type>;
type Int32ArrayDecoder = PrimitiveArrayDecoder<Int32Type>;
type Int16ArrayDecoder = PrimitiveArrayDecoder<Int16Type>;
type Int8ArrayDecoder = PrimitiveArrayDecoder<Int8Type>;
type Float32ArrayDecoder = PrimitiveArrayDecoder<Float32Type>;
type Float64ArrayDecoder = PrimitiveArrayDecoder<Float64Type>;
type DateArrayDecoder = PrimitiveArrayDecoder<Date32Type>; // TODO: does ORC encode as i64 or i32?

struct BooleanArrayDecoder {
    iter: Box<dyn PrimitiveValueDecoder<bool> + Send>,
    present: Option<PresentDecoder>,
}

impl BooleanArrayDecoder {
    pub fn new(
        iter: Box<dyn PrimitiveValueDecoder<bool> + Send>,
        present: Option<PresentDecoder>,
    ) -> Self {
        Self { iter, present }
    }
}

impl ArrayBatchDecoder for BooleanArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;
        let mut data = vec![false; batch_size];
        let array = match present {
            Some(present) => {
                self.iter.decode_spaced(data.as_mut_slice(), &present)?;
                BooleanArray::new(data.into(), Some(present))
            }
            None => {
                self.iter.decode(data.as_mut_slice())?;
                BooleanArray::from(data)
            }
        };
        Ok(Arc::new(array))
    }
}

struct PresentDecoder {
    // TODO: ideally directly reference BooleanDecoder, doing this way to avoid
    //       the generic propagation that would be required (BooleanDecoder<R: Read>)
    inner: Box<dyn PrimitiveValueDecoder<bool> + Send>,
}

impl PresentDecoder {
    fn from_stripe(stripe: &Stripe, column: &Column) -> Option<Self> {
        stripe
            .stream_map()
            .get_opt(column, Kind::Present)
            .map(|stream| {
                let inner = Box::new(BooleanDecoder::new(stream));
                PresentDecoder { inner }
            })
    }

    fn next_buffer(&mut self, size: usize) -> Result<NullBuffer> {
        let mut data = vec![false; size];
        self.inner.decode(&mut data)?;
        Ok(NullBuffer::from(data))
    }
}

fn merge_parent_present(
    parent_present: &NullBuffer,
    present: Result<NullBuffer>,
) -> Result<NullBuffer> {
    let present = present?;
    let non_null_count = parent_present.len() - parent_present.null_count();
    debug_assert!(present.len() == non_null_count);
    let mut builder = BooleanBufferBuilder::new(parent_present.len());
    builder.append_n(parent_present.len(), false);
    for (idx, p) in parent_present.valid_indices().zip(present.iter()) {
        builder.set_bit(idx, p);
    }
    Ok(builder.finish().into())
}

fn derive_present_vec(
    present: &mut Option<PresentDecoder>,
    parent_present: Option<&NullBuffer>,
    batch_size: usize,
) -> Option<Result<NullBuffer>> {
    match (present, parent_present) {
        (Some(present), Some(parent_present)) => {
            let element_count = parent_present.len() - parent_present.null_count();
            let present = present.next_buffer(element_count);
            Some(merge_parent_present(parent_present, present))
        }
        (Some(present), None) => Some(present.next_buffer(batch_size)),
        (None, Some(parent_present)) => Some(Ok(parent_present.clone())),
        (None, None) => None,
    }
}

pub struct NaiveStripeDecoder {
    stripe: Stripe,
    schema_ref: SchemaRef,
    decoders: Vec<Box<dyn ArrayBatchDecoder>>,
    index: usize,
    batch_size: usize,
    number_of_rows: usize,
}

impl Iterator for NaiveStripeDecoder {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.number_of_rows {
            let record = self
                .decode_next_batch(self.number_of_rows - self.index)
                .transpose()?;
            self.index += self.batch_size;
            Some(record)
        } else {
            None
        }
    }
}

pub fn array_decoder_factory(
    column: &Column,
    field: Arc<Field>,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let decoder: Box<dyn ArrayBatchDecoder> = match (column.data_type(), field.data_type()) {
        // TODO: try make branches more generic, reduce duplication
        (DataType::Boolean { .. }, ArrowDataType::Boolean) => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(BooleanDecoder::new(iter));
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(BooleanArrayDecoder::new(iter, present))
        }
        (DataType::Byte { .. }, ArrowDataType::Int8) => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(ByteRleDecoder::new(iter));
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(Int8ArrayDecoder::new(iter, present))
        }
        (DataType::Short { .. }, ArrowDataType::Int16) => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(Int16ArrayDecoder::new(iter, present))
        }
        (DataType::Int { .. }, ArrowDataType::Int32) => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(Int32ArrayDecoder::new(iter, present))
        }
        (DataType::Long { .. }, ArrowDataType::Int64) => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(Int64ArrayDecoder::new(iter, present))
        }
        (DataType::Float { .. }, ArrowDataType::Float32) => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(FloatDecoder::new(iter));
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(Float32ArrayDecoder::new(iter, present))
        }
        (DataType::Double { .. }, ArrowDataType::Float64) => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(FloatDecoder::new(iter));
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(Float64ArrayDecoder::new(iter, present))
        }
        (DataType::String { .. }, ArrowDataType::Utf8)
        | (DataType::Varchar { .. }, ArrowDataType::Utf8)
        | (DataType::Char { .. }, ArrowDataType::Utf8) => new_string_decoder(column, stripe)?,
        (DataType::Binary { .. }, ArrowDataType::Binary) => new_binary_decoder(column, stripe)?,
        (
            DataType::Decimal {
                precision, scale, ..
            },
            ArrowDataType::Decimal128(a_precision, a_scale),
        ) if *precision as u8 == *a_precision && *scale as i8 == *a_scale => {
            new_decimal_decoder(column, stripe, *precision, *scale)?
        }
        (DataType::Timestamp { .. }, field_type) => {
            new_timestamp_decoder(column, field_type.clone(), stripe)?
        }
        (DataType::TimestampWithLocalTimezone { .. }, field_type) => {
            new_timestamp_instant_decoder(column, field_type.clone(), stripe)?
        }
        (DataType::Date { .. }, ArrowDataType::Date32) => {
            // TODO: allow Date64
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = PresentDecoder::from_stripe(stripe, column);
            Box::new(DateArrayDecoder::new(iter, present))
        }
        (DataType::Struct { .. }, ArrowDataType::Struct(fields)) => {
            Box::new(StructArrayDecoder::new(column, fields.clone(), stripe)?)
        }
        (DataType::List { .. }, ArrowDataType::List(field)) => {
            // TODO: add support for ArrowDataType::LargeList
            Box::new(ListArrayDecoder::new(column, field.clone(), stripe)?)
        }
        (DataType::Map { .. }, ArrowDataType::Map(entries, sorted)) => {
            ensure!(!sorted, UnsupportedTypeVariantSnafu { msg: "Sorted map" });
            let ArrowDataType::Struct(entries) = entries.data_type() else {
                UnexpectedSnafu {
                    msg: "arrow Map with non-Struct entry type".to_owned(),
                }
                .fail()?
            };
            ensure!(
                entries.len() == 2,
                UnexpectedSnafu {
                    msg: format!(
                        "arrow Map with {} columns per entry (expected 2)",
                        entries.len()
                    )
                }
            );
            let keys_field = entries[0].clone();
            let values_field = entries[1].clone();

            Box::new(MapArrayDecoder::new(
                column,
                keys_field,
                values_field,
                stripe,
            )?)
        }
        (DataType::Union { .. }, ArrowDataType::Union(fields, _)) => {
            Box::new(UnionArrayDecoder::new(column, fields.clone(), stripe)?)
        }
        (data_type, field_type) => {
            return MismatchedSchemaSnafu {
                orc_type: data_type.clone(),
                arrow_type: field_type.clone(),
            }
            .fail()
        }
    };

    Ok(decoder)
}

impl NaiveStripeDecoder {
    fn inner_decode_next_batch(&mut self, remaining: usize) -> Result<Vec<ArrayRef>> {
        let chunk = self.batch_size.min(remaining);

        let mut fields = Vec::with_capacity(self.stripe.columns().len());

        for decoder in &mut self.decoders {
            let array = decoder.next_batch(chunk, None)?;
            if array.is_empty() {
                break;
            } else {
                fields.push(array);
            }
        }

        Ok(fields)
    }

    fn decode_next_batch(&mut self, remaining: usize) -> Result<Option<RecordBatch>> {
        let fields = self.inner_decode_next_batch(remaining)?;

        if fields.is_empty() {
            if remaining == 0 {
                Ok(None)
            } else {
                // In case of empty projection, we need to create a RecordBatch with `row_count` only
                // to reflect the row number
                Ok(Some(
                    RecordBatch::try_new_with_options(
                        Arc::clone(&self.schema_ref),
                        fields,
                        &RecordBatchOptions::new()
                            .with_row_count(Some(self.batch_size.min(remaining))),
                    )
                    .context(error::ConvertRecordBatchSnafu)?,
                ))
            }
        } else {
            //TODO(weny): any better way?
            let fields = self
                .schema_ref
                .fields
                .into_iter()
                .map(|field| field.name())
                .zip(fields)
                .collect::<Vec<_>>();

            Ok(Some(
                RecordBatch::try_from_iter(fields).context(error::ConvertRecordBatchSnafu)?,
            ))
        }
    }

    pub fn new(stripe: Stripe, schema_ref: SchemaRef, batch_size: usize) -> Result<Self> {
        let mut decoders = Vec::with_capacity(stripe.columns().len());
        let number_of_rows = stripe.number_of_rows();

        for (col, field) in stripe
            .columns()
            .iter()
            .zip(schema_ref.fields.iter().cloned())
        {
            let decoder = array_decoder_factory(col, field, &stripe)?;
            decoders.push(decoder);
        }

        Ok(Self {
            stripe,
            schema_ref,
            decoders,
            index: 0,
            batch_size,
            number_of_rows,
        })
    }
}
