pub mod column;

use std::collections::HashMap;
use std::io::{Read, Seek};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder,
    Date32Array, Date32Builder, Float32Array, Float32Builder, Float64Builder, Int16Array,
    Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, Int8Array, Int8Builder,
    ListBuilder, MapBuilder, PrimitiveBuilder, StringArray, StringBuilder, StringDictionaryBuilder,
    StructBuilder, TimestampNanosecondBuilder,
};
use arrow::array::{Float64Array, TimestampNanosecondArray};
use arrow::datatypes::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
    SchemaRef, TimestampNanosecondType, UInt64Type,
};
use arrow::datatypes::{Field, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use bytes::Bytes;
use snafu::{OptionExt, ResultExt};

use self::column::list::{new_list_iter, ListDecoder};
use self::column::map::{new_map_iter, MapDecoder};
use self::column::struct_column::new_struct_iter;
use self::column::tinyint::new_i8_iter;
use self::column::Column;
use crate::arrow_reader::column::binary::new_binary_iterator;
use crate::arrow_reader::column::boolean::new_boolean_iter;
use crate::arrow_reader::column::float::{new_f32_iter, new_f64_iter};
use crate::arrow_reader::column::int::new_i64_iter;
use crate::arrow_reader::column::string::StringDecoder;
use crate::arrow_reader::column::struct_column::StructDecoder;
use crate::arrow_reader::column::timestamp::new_timestamp_iter;
use crate::arrow_reader::column::NullableIterator;
use crate::builder::BoxedArrayBuilder;
use crate::error::{self, InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::proto::StripeFooter;
use crate::reader::decompress::{Compression, Decompressor};
use crate::reader::schema::{create_field, TypeDescription};
use crate::reader::Reader;
use crate::stripe::StripeMetadata;

pub struct ArrowReader<R: Read> {
    cursor: Cursor<R>,
    schema_ref: SchemaRef,
    current_stripe: Option<Box<dyn Iterator<Item = Result<RecordBatch>>>>,
    batch_size: usize,
}

pub const DEFAULT_BATCH_SIZE: usize = 8192;

impl<R: Read> ArrowReader<R> {
    pub fn new(cursor: Cursor<R>, batch_size: Option<usize>) -> Self {
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let schema = Arc::new(create_arrow_schema(&cursor));
        Self {
            cursor,
            schema_ref: schema,
            current_stripe: None,
            batch_size,
        }
    }

    pub fn total_row_count(&self) -> u64 {
        self.cursor.reader.metadata().number_of_rows()
    }
}

impl<R: Read + Seek> ArrowReader<R> {
    fn try_advance_stripe(&mut self) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        match self
            .cursor
            .next()
            .map(|r| r.map_err(|err| ArrowError::ExternalError(Box::new(err))))
        {
            Some(Ok(stripe)) => {
                match NaiveStripeDecoder::new(stripe, self.schema_ref.clone(), self.batch_size)
                    .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                {
                    Ok(decoder) => {
                        self.current_stripe = Some(Box::new(decoder));
                        self.next()
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

pub fn create_arrow_schema<R>(cursor: &Cursor<R>) -> Schema {
    let metadata = cursor
        .reader
        .metadata()
        .user_custom_metadata()
        .iter()
        .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).to_string()))
        .collect::<HashMap<_, _>>();

    let fields = cursor
        .columns
        .iter()
        .map(|(name, typ)| Arc::new(create_field((name, typ))))
        .collect::<Vec<_>>();

    Schema::new_with_metadata(fields, metadata)
}

impl<R: Read + Seek> RecordBatchReader for ArrowReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }
}

impl<R: Read + Seek> Iterator for ArrowReader<R> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_stripe.as_mut() {
            Some(stripe) => {
                match stripe
                    .next()
                    .map(|batch| batch.map_err(|err| ArrowError::ExternalError(Box::new(err))))
                {
                    Some(rb) => Some(rb),
                    None => self.try_advance_stripe(),
                }
            }
            None => self.try_advance_stripe(),
        }
    }
}

pub enum Decoder {
    Int64(NullableIterator<i64>),
    Int32(NullableIterator<i64>),
    Int16(NullableIterator<i64>),
    Int8(NullableIterator<i8>),
    Boolean(NullableIterator<bool>),
    Float32(NullableIterator<f32>),
    Float64(NullableIterator<f64>),
    Timestamp(NullableIterator<i64>),
    Date(NullableIterator<i64>),
    String(StringDecoder),
    Binary(NullableIterator<Vec<u8>>),
    Struct(StructDecoder),
    List(ListDecoder),
    Map(MapDecoder),
}

macro_rules! impl_append_struct_value {
    ($typ:ident) => {
        paste::item! {

            fn [<append_struct_ $typ:lower _ value>](
                idx: usize,
                column: &ArrayRef,
                builder: &mut StructBuilder,
            ) {
                type Array = [<$typ Array>];
                type Builder = [<$typ Builder>];

                let values = column.as_any().downcast_ref::<Array>().unwrap();
                for value in values {
                    builder
                        .field_builder::<Builder>(idx)
                        .unwrap()
                        .append_option(value);
                }
            }
        }
    };
}

impl_append_struct_value!(Boolean);
impl_append_struct_value!(Int8);
impl_append_struct_value!(Int16);
impl_append_struct_value!(Int32);
impl_append_struct_value!(Int64);
impl_append_struct_value!(Float32);
impl_append_struct_value!(Float64);
impl_append_struct_value!(Date32);
impl_append_struct_value!(Binary);
impl_append_struct_value!(TimestampNanosecond);

macro_rules! impl_append_struct_null {
    ($typ:ident) => {
        paste::item! {

            fn [<append_struct_ $typ:lower _ null>](
                idx: usize,
                builder: &mut StructBuilder,
            ) {
                type Builder = [<$typ Builder>];

                builder
                    .field_builder::<Builder>(idx)
                    .unwrap()
                    .append_null();
            }
        }
    };
}

impl_append_struct_null!(Boolean);
impl_append_struct_null!(Int8);
impl_append_struct_null!(Int16);
impl_append_struct_null!(Int32);
impl_append_struct_null!(Int64);
impl_append_struct_null!(Float32);
impl_append_struct_null!(Float64);
impl_append_struct_null!(Date32);
impl_append_struct_null!(Binary);
impl_append_struct_null!(TimestampNanosecond);

pub fn append_struct_value(
    idx: usize,
    column: &ArrayRef,
    builder: &mut StructBuilder,
    decoder: &Decoder,
) -> Result<()> {
    match column.data_type() {
        arrow::datatypes::DataType::Boolean => {
            append_struct_boolean_value(idx, column, builder);
        }
        arrow::datatypes::DataType::Int8 => append_struct_int8_value(idx, column, builder),
        arrow::datatypes::DataType::Int16 => append_struct_int16_value(idx, column, builder),
        arrow::datatypes::DataType::Int32 => append_struct_int32_value(idx, column, builder),
        arrow::datatypes::DataType::Int64 => append_struct_int64_value(idx, column, builder),
        arrow::datatypes::DataType::Float32 => append_struct_float32_value(idx, column, builder),
        arrow::datatypes::DataType::Float64 => append_struct_float64_value(idx, column, builder),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            append_struct_timestampnanosecond_value(idx, column, builder)
        }
        &arrow::datatypes::DataType::Binary => append_struct_binary_value(idx, column, builder),
        arrow::datatypes::DataType::Utf8 => {
            let values = column.as_any().downcast_ref::<StringArray>().unwrap();

            match decoder {
                Decoder::String(decoder) => match decoder {
                    StringDecoder::Direct(_) => {
                        for value in values {
                            builder
                                .field_builder::<StringBuilder>(idx)
                                .unwrap()
                                .append_option(value);
                        }
                    }
                    StringDecoder::Dictionary(_) => {
                        for value in values {
                            builder
                                .field_builder::<StringDictionaryBuilder<UInt64Type>>(idx)
                                .unwrap()
                                .append_option(value);
                        }
                    }
                },
                _ => unreachable!(),
            }
        }
        arrow::datatypes::DataType::Date32 => append_struct_date32_value(idx, column, builder),

        _ => unreachable!(),
    }

    Ok(())
}

pub fn append_struct_null(
    idx: usize,
    field: &Field,
    builder: &mut StructBuilder,
    decoder: &Decoder,
) -> Result<()> {
    match field.data_type() {
        arrow::datatypes::DataType::Boolean => {
            append_struct_boolean_null(idx, builder);
        }
        arrow::datatypes::DataType::Int8 => append_struct_int8_null(idx, builder),
        arrow::datatypes::DataType::Int16 => append_struct_int16_null(idx, builder),
        arrow::datatypes::DataType::Int32 => append_struct_int32_null(idx, builder),
        arrow::datatypes::DataType::Int64 => append_struct_int64_null(idx, builder),
        arrow::datatypes::DataType::Float32 => append_struct_float32_null(idx, builder),
        arrow::datatypes::DataType::Float64 => append_struct_float64_null(idx, builder),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            append_struct_timestampnanosecond_null(idx, builder)
        }
        &arrow::datatypes::DataType::Binary => append_struct_binary_null(idx, builder),
        arrow::datatypes::DataType::Utf8 => match decoder {
            Decoder::String(decoder) => match decoder {
                StringDecoder::Direct(_) => {
                    builder
                        .field_builder::<StringBuilder>(idx)
                        .unwrap()
                        .append_null();
                }
                StringDecoder::Dictionary(_) => {
                    builder
                        .field_builder::<StringDictionaryBuilder<UInt64Type>>(idx)
                        .unwrap()
                        .append_null();
                }
            },
            _ => unreachable!(),
        },
        arrow::datatypes::DataType::Date32 => append_struct_date32_null(idx, builder),

        _ => unreachable!(),
    }

    Ok(())
}

impl Decoder {
    pub fn new_array_builder(&self, capacity: usize) -> Box<dyn ArrayBuilder> {
        match self {
            Decoder::Int64(_) => Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(capacity)),
            Decoder::Int32(_) => Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(capacity)),
            Decoder::Int16(_) => Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(capacity)),
            Decoder::Int8(_) => Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(capacity)),
            Decoder::Boolean(_) => Box::new(BooleanBuilder::with_capacity(capacity)),
            Decoder::Float32(_) => {
                Box::new(PrimitiveBuilder::<Float32Type>::with_capacity(capacity))
            }
            Decoder::Float64(_) => {
                Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(capacity))
            }
            Decoder::Timestamp(_) => Box::new(
                PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(capacity),
            ),
            Decoder::Date(_) => Box::new(PrimitiveBuilder::<Date32Type>::with_capacity(capacity)),
            Decoder::String(decoder) => match decoder {
                StringDecoder::Direct(_) => Box::new(StringBuilder::new()),
                StringDecoder::Dictionary((_, dictionary)) => {
                    // Safety: keys won't overflow
                    let builder = StringDictionaryBuilder::<UInt64Type>::new_with_dictionary(
                        capacity, dictionary,
                    )
                    .unwrap();

                    Box::new(builder)
                }
            },
            Decoder::Binary(_) => Box::new(BinaryBuilder::new()),
            Decoder::Struct(decoder) => decoder.new_builder(capacity),
            Decoder::List(decoder) => {
                let builder = decoder.inner.new_array_builder(capacity);
                Box::new(ListBuilder::new(BoxedArrayBuilder { builder }))
            }
            Decoder::Map(decoder) => {
                let key = BoxedArrayBuilder {
                    builder: decoder.key.new_array_builder(capacity),
                };
                let value = BoxedArrayBuilder {
                    builder: decoder.value.new_array_builder(capacity),
                };
                Box::new(MapBuilder::new(None, key, value))
            }
        }
    }

    // returns true if has more.
    pub fn append_value(
        &mut self,
        builder: &mut Box<dyn ArrayBuilder>,
        chunk: usize,
    ) -> Result<bool> {
        let mut has_more = false;
        match self {
            Decoder::Int64(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;

                if let Some(values) = values {
                    has_more = true;
                    let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();

                    for value in values {
                        builder.append_option(value);
                    }
                }
            }
            Decoder::Int32(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;

                if let Some(values) = values {
                    has_more = true;
                    let builder = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();

                    for value in values {
                        builder.append_option(value.map(|v| v as i32));
                    }
                }
            }
            Decoder::Int16(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;

                if let Some(values) = values {
                    has_more = true;
                    let builder = builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap();

                    for value in values {
                        builder.append_option(value.map(|v| v as i16));
                    }
                }
            }
            Decoder::Int8(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;
                if let Some(values) = values {
                    has_more = true;
                    let builder = builder.as_any_mut().downcast_mut::<Int8Builder>().unwrap();

                    for value in values {
                        builder.append_option(value);
                    }
                }
            }
            Decoder::Boolean(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;
                if let Some(values) = values {
                    has_more = true;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .unwrap();
                    for value in values {
                        builder.append_option(value);
                    }
                }
            }
            Decoder::Float32(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;
                if let Some(values) = values {
                    has_more = true;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .unwrap();

                    for value in values {
                        builder.append_option(value);
                    }
                }
            }
            Decoder::Float64(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;
                if let Some(values) = values {
                    has_more = true;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap();
                    for value in values {
                        builder.append_option(value);
                    }
                }
            }
            Decoder::Timestamp(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;
                if let Some(values) = values {
                    has_more = true;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                        .unwrap();
                    for value in values {
                        builder.append_option(value);
                    }
                }
            }
            Decoder::Date(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;
                if let Some(values) = values {
                    has_more = true;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Date32Builder>()
                        .unwrap();

                    // Dates are just signed integers indicating no. of days since epoch
                    // Same as for Arrow, so no conversion needed
                    for value in values {
                        builder.append_option(value.map(|v| v as i32));
                    }
                }
            }
            Decoder::String(decoder) => match decoder {
                StringDecoder::Direct(iter) => {
                    let values = iter.collect_chunk(chunk).transpose()?;
                    if let Some(values) = values {
                        has_more = true;
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap();
                        for value in values {
                            builder.append_option(value);
                        }
                    }
                }
                StringDecoder::Dictionary((indexes, dictionary)) => {
                    let values = indexes.collect_chunk(chunk).transpose()?;
                    if let Some(indexes) = values {
                        has_more = true;

                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<StringDictionaryBuilder<UInt64Type>>()
                            .unwrap();
                        for index in indexes {
                            builder.append_option(index.map(|idx| dictionary.value(idx as usize)));
                        }
                    }
                }
            },
            Decoder::Binary(iter) => {
                let values = iter.collect_chunk(chunk).transpose()?;
                if let Some(values) = values {
                    has_more = true;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .unwrap();

                    for value in values {
                        builder.append_option(value);
                    }
                }
            }
            Decoder::Struct(iter) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();

                let values = iter.collect_chunk(builder, chunk).transpose()?;

                if let Some(values) = values {
                    has_more = true;

                    for (idx, column) in values.iter().enumerate() {
                        append_struct_value(idx, column, builder, &iter.decoders[idx])?;
                    }
                }
            }
            Decoder::List(iter) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<BoxedArrayBuilder>>()
                    .unwrap();

                has_more = iter.collect_chunk(builder, chunk).transpose()?.is_some();
            }
            Decoder::Map(iter) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<BoxedArrayBuilder, BoxedArrayBuilder>>()
                    .unwrap();

                has_more = iter.collect_chunk(builder, chunk).transpose()?.is_some();
            }
        }

        Ok(has_more)
    }

    pub fn append_null(&self, builder: &mut Box<dyn ArrayBuilder>) -> Result<()> {
        match self {
            Decoder::Int64(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Int32(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Int16(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Int16Builder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Int8(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Int8Builder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Boolean(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Float32(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Float64(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Timestamp(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Date(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Date32Builder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::String(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Binary(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Struct(iter) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();

                builder.append_null();

                for (idx, filed) in iter.fields.iter().enumerate() {
                    append_struct_null(idx, filed, builder, &iter.decoders[idx])?;
                }
            }
            &Decoder::List(_) => {
                builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<BoxedArrayBuilder>>()
                    .unwrap()
                    .append_null();
            }
            Decoder::Map(_) => {
                let _ = builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<BoxedArrayBuilder, BoxedArrayBuilder>>()
                    .unwrap()
                    .append(false);
            }
        }

        Ok(())
    }
}

impl BatchDecoder for Decoder {
    fn next_batch(&mut self, chunk: usize) -> Result<Option<ArrayRef>> {
        let mut builder = self.new_array_builder(chunk);

        let _ = !self.append_value(&mut builder, chunk)?;

        let output = builder.finish();

        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
}

pub struct NaiveStripeDecoder {
    stripe: Stripe,
    schema_ref: SchemaRef,
    decoders: Vec<Decoder>,
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

pub trait BatchDecoder: Send {
    fn next_batch(&mut self, chunk: usize) -> Result<Option<ArrayRef>>;
}

pub fn reader_factory(col: &Column, stripe: &Stripe) -> Result<Decoder> {
    let reader = match col.kind() {
        crate::proto::r#type::Kind::Boolean => Decoder::Boolean(new_boolean_iter(col, stripe)?),
        crate::proto::r#type::Kind::Byte => Decoder::Int8(new_i8_iter(col, stripe)?),
        crate::proto::r#type::Kind::Short => Decoder::Int16(new_i64_iter(col, stripe)?),
        crate::proto::r#type::Kind::Int => Decoder::Int32(new_i64_iter(col, stripe)?),
        crate::proto::r#type::Kind::Long => Decoder::Int64(new_i64_iter(col, stripe)?),
        crate::proto::r#type::Kind::Float => Decoder::Float32(new_f32_iter(col, stripe)?),
        crate::proto::r#type::Kind::Double => Decoder::Float64(new_f64_iter(col, stripe)?),
        crate::proto::r#type::Kind::String => Decoder::String(StringDecoder::new(col, stripe)?),
        crate::proto::r#type::Kind::Binary => Decoder::Binary(new_binary_iterator(col, stripe)?),
        crate::proto::r#type::Kind::Timestamp => {
            Decoder::Timestamp(new_timestamp_iter(col, stripe)?)
        }
        crate::proto::r#type::Kind::List => Decoder::List(new_list_iter(col, stripe)?),
        crate::proto::r#type::Kind::Map => Decoder::Map(new_map_iter(col, stripe)?),
        crate::proto::r#type::Kind::Struct => Decoder::Struct(new_struct_iter(col, stripe)?),
        crate::proto::r#type::Kind::Union => todo!(),
        crate::proto::r#type::Kind::Decimal => todo!(),
        crate::proto::r#type::Kind::Date => Decoder::Date(new_i64_iter(col, stripe)?),
        crate::proto::r#type::Kind::Varchar => Decoder::String(StringDecoder::new(col, stripe)?),
        crate::proto::r#type::Kind::Char => Decoder::String(StringDecoder::new(col, stripe)?),
        crate::proto::r#type::Kind::TimestampInstant => todo!(),
    };

    Ok(reader)
}

impl NaiveStripeDecoder {
    fn inner_decode_next_batch(&mut self, remaining: usize) -> Result<Vec<ArrayRef>> {
        let chunk = self.batch_size.min(remaining);

        let mut fields = Vec::with_capacity(self.stripe.columns.len());

        for decoder in &mut self.decoders {
            match decoder.next_batch(chunk)? {
                Some(array) => fields.push(array),
                None => break,
            }
        }

        Ok(fields)
    }

    fn decode_next_batch(&mut self, remaining: usize) -> Result<Option<RecordBatch>> {
        let fields = self.inner_decode_next_batch(remaining)?;

        if fields.is_empty() {
            Ok(None)
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
        let mut decoders = Vec::with_capacity(stripe.columns.len());
        let number_of_rows = stripe
            .columns
            .first()
            .map(|c| c.number_of_rows())
            .unwrap_or_default();

        for col in &stripe.columns {
            let decoder = reader_factory(col, &stripe)?;
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

pub struct Cursor<R> {
    pub(crate) reader: Reader<R>,
    pub(crate) columns: Arc<Vec<(String, Arc<TypeDescription>)>>,
    pub(crate) stripe_offset: usize,
}

impl<R> Cursor<R> {
    pub fn new<T: AsRef<str>>(r: Reader<R>, fields: &[T]) -> Result<Self> {
        let mut columns = Vec::with_capacity(fields.len());
        for name in fields {
            let field = r
                .metadata()
                .type_description()
                .field(name.as_ref())
                .context(error::FieldNotFoundSnafu {
                    name: name.as_ref(),
                })?;
            columns.push((name.as_ref().to_string(), field));
        }
        Ok(Self {
            reader: r,
            columns: Arc::new(columns),
            stripe_offset: 0,
        })
    }

    pub fn root(r: Reader<R>) -> Result<Self> {
        let columns = r.metadata().type_description().children();
        Ok(Self {
            reader: r,
            columns: Arc::new(columns),
            stripe_offset: 0,
        })
    }
}

impl<R: Read + Seek> Iterator for Cursor<R> {
    type Item = Result<Stripe>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(info) = self.reader.stripe(self.stripe_offset).cloned() {
            let stripe = Stripe::new(&mut self.reader, &self.columns, self.stripe_offset, &info);

            self.stripe_offset += 1;

            Some(stripe)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Stripe {
    pub(crate) footer: Arc<StripeFooter>,
    pub(crate) columns: Vec<Column>,
    pub(crate) stripe_offset: usize,
    /// <(ColumnId, Kind), Bytes>
    pub(crate) stream_map: Arc<StreamMap>,
}

#[derive(Debug)]
pub struct StreamMap {
    pub inner: HashMap<(u32, Kind), Bytes>,
    pub compression: Option<Compression>,
}

impl StreamMap {
    pub fn get(&self, column: &Column, kind: Kind) -> Result<Decompressor> {
        self.get_opt(column, kind).context(InvalidColumnSnafu {
            name: column.name(),
        })
    }

    pub fn get_opt(&self, column: &Column, kind: Kind) -> Option<Decompressor> {
        let column_id = column.column_id();

        self.inner
            .get(&(column_id, kind))
            .cloned()
            .map(|data| Decompressor::new(data, self.compression, vec![]))
    }
}

impl Stripe {
    pub fn new<R: Read + Seek>(
        r: &mut Reader<R>,
        column_defs: &[(String, Arc<TypeDescription>)],
        stripe: usize,
        info: &StripeMetadata,
    ) -> Result<Self> {
        let footer = Arc::new(r.stripe_footer(stripe).clone());

        let compression = r.metadata().compression();
        //TODO(weny): add tz
        let mut columns = Vec::with_capacity(column_defs.len());
        for (name, typ) in column_defs.iter() {
            columns.push(Column::new(name, typ, &footer, info.number_of_rows()));
        }

        let mut stream_map = HashMap::new();
        let mut stream_offset = info.offset();
        for stream in &footer.streams {
            let length = stream.length();
            let column_id = stream.column();
            let kind = stream.kind();
            let data = Column::read_stream(r, stream_offset, length as usize)?;

            // TODO(weny): filter out unused streams.
            stream_map.insert((column_id, kind), data);

            stream_offset += length;
        }

        Ok(Self {
            footer,
            columns,
            stripe_offset: stripe,
            stream_map: Arc::new(StreamMap {
                inner: stream_map,
                compression,
            }),
        })
    }

    pub fn footer(&self) -> &Arc<StripeFooter> {
        &self.footer
    }

    pub fn stripe_offset(&self) -> usize {
        self.stripe_offset
    }
}
