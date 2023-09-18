pub mod column;

use std::collections::HashMap;
use std::io::{Read, Seek};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, DictionaryArray, PrimitiveArray, StringArray,
};
use arrow::datatypes::{
    Date32Type, Int16Type, Int32Type, Int64Type, Schema, SchemaRef, TimestampNanosecondType,
};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use snafu::{OptionExt, ResultExt};

use self::column::Column;
use crate::arrow_reader::column::binary::new_binary_iterator;
use crate::arrow_reader::column::boolean::new_boolean_iter;
use crate::arrow_reader::column::date::{new_date_iter, UNIX_EPOCH_FROM_CE};
use crate::arrow_reader::column::float::{new_f32_iter, new_f64_iter};
use crate::arrow_reader::column::int::new_i64_iter;
use crate::arrow_reader::column::string::StringDecoder;
use crate::arrow_reader::column::timestamp::new_timestamp_iter;
use crate::arrow_reader::column::NullableIterator;
use crate::error::{self, Result};
use crate::proto::{StripeFooter, StripeInformation};
use crate::reader::schema::{create_field, TypeDescription};
use crate::reader::Reader;

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
}

pub fn create_arrow_schema<R>(cursor: &Cursor<R>) -> Schema {
    let metadata = cursor
        .reader
        .metadata()
        .footer
        .metadata
        .iter()
        .map(|kv| {
            (
                kv.name().to_string(),
                String::from_utf8_lossy(kv.value()).to_string(),
            )
        })
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
            Some(stripe) => stripe
                .next()
                .map(|batch| batch.map_err(|err| ArrowError::ExternalError(Box::new(err)))),
            None => match self
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
            },
        }
    }
}

pub enum Decoder {
    Int64(NullableIterator<i64>),
    Int32(NullableIterator<i64>),
    Int16(NullableIterator<i64>),
    Boolean(NullableIterator<bool>),
    Float32(NullableIterator<f32>),
    Float64(NullableIterator<f64>),
    Timestamp(NullableIterator<NaiveDateTime>),
    Date(NullableIterator<NaiveDate>),
    String(StringDecoder),
    Binary(NullableIterator<Vec<u8>>),
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
            let record = self.decode_next_batch().transpose()?;
            self.index += self.batch_size;
            Some(record)
        } else {
            None
        }
    }
}

impl<T> NullableIterator<T> {
    fn collect_chunk(&mut self, chunk: usize) -> Option<Result<Vec<Option<T>>>> {
        let mut buf = Vec::with_capacity(chunk);
        for _ in 0..chunk {
            match self.next() {
                Some(Ok(value)) => {
                    buf.push(value);
                }
                Some(Err(err)) => return Some(Err(err)),
                None => break,
            }
        }

        Some(Ok(buf))
    }
}

macro_rules! impl_decode_next_batch {
    ($name:ident) => {
        paste::item! {
            fn [<decode_next_batch_ $name>](
                decoder: &mut NullableIterator<$name>,
                chunk: usize,
            ) -> Result<Option<ArrayRef>> {
                Ok(match decoder.collect_chunk(chunk).transpose()? {
                    Some(values) => Some(Arc::new(PrimitiveArray::from(values)) as ArrayRef),
                    None => None,
                })
            }
        }
    };
}

macro_rules! impl_decode_next_batch_cast {
    ($target:ident,$tp:ident) => {
        paste::item! {
            fn [<decode_next_batch_ $target>](
                decoder: &mut NullableIterator<i64>,
                chunk: usize,
            ) -> Result<Option<ArrayRef>> {
                Ok(match decoder.collect_chunk(chunk).transpose()? {
                    Some(values) => {
                        let values = values
                            .into_iter()
                            .map(|v| v.map(|v| v as $target))
                            .collect::<Vec<_>>();
                        Some(Arc::new(PrimitiveArray::<$tp>::from(values)) as ArrayRef)
                    }
                    None => None,
                })
            }
        }
    };
}

impl_decode_next_batch_cast!(i64, Int64Type);
impl_decode_next_batch_cast!(i32, Int32Type);
impl_decode_next_batch_cast!(i16, Int16Type);
impl_decode_next_batch!(f32);
impl_decode_next_batch!(f64);

impl NaiveStripeDecoder {
    fn decode_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let chunk = self.batch_size;

        let mut fields = Vec::with_capacity(self.stripe.columns.len());

        for decoder in &mut self.decoders {
            match decoder {
                Decoder::Boolean(decoder) => {
                    match decoder.collect_chunk(chunk).transpose()? {
                        Some(values) => {
                            fields.push(Arc::new(BooleanArray::from(values)) as ArrayRef)
                        }
                        None => break,
                    };
                }
                Decoder::Int64(decoder) => match decode_next_batch_i64(decoder, chunk)? {
                    Some(array) => fields.push(array),
                    None => break,
                },
                Decoder::Int32(decoder) => match decode_next_batch_i32(decoder, chunk)? {
                    Some(array) => fields.push(array),
                    None => break,
                },
                Decoder::Int16(decoder) => match decode_next_batch_i16(decoder, chunk)? {
                    Some(array) => fields.push(array),
                    None => break,
                },
                Decoder::Float32(decoder) => match decode_next_batch_f32(decoder, chunk)? {
                    Some(array) => fields.push(array),
                    None => break,
                },
                Decoder::Float64(decoder) => match decode_next_batch_f64(decoder, chunk)? {
                    Some(array) => fields.push(array),
                    None => break,
                },
                Decoder::Timestamp(decoder) => match decoder.collect_chunk(chunk).transpose()? {
                    Some(values) => {
                        let iter = values
                            .into_iter()
                            .map(|value| value.map(|value| value.timestamp_nanos()));
                        fields.push(
                            Arc::new(PrimitiveArray::<TimestampNanosecondType>::from_iter(iter))
                                as ArrayRef,
                        );
                    }
                    None => break,
                },
                Decoder::Date(decoder) => match decoder.collect_chunk(chunk).transpose()? {
                    Some(values) => {
                        let iter = values.into_iter().map(|value| {
                            value.map(|value| value.num_days_from_ce() - UNIX_EPOCH_FROM_CE)
                        });
                        fields.push(
                            Arc::new(PrimitiveArray::<Date32Type>::from_iter(iter)) as ArrayRef
                        );
                    }
                    None => break,
                },
                Decoder::String(decoder) => match decoder {
                    StringDecoder::Direct(decoder) => {
                        match decoder.collect_chunk(chunk).transpose()? {
                            Some(values) => {
                                fields.push(Arc::new(StringArray::from(values)) as ArrayRef);
                            }
                            None => break,
                        }
                    }
                    StringDecoder::Dictionary((indexes, dictionary)) => {
                        match indexes.collect_chunk(chunk).transpose()? {
                            Some(indexes) => {
                                fields.push(Arc::new(DictionaryArray::<Int64Type>::new(
                                    indexes.into(),
                                    dictionary.clone(),
                                )));
                            }
                            None => break,
                        }
                    }
                },
                Decoder::Binary(binary) => match binary.collect_chunk(chunk).transpose()? {
                    Some(values) => {
                        let ref_vec = values.iter().map(|opt| opt.as_deref()).collect::<Vec<_>>();
                        fields.push(Arc::new(BinaryArray::from_opt_vec(ref_vec)) as ArrayRef);
                    }
                    None => break,
                },
            }
        }

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
            .get(0)
            .map(|c| c.number_of_rows())
            .unwrap_or_default();
        for col in &stripe.columns {
            let decoder = match col.kind() {
                crate::proto::r#type::Kind::Boolean => Decoder::Boolean(new_boolean_iter(col)?),
                crate::proto::r#type::Kind::Byte => todo!(),
                crate::proto::r#type::Kind::Short => Decoder::Int16(new_i64_iter(col)?),
                crate::proto::r#type::Kind::Int => Decoder::Int32(new_i64_iter(col)?),
                crate::proto::r#type::Kind::Long => Decoder::Int64(new_i64_iter(col)?),
                crate::proto::r#type::Kind::Float => Decoder::Float32(new_f32_iter(col)?),
                crate::proto::r#type::Kind::Double => Decoder::Float64(new_f64_iter(col)?),
                crate::proto::r#type::Kind::String => Decoder::String(StringDecoder::new(col)?),
                crate::proto::r#type::Kind::Binary => Decoder::Binary(new_binary_iterator(col)?),
                crate::proto::r#type::Kind::Timestamp => {
                    Decoder::Timestamp(new_timestamp_iter(col)?)
                }
                crate::proto::r#type::Kind::List => todo!(),
                crate::proto::r#type::Kind::Map => todo!(),
                crate::proto::r#type::Kind::Struct => todo!(),
                crate::proto::r#type::Kind::Union => todo!(),
                crate::proto::r#type::Kind::Decimal => todo!(),
                crate::proto::r#type::Kind::Date => Decoder::Date(new_date_iter(col)?),
                crate::proto::r#type::Kind::Varchar => Decoder::String(StringDecoder::new(col)?),
                crate::proto::r#type::Kind::Char => Decoder::String(StringDecoder::new(col)?),
            };
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
                .schema
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
        let root = &r.metadata().footer.types[0];
        let fields = &root.field_names.clone();
        Self::new(r, fields)
    }
}

impl<R: Read + Seek> Iterator for Cursor<R> {
    type Item = Result<Stripe>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(info) = self.reader.stripe(self.stripe_offset) {
            let stripe = Stripe::new(&mut self.reader, &self.columns, self.stripe_offset, info);

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
    pub(crate) info: StripeInformation,
}

impl Stripe {
    pub fn new<R: Read + Seek>(
        r: &mut Reader<R>,
        columns: &[(String, Arc<TypeDescription>)],
        stripe: usize,
        info: StripeInformation,
    ) -> Result<Self> {
        let footer = Arc::new(r.stripe_footer(stripe).clone());

        let compression = r.metadata().postscript.compression();
        //TODO(weny): add tz
        let columns = columns
            .iter()
            .map(|(name, typ)| Column::new(r, compression, name, typ, &footer, &info))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            footer,
            columns,
            stripe_offset: stripe,
            info,
        })
    }

    pub fn footer(&self) -> &Arc<StripeFooter> {
        &self.footer
    }

    pub fn stripe_offset(&self) -> usize {
        self.stripe_offset
    }

    pub fn stripe_info(&self) -> &StripeInformation {
        &self.info
    }
}
