use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, BooleanBuilder, PrimitiveArray, PrimitiveBuilder};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{ArrowPrimitiveType, Decimal128Type, UInt64Type};
use arrow::datatypes::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef,
    TimestampNanosecondType,
};
use arrow::record_batch::RecordBatch;
use snafu::ResultExt;

use crate::error::{self, ArrowSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::boolean_rle::BooleanIter;
use crate::reader::decode::byte_rle::ByteRleIter;
use crate::reader::decode::float::FloatIter;
use crate::reader::decode::get_rle_reader;
use crate::schema::DataType;
use crate::stripe::Stripe;

use self::decimal::new_decimal_decoder;
use self::list::ListArrayDecoder;
use self::map::MapArrayDecoder;
use self::string::{new_binary_decoder, new_string_decoder};
use self::struct_decoder::StructArrayDecoder;

use super::column::timestamp::TimestampIterator;
use super::column::{get_present_vec, Column};

mod decimal;
mod list;
mod map;
mod string;
mod struct_decoder;

struct PrimitiveArrayDecoder<T: ArrowPrimitiveType> {
    iter: Box<dyn Iterator<Item = Result<T::Native>> + Send>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
}

impl<T: ArrowPrimitiveType> PrimitiveArrayDecoder<T> {
    pub fn new(
        iter: Box<dyn Iterator<Item = Result<T::Native>> + Send>,
        present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    ) -> Self {
        Self { iter, present }
    }

    fn next_primitive_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<PrimitiveArray<T>> {
        let present = derive_present_vec(&mut self.present, parent_present, batch_size);

        match present {
            Some(present) => {
                let mut builder = PrimitiveBuilder::<T>::with_capacity(batch_size);
                for is_present in present {
                    if is_present {
                        // TODO: return as error instead
                        let val = self
                            .iter
                            .next()
                            .transpose()?
                            .expect("array less than expected length");
                        builder.append_value(val);
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(array)
            }
            None => {
                let data = self
                    .iter
                    .by_ref()
                    .take(batch_size)
                    .collect::<Result<Vec<_>>>()?;
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
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let array = self.next_primitive_batch(batch_size, parent_present)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

type UInt64ArrayDecoder = PrimitiveArrayDecoder<UInt64Type>;
type Int64ArrayDecoder = PrimitiveArrayDecoder<Int64Type>;
type Int32ArrayDecoder = PrimitiveArrayDecoder<Int32Type>;
type Int16ArrayDecoder = PrimitiveArrayDecoder<Int16Type>;
type Int8ArrayDecoder = PrimitiveArrayDecoder<Int8Type>;
type Float32ArrayDecoder = PrimitiveArrayDecoder<Float32Type>;
type Float64ArrayDecoder = PrimitiveArrayDecoder<Float64Type>;
type TimestampArrayDecoder = PrimitiveArrayDecoder<TimestampNanosecondType>;
type DateArrayDecoder = PrimitiveArrayDecoder<Date32Type>; // TODO: does ORC encode as i64 or i32?

/// Wrapper around TimestampArrayDecoder to allow specifying the timezone of the output
/// timestamp array
struct TimestampInstantArrayDecoder(TimestampArrayDecoder);

impl TimestampInstantArrayDecoder {
    pub fn new(
        iter: Box<dyn Iterator<Item = Result<i64>> + Send>,
        present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    ) -> Self {
        Self(TimestampArrayDecoder::new(iter, present))
    }
}

impl ArrayBatchDecoder for TimestampInstantArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let array = self
            .0
            .next_primitive_batch(batch_size, parent_present)?
            .with_timezone("UTC");
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

/// Wrapper around PrimitiveArrayDecoder to allow specifying the precision and scale
/// of the output decimal array.
struct DecimalArrayDecoder {
    precision: u8,
    scale: i8,
    inner: PrimitiveArrayDecoder<Decimal128Type>,
}

impl DecimalArrayDecoder {
    pub fn new(
        precision: u8,
        scale: i8,
        iter: Box<dyn Iterator<Item = Result<i128>> + Send>,
        present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    ) -> Self {
        let inner = PrimitiveArrayDecoder::<Decimal128Type>::new(iter, present);
        Self {
            precision,
            scale,
            inner,
        }
    }
}

impl ArrayBatchDecoder for DecimalArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let array = self
            .inner
            .next_primitive_batch(batch_size, parent_present)?
            .with_precision_and_scale(self.precision, self.scale)
            .context(ArrowSnafu)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

struct BooleanArrayDecoder {
    iter: Box<dyn Iterator<Item = Result<bool>> + Send>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
}

impl BooleanArrayDecoder {
    pub fn new(
        iter: Box<dyn Iterator<Item = Result<bool>> + Send>,
        present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    ) -> Self {
        Self { iter, present }
    }
}

impl ArrayBatchDecoder for BooleanArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let present = derive_present_vec(&mut self.present, parent_present, batch_size);

        match present {
            Some(present) => {
                let mut builder = BooleanBuilder::with_capacity(batch_size);
                for is_present in present {
                    if is_present {
                        // TODO: return as error instead
                        let val = self
                            .iter
                            .next()
                            .transpose()?
                            .expect("array less than expected length");
                        builder.append_value(val);
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                let array = Arc::new(array) as ArrayRef;
                Ok(array)
            }
            None => {
                let data = self
                    .iter
                    .by_ref()
                    .take(batch_size)
                    .collect::<Result<Vec<_>>>()?;
                let array = BooleanArray::from(data);
                let array = Arc::new(array) as ArrayRef;
                Ok(array)
            }
        }
    }
}

fn merge_parent_present(
    parent_present: &[bool],
    present: impl IntoIterator<Item = bool> + Send,
) -> Vec<bool> {
    // present must have len <= parent_present
    let mut present = present.into_iter();
    let mut merged_present = Vec::with_capacity(parent_present.len());
    for &is_present in parent_present {
        if is_present {
            let p = present.next().expect("array less than expected length");
            merged_present.push(p);
        } else {
            merged_present.push(false);
        }
    }
    merged_present
}

fn derive_present_vec(
    present: &mut Option<Box<dyn Iterator<Item = bool> + Send>>,
    parent_present: Option<&[bool]>,
    batch_size: usize,
) -> Option<Vec<bool>> {
    match (present, parent_present) {
        (Some(present), Some(parent_present)) => {
            let present = present.by_ref().take(batch_size);
            Some(merge_parent_present(parent_present, present))
        }
        (Some(present), None) => Some(present.by_ref().take(batch_size).collect::<Vec<_>>()),
        (None, Some(parent_present)) => Some(parent_present.to_vec()),
        (None, None) => None,
    }
}

/// Fix the lengths to account for nulls (represented as 0 length)
fn populate_lengths_with_nulls(
    lengths: Vec<u64>,
    batch_size: usize,
    present: &Option<Vec<bool>>,
) -> Vec<usize> {
    if let Some(present) = present {
        let mut lengths_with_nulls = Vec::with_capacity(batch_size);
        let mut lengths = lengths.iter();
        for &is_present in present {
            if is_present {
                let length = *lengths.next().unwrap();
                lengths_with_nulls.push(length as usize);
            } else {
                lengths_with_nulls.push(0);
            }
        }
        lengths_with_nulls
    } else {
        lengths.into_iter().map(|l| l as usize).collect()
    }
}

fn create_null_buffer(present: Option<Vec<bool>>) -> Option<NullBuffer> {
    match present {
        // Edge case where keys of map cannot have a null buffer
        Some(present) if present.iter().all(|&p| p) => None,
        Some(present) => Some(NullBuffer::from(present)),
        None => None,
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
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef>;
}

pub fn array_decoder_factory(
    column: &Column,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let decoder: Box<dyn ArrayBatchDecoder> = match column.data_type() {
        // TODO: try make branches more generic, reduce duplication
        DataType::Boolean { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(BooleanIter::new(iter));
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(BooleanArrayDecoder::new(iter, present))
        }
        DataType::Byte { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(ByteRleIter::new(iter).map(|value| value.map(|value| value as i8)));
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(Int8ArrayDecoder::new(iter, present))
        }
        DataType::Short { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(Int16ArrayDecoder::new(iter, present))
        }
        DataType::Int { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(Int32ArrayDecoder::new(iter, present))
        }
        DataType::Long { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(Int64ArrayDecoder::new(iter, present))
        }
        DataType::Float { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(FloatIter::new(iter, stripe.number_of_rows()));
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(Float32ArrayDecoder::new(iter, present))
        }
        DataType::Double { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = Box::new(FloatIter::new(iter, stripe.number_of_rows()));
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(Float64ArrayDecoder::new(iter, present))
        }
        DataType::String { .. } | DataType::Varchar { .. } | DataType::Char { .. } => {
            new_string_decoder(column, stripe)?
        }
        DataType::Binary { .. } => new_binary_decoder(column, stripe)?,
        DataType::Decimal {
            precision, scale, ..
        } => new_decimal_decoder(column, stripe, *precision, *scale)?,
        DataType::Timestamp { .. } => {
            // TODO: this needs to consider timezone
            // TODO: here
            let data = stripe.stream_map().get(column, Kind::Data);
            let data = get_rle_reader(column, data)?;

            let secondary = stripe.stream_map().get(column, Kind::Secondary);
            let secondary = get_rle_reader(column, secondary)?;

            let iter = Box::new(TimestampIterator::new(data, secondary));
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

            Box::new(TimestampArrayDecoder::new(iter, present))
        }
        // TODO: duplicated with above
        DataType::TimestampWithLocalTimezone { .. } => {
            let data = stripe.stream_map().get(column, Kind::Data);
            let data = get_rle_reader(column, data)?;

            let secondary = stripe.stream_map().get(column, Kind::Secondary);
            let secondary = get_rle_reader(column, secondary)?;

            let iter = Box::new(TimestampIterator::new(data, secondary));
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

            Box::new(TimestampInstantArrayDecoder::new(iter, present))
        }
        DataType::Date { .. } => {
            let iter = stripe.stream_map().get(column, Kind::Data);
            let iter = get_rle_reader(column, iter)?;
            let present = get_present_vec(column, stripe)?
                .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);
            Box::new(DateArrayDecoder::new(iter, present))
        }
        DataType::Struct { .. } => Box::new(StructArrayDecoder::new(column, stripe)?),
        DataType::List { .. } => Box::new(ListArrayDecoder::new(column, stripe)?),
        DataType::Map { .. } => Box::new(MapArrayDecoder::new(column, stripe)?),
        DataType::Union { .. } => todo!(),
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
        let mut decoders = Vec::with_capacity(stripe.columns().len());
        let number_of_rows = stripe.number_of_rows();

        for col in stripe.columns() {
            let decoder = array_decoder_factory(col, &stripe)?;
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
