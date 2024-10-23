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

use crate::{
    array_decoder::ArrowDataType,
    column::Column,
    encoding::{
        integer::{get_rle_reader, get_unsigned_rle_reader},
        timestamp::{TimestampDecoder, TimestampNanosecondAsDecimalDecoder},
        PrimitiveValueDecoder,
    },
    error::{MismatchedSchemaSnafu, Result},
    proto::stream::Kind,
    stripe::Stripe,
};
use arrow::datatypes::{
    ArrowTimestampType, Decimal128Type, DecimalType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::{array::ArrayRef, buffer::NullBuffer};
use chrono::offset::TimeZone;
use chrono::TimeDelta;
use chrono_tz::{Tz, UTC};

use super::{
    decimal::DecimalArrayDecoder, ArrayBatchDecoder, PresentDecoder, PrimitiveArrayDecoder,
};
use crate::error::UnsupportedTypeVariantSnafu;

const NANOSECONDS_IN_SECOND: i128 = 1_000_000_000;
const NANOSECOND_DIGITS: i8 = 9;

/// Seconds from ORC epoch of 1 January 2015, which serves as the 0
/// point for all timestamp values, to the UNIX epoch of 1 January 1970.
const ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH: i64 = 1_420_070_400;

fn get_inner_timestamp_decoder<T: ArrowTimestampType + Send>(
    column: &Column,
    stripe: &Stripe,
    seconds_since_unix_epoch: i64,
) -> Result<PrimitiveArrayDecoder<T>> {
    let data = stripe.stream_map().get(column, Kind::Data);
    let data = get_rle_reader(column, data)?;

    let secondary = stripe.stream_map().get(column, Kind::Secondary);
    let secondary = get_unsigned_rle_reader(column, secondary);

    let present = PresentDecoder::from_stripe(stripe, column);

    let iter = Box::new(TimestampDecoder::<T>::new(
        seconds_since_unix_epoch,
        data,
        secondary,
    ));
    Ok(PrimitiveArrayDecoder::<T>::new(iter, present))
}

fn get_timestamp_decoder<T: ArrowTimestampType + Send>(
    column: &Column,
    stripe: &Stripe,
    seconds_since_unix_epoch: i64,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let inner = get_inner_timestamp_decoder::<T>(column, stripe, seconds_since_unix_epoch)?;
    match stripe.writer_tz() {
        Some(writer_tz) => Ok(Box::new(TimestampOffsetArrayDecoder { inner, writer_tz })),
        None => Ok(Box::new(inner)),
    }
}

fn get_timestamp_instant_decoder<T: ArrowTimestampType + Send>(
    column: &Column,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    // TIMESTAMP_INSTANT is encoded as UTC so we don't check writer timezone in stripe
    let inner =
        get_inner_timestamp_decoder::<T>(column, stripe, ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH)?;
    Ok(Box::new(TimestampInstantArrayDecoder(inner)))
}

fn decimal128_decoder(
    column: &Column,
    stripe: &Stripe,
    seconds_since_unix_epoch: i64,
    writer_tz: Option<Tz>,
) -> Result<DecimalArrayDecoder> {
    let data = stripe.stream_map().get(column, Kind::Data);
    let data = get_rle_reader(column, data)?;

    let secondary = stripe.stream_map().get(column, Kind::Secondary);
    let secondary = get_rle_reader(column, secondary)?;

    let present = PresentDecoder::from_stripe(stripe, column);

    let iter = TimestampNanosecondAsDecimalDecoder::new(seconds_since_unix_epoch, data, secondary);

    let iter: Box<dyn PrimitiveValueDecoder<i128> + Send> = match writer_tz {
        Some(UTC) | None => Box::new(iter),
        Some(writer_tz) => Box::new(TimestampNanosecondAsDecimalWithTzDecoder(iter, writer_tz)),
    };

    Ok(DecimalArrayDecoder::new(
        Decimal128Type::MAX_PRECISION,
        NANOSECOND_DIGITS,
        iter,
        present,
    ))
}

/// Decodes a TIMESTAMP column stripe into batches of Timestamp{Nano,Micro,Milli,}secondArrays
/// with no timezone. Will convert timestamps from writer timezone to UTC if a writer timezone
/// is specified for the stripe.
pub fn new_timestamp_decoder(
    column: &Column,
    field_type: ArrowDataType,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let seconds_since_unix_epoch = match stripe.writer_tz() {
        Some(writer_tz) => {
            // If writer timezone exists then we must take the ORC epoch according
            // to that timezone, and find seconds since UTC UNIX epoch for the base.
            writer_tz
                .with_ymd_and_hms(2015, 1, 1, 0, 0, 0)
                .unwrap()
                .timestamp()
        }
        None => {
            // No writer timezone, we can assume UTC, so we can use known fixed value
            // for the base offset.
            ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH
        }
    };

    match field_type {
        ArrowDataType::Timestamp(TimeUnit::Second, None) => {
            get_timestamp_decoder::<TimestampSecondType>(column, stripe, seconds_since_unix_epoch)
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
            get_timestamp_decoder::<TimestampMillisecondType>(
                column,
                stripe,
                seconds_since_unix_epoch,
            )
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
            get_timestamp_decoder::<TimestampMicrosecondType>(
                column,
                stripe,
                seconds_since_unix_epoch,
            )
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => {
            get_timestamp_decoder::<TimestampNanosecondType>(
                column,
                stripe,
                seconds_since_unix_epoch,
            )
        }
        ArrowDataType::Decimal128(Decimal128Type::MAX_PRECISION, NANOSECOND_DIGITS) => {
            Ok(Box::new(decimal128_decoder(
                column,
                stripe,
                seconds_since_unix_epoch,
                stripe.writer_tz(),
            )?))
        }
        _ => MismatchedSchemaSnafu {
            orc_type: column.data_type().clone(),
            arrow_type: field_type,
        }
        .fail(),
    }
}

/// Decodes a TIMESTAMP_INSTANT column stripe into batches of
/// Timestamp{Nano,Micro,Milli,}secondArrays with UTC timezone.
pub fn new_timestamp_instant_decoder(
    column: &Column,
    field_type: ArrowDataType,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    match field_type {
        ArrowDataType::Timestamp(TimeUnit::Second, Some(tz)) if tz.as_ref() == "UTC" => {
            get_timestamp_instant_decoder::<TimestampSecondType>(column, stripe)
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
            get_timestamp_instant_decoder::<TimestampMillisecondType>(column, stripe)
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            get_timestamp_instant_decoder::<TimestampMicrosecondType>(column, stripe)
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) if tz.as_ref() == "UTC" => {
            get_timestamp_instant_decoder::<TimestampNanosecondType>(column, stripe)
        }
        ArrowDataType::Timestamp(_, Some(_)) => UnsupportedTypeVariantSnafu {
            msg: "Non-UTC Arrow timestamps",
        }
        .fail(),
        ArrowDataType::Decimal128(Decimal128Type::MAX_PRECISION, NANOSECOND_DIGITS) => {
            Ok(Box::new(decimal128_decoder(
                column,
                stripe,
                ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH,
                None,
            )?))
        }
        _ => MismatchedSchemaSnafu {
            orc_type: column.data_type().clone(),
            arrow_type: field_type,
        }
        .fail()?,
    }
}

/// Wrapper around PrimitiveArrayDecoder to decode timestamps which are encoded in
/// timezone of the writer to their UTC value.
struct TimestampOffsetArrayDecoder<T: ArrowTimestampType> {
    inner: PrimitiveArrayDecoder<T>,
    writer_tz: chrono_tz::Tz,
}

impl<T: ArrowTimestampType> ArrayBatchDecoder for TimestampOffsetArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self
            .inner
            .next_primitive_batch(batch_size, parent_present)?;

        let convert_timezone = |ts| {
            // Convert from writer timezone to reader timezone (which we default to UTC)
            // TODO: more efficient way of doing this?
            self.writer_tz
                .timestamp_nanos(ts)
                .naive_local()
                .and_utc()
                .timestamp_nanos_opt()
        };
        let array = array
            // first try to convert all non-nullable batches to non-nullable batches
            .try_unary::<_, T, _>(|ts| convert_timezone(ts).ok_or(()))
            // in the rare case one of the values was out of the timeunit's range (eg. see
            // <https://docs.rs/chrono/latest/chrono/struct.DateTime.html#method.timestamp_nanos_opt>),
            // for nanoseconds), try again by allowing a nullable batch as output
            .unwrap_or_else(|()| array.unary_opt::<_, T>(convert_timezone));
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

/// Wrapper around PrimitiveArrayDecoder to allow specifying the timezone of the output
/// timestamp array as UTC.
struct TimestampInstantArrayDecoder<T: ArrowTimestampType>(PrimitiveArrayDecoder<T>);

impl<T: ArrowTimestampType> ArrayBatchDecoder for TimestampInstantArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self
            .0
            .next_primitive_batch(batch_size, parent_present)?
            .with_timezone("UTC");
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

struct TimestampNanosecondAsDecimalWithTzDecoder(TimestampNanosecondAsDecimalDecoder, Tz);

impl TimestampNanosecondAsDecimalWithTzDecoder {
    fn next_inner(&self, ts: i128) -> i128 {
        let seconds = ts.div_euclid(NANOSECONDS_IN_SECOND);
        let nanoseconds = ts.rem_euclid(NANOSECONDS_IN_SECOND);

        // The addition may panic, because chrono stores dates in an i32,
        // which can be overflowed with an i64 of seconds.
        let dt = (self.1.timestamp_nanos(0)
            + TimeDelta::new(seconds as i64, nanoseconds as u32)
                .expect("TimeDelta duration out of bound"))
        .naive_local()
        .and_utc();

        (dt.timestamp() as i128) * NANOSECONDS_IN_SECOND + (dt.timestamp_subsec_nanos() as i128)
    }
}

impl PrimitiveValueDecoder<i128> for TimestampNanosecondAsDecimalWithTzDecoder {
    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        self.0.decode(out)?;
        for x in out.iter_mut() {
            *x = self.next_inner(*x);
        }
        Ok(())
    }
}
