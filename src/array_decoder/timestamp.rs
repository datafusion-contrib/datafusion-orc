use std::sync::Arc;

use crate::{
    array_decoder::ArrowDataType,
    column::{get_present_vec, Column},
    error::{MismatchedSchemaSnafu, Result},
    proto::stream::Kind,
    reader::decode::{get_rle_reader, timestamp::TimestampIterator},
    stripe::Stripe,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{
    ArrowTimestampType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use chrono::offset::TimeZone;
use chrono_tz::Tz;
use snafu::ensure;

use super::{ArrayBatchDecoder, PrimitiveArrayDecoder};
use crate::error::UnsupportedTypeVariantSnafu;

/// Statically dispatches to the right ArrowTimestampType based on the value of $time_unit
/// to create a $decoder_type with that type as type parameter and $iter/$present as value
/// parameters, then applies $f to it and $tz.
///
/// $f has to be generic so it cannot be a closure.
macro_rules! decoder_for_time_unit {
    ($column: expr, $time_unit:expr, $seconds_since_unix_epoch:expr, $stripe:expr, $tz:expr, $f:expr,) => {{
        let column = $column;
        let stripe = $stripe;
        let data = stripe.stream_map().get(column, Kind::Data);
        let data = get_rle_reader(column, data)?;

        let secondary = stripe.stream_map().get(column, Kind::Secondary);
        let secondary = get_rle_reader(column, secondary)?;

        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

        match $time_unit {
            TimeUnit::Second => {
                let iter = Box::new(TimestampIterator::<TimestampSecondType>::new(
                    $seconds_since_unix_epoch,
                    data,
                    secondary,
                ));
                Ok(Box::new(($f)(
                    PrimitiveArrayDecoder::<TimestampSecondType>::new(iter, present),
                    $tz,
                )))
            }
            TimeUnit::Millisecond => {
                let iter = Box::new(TimestampIterator::<TimestampMillisecondType>::new(
                    $seconds_since_unix_epoch,
                    data,
                    secondary,
                ));
                Ok(Box::new(($f)(
                    PrimitiveArrayDecoder::<TimestampMillisecondType>::new(iter, present),
                    $tz,
                )))
            }
            TimeUnit::Microsecond => {
                let iter = Box::new(TimestampIterator::<TimestampMicrosecondType>::new(
                    $seconds_since_unix_epoch,
                    data,
                    secondary,
                ));
                Ok(Box::new(($f)(
                    PrimitiveArrayDecoder::<TimestampMicrosecondType>::new(iter, present),
                    $tz,
                )))
            }
            TimeUnit::Nanosecond => {
                let iter = Box::new(TimestampIterator::<TimestampNanosecondType>::new(
                    $seconds_since_unix_epoch,
                    data,
                    secondary,
                ));
                Ok(Box::new(($f)(
                    PrimitiveArrayDecoder::<TimestampNanosecondType>::new(iter, present),
                    $tz,
                )))
            }
        }
    }};
}

/// Seconds from ORC epoch of 1 January 2015, which serves as the 0
/// point for all timestamp values, to the UNIX epoch of 1 January 1970.
const ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH: i64 = 1_420_070_400;

/// Decodes a TIMESTAMP column stripe into batches of Timestamp{Nano,Micro,Milli,}secondArrays
/// with no timezone. Will convert timestamps from writer timezone to UTC if a writer timezone
/// is specified for the stripe.
pub fn new_timestamp_decoder(
    column: &Column,
    field_type: ArrowDataType,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let ArrowDataType::Timestamp(time_unit, None) = field_type else {
        MismatchedSchemaSnafu {
            orc_type: column.data_type().clone(),
            arrow_type: field_type,
        }
        .fail()?
    };

    match stripe.writer_tz() {
        Some(writer_tz) => {
            // If writer timezone exists then we must take the ORC epoch according
            // to that timezone, and find seconds since UTC UNIX epoch for the base.
            let seconds_since_unix_epoch = writer_tz
                .with_ymd_and_hms(2015, 1, 1, 0, 0, 0)
                .unwrap()
                .timestamp();

            fn f<T: ArrowTimestampType>(
                decoder: PrimitiveArrayDecoder<T>,
                writer_tz: Tz,
            ) -> TimestampOffsetArrayDecoder<T> {
                TimestampOffsetArrayDecoder {
                    inner: decoder,
                    writer_tz,
                }
            }
            decoder_for_time_unit!(
                column,
                time_unit,
                seconds_since_unix_epoch,
                stripe,
                writer_tz,
                f,
            )
        }
        None => {
            fn f<T: ArrowTimestampType>(
                decoder: PrimitiveArrayDecoder<T>,
                _writer_tz: (),
            ) -> PrimitiveArrayDecoder<T> {
                decoder
            }

            decoder_for_time_unit!(
                column,
                time_unit,
                // No writer timezone, we can assume UTC, so we can use known fixed value
                // for the base offset.
                ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH,
                stripe,
                (),
                f,
            )
        }
    }
}

/// Decodes a TIMESTAMP_INSTANT column stripe into batches of
/// Timestamp{Nano,Micro,Milli,}secondArrays with UTC timezone.
pub fn new_timestamp_instant_decoder(
    column: &Column,
    field_type: ArrowDataType,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let ArrowDataType::Timestamp(time_unit, Some(tz)) = field_type else {
        MismatchedSchemaSnafu {
            orc_type: column.data_type().clone(),
            arrow_type: field_type,
        }
        .fail()?
    };
    ensure!(
        tz.as_ref() == "UTC",
        UnsupportedTypeVariantSnafu {
            msg: "Non-UTC Arrow timestamps"
        }
    );

    fn f<T: ArrowTimestampType>(
        decoder: PrimitiveArrayDecoder<T>,
        _writer_tz: (),
    ) -> TimestampInstantArrayDecoder<T> {
        TimestampInstantArrayDecoder(decoder)
    }

    decoder_for_time_unit!(
        column,
        time_unit,
        // TIMESTAMP_INSTANT is encoded as UTC so we don't check writer timezone in stripe
        ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH,
        stripe,
        (),
        f,
    )
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
        parent_present: Option<&[bool]>,
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
