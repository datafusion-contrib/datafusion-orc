use std::sync::Arc;

use crate::{
    column::{get_present_vec, Column},
    error::Result,
    proto::stream::Kind,
    reader::decode::{get_rle_reader, get_unsigned_rle_reader, timestamp::TimestampIterator},
    stripe::Stripe,
};
use arrow::{array::ArrayRef, datatypes::TimestampNanosecondType};
use chrono::offset::TimeZone;

use super::{ArrayBatchDecoder, PrimitiveArrayDecoder};

/// Seconds from ORC epoch of 1 January 2015, which serves as the 0
/// point for all timestamp values, to the UNIX epoch of 1 January 1970.
const ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH: i64 = 1_420_070_400;

/// Decodes a TIMESTAMP column stripe into batches of TimestampNanosecondArrays with no
/// timezone. Will convert timestamps from writer timezone to UTC if a writer timezone
/// is specified for the stripe.
pub fn new_timestamp_decoder(
    column: &Column,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let data = stripe.stream_map().get(column, Kind::Data);
    let data = get_rle_reader(column, data)?;

    let secondary = stripe.stream_map().get(column, Kind::Secondary);
    let secondary = get_unsigned_rle_reader(column, secondary);

    let present = get_present_vec(column, stripe)?
        .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

    match stripe.writer_tz() {
        Some(tz) => {
            // If writer timezone exists then we must take the ORC epoch according
            // to that timezone, and find seconds since UTC UNIX epoch for the base.
            let seconds_since_unix_epoch = tz
                .with_ymd_and_hms(2015, 1, 1, 0, 0, 0)
                .unwrap()
                .timestamp();
            let iter = Box::new(TimestampIterator::new(
                seconds_since_unix_epoch,
                data,
                secondary,
            ));
            let decoder = RawTimestampArrayDecoder::new(iter, present);
            Ok(Box::new(TimestampOffsetArrayDecoder {
                inner: decoder,
                writer_tz: tz,
            }))
        }
        None => {
            // No writer timezone, we can assume UTC, so we casn use known fixed value
            // for the base offset.
            let iter = Box::new(TimestampIterator::new(
                ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH,
                data,
                secondary,
            ));
            let decoder = RawTimestampArrayDecoder::new(iter, present);
            Ok(Box::new(decoder))
        }
    }
}

/// Decodes a TIMESTAMP_INSTANT column stripe into batches of TimestampNanosecondArrays with
/// UTC timezone.
pub fn new_timestamp_instant_decoder(
    column: &Column,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let data = stripe.stream_map().get(column, Kind::Data);
    let data = get_rle_reader(column, data)?;

    let secondary = stripe.stream_map().get(column, Kind::Secondary);
    let secondary = get_unsigned_rle_reader(column, secondary);

    let present = get_present_vec(column, stripe)?
        .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

    // TIMESTAMP_INSTANT is encoded as UTC so we don't check writer timezone in stripe
    let iter = Box::new(TimestampIterator::new(
        ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH,
        data,
        secondary,
    ));

    let decoder = RawTimestampArrayDecoder::new(iter, present);
    Ok(Box::new(TimestampInstantArrayDecoder(decoder)))
}

type RawTimestampArrayDecoder = PrimitiveArrayDecoder<TimestampNanosecondType>;

/// Wrapper around RawTimestampArrayDecoder to decode timestamps which are encoded in
/// timezone of the writer to their UTC value.
struct TimestampOffsetArrayDecoder {
    inner: RawTimestampArrayDecoder,
    writer_tz: chrono_tz::Tz,
}

impl ArrayBatchDecoder for TimestampOffsetArrayDecoder {
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
            .try_unary::<_, TimestampNanosecondType, _>(|ts| convert_timezone(ts).ok_or(()))
            // in the rare case one of the values was out of the 1677-2262 range (see
            // <https://docs.rs/chrono/latest/chrono/struct.DateTime.html#method.timestamp_nanos_opt>),
            // try again by allowing a nullable batch as output
            .unwrap_or_else(|()| array.unary_opt::<_, TimestampNanosecondType>(convert_timezone));
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

/// Wrapper around RawTimestampArrayDecoder to allow specifying the timezone of the output
/// timestamp array as UTC.
struct TimestampInstantArrayDecoder(RawTimestampArrayDecoder);

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
