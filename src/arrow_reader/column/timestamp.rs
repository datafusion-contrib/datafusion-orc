use chrono::NaiveDateTime;
use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::{InvalidTimestampSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::{get_direct_signed_rle_reader, get_direct_unsigned_rle_reader};

// TIMESTAMP_BASE is 1 January 2015, the base value for all timestamp values.
const TIMESTAMP_BASE: i64 = 1420070400;

pub struct TimestampIterator {
    data: Box<dyn Iterator<Item = Result<i64>> + Send>,
    secondary: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl TimestampIterator {
    fn iter_next(&mut self) -> Result<Option<NaiveDateTime>> {
        let next = match (self.data.next(), self.secondary.next()) {
            (Some(data), Some(nanos)) => {
                let data = data?;
                let mut nanos = nanos?;
                let zeros = nanos & 0x7;
                nanos >>= 3;
                if zeros != 0 {
                    nanos *= 10_u64.pow(zeros as u32 + 1);
                }
                let timestamp =
                    NaiveDateTime::from_timestamp_opt(data + TIMESTAMP_BASE, nanos as u32)
                        .context(InvalidTimestampSnafu)?;

                Some(timestamp)
            }
            // TODO: throw error for mismatched stream lengths?
            _ => None,
        };
        Ok(next)
    }
}

impl Iterator for TimestampIterator {
    type Item = Result<NaiveDateTime>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_next().transpose()
    }
}

pub fn new_timestamp_iter(
    column: &Column,
    stripe: &Stripe,
) -> Result<NullableIterator<NaiveDateTime>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let reader = stripe.stream_map.get(column, Kind::Data)?;
    let data = get_direct_signed_rle_reader(column, reader)?;

    let reader = stripe.stream_map.get(column, Kind::Secondary)?;
    let secondary = get_direct_unsigned_rle_reader(column, reader)?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter: Box::new(TimestampIterator { data, secondary }),
    })
}
