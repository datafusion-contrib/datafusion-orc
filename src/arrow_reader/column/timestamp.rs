use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::{get_direct_signed_rle_reader, get_direct_unsigned_rle_reader};

// TIMESTAMP_BASE is 1 January 2015, the base value for all timestamp values.
// This records the number of seconds since 1 January 1970 (epoch) for the base,
// since Arrow uses the epoch as the base instead.
const TIMESTAMP_BASE_SECONDS_SINCE_EPOCH: i64 = 1_420_070_400;
const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;

pub struct TimestampIterator {
    data: Box<dyn Iterator<Item = Result<i64>> + Send>,
    secondary: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl TimestampIterator {
    fn iter_next(&mut self) -> Result<Option<i64>> {
        let next = match (self.data.next(), self.secondary.next()) {
            (Some(seconds_since_orc_base), Some(nanos)) => {
                let data = seconds_since_orc_base?;
                let mut nanos = nanos?;
                // last 3 bits indicate how many trailing zeros were truncated
                let zeros = nanos & 0x7;
                nanos >>= 3;
                // multiply by powers of 10 to get back the trailing zeros
                if zeros != 0 {
                    nanos *= 10_u64.pow(zeros as u32 + 1);
                }
                // convert into nanoseconds since epoch, which Arrow uses as native representation
                // of timestamps
                let nanoseconds_since_epoch = (data + TIMESTAMP_BASE_SECONDS_SINCE_EPOCH)
                    * NANOSECONDS_IN_SECOND
                    + (nanos as i64);
                Some(nanoseconds_since_epoch)
            }
            // TODO: throw error for mismatched stream lengths?
            _ => None,
        };
        Ok(next)
    }
}

impl Iterator for TimestampIterator {
    type Item = Result<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_next().transpose()
    }
}

pub fn new_timestamp_iter(column: &Column, stripe: &Stripe) -> Result<NullableIterator<i64>> {
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
