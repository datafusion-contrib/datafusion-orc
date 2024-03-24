use crate::error::Result;

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
    pub fn new(
        data: Box<dyn Iterator<Item = Result<i64>> + Send>,
        secondary: Box<dyn Iterator<Item = Result<u64>> + Send>,
    ) -> Self {
        Self { data, secondary }
    }
}

impl Iterator for TimestampIterator {
    type Item = Result<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: throw error for mismatched stream lengths?
        let (seconds_since_orc_base, nanoseconds) =
            self.data.by_ref().zip(self.secondary.by_ref()).next()?;
        decode_timestamp(seconds_since_orc_base, nanoseconds).transpose()
    }
}

fn decode_timestamp(
    seconds_since_orc_base: Result<i64>,
    nanoseconds: Result<u64>,
) -> Result<Option<i64>> {
    let data = seconds_since_orc_base?;
    let mut nanoseconds = nanoseconds?;
    // last 3 bits indicate how many trailing zeros were truncated
    let zeros = nanoseconds & 0x7;
    nanoseconds >>= 3;
    // multiply by powers of 10 to get back the trailing zeros
    if zeros != 0 {
        nanoseconds *= 10_u64.pow(zeros as u32 + 1);
    }
    // convert into nanoseconds since epoch, which Arrow uses as native representation
    // of timestamps
    let nanoseconds_since_epoch =
        (data + TIMESTAMP_BASE_SECONDS_SINCE_EPOCH) * NANOSECONDS_IN_SECOND + (nanoseconds as i64);
    Ok(Some(nanoseconds_since_epoch))
}
