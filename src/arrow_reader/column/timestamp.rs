use crate::error::Result;

const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;

pub struct TimestampIterator {
    base_from_epoch: i64,
    data: Box<dyn Iterator<Item = Result<i64>> + Send>,
    secondary: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl TimestampIterator {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn Iterator<Item = Result<i64>> + Send>,
        secondary: Box<dyn Iterator<Item = Result<u64>> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
        }
    }
}

impl Iterator for TimestampIterator {
    type Item = Result<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: throw error for mismatched stream lengths?
        let (seconds_since_orc_base, nanoseconds) =
            self.data.by_ref().zip(self.secondary.by_ref()).next()?;
        decode_timestamp(self.base_from_epoch, seconds_since_orc_base, nanoseconds).transpose()
    }
}

fn decode_timestamp(
    base: i64,
    seconds_since_orc_base: Result<i64>,
    nanoseconds: Result<u64>,
) -> Result<Option<i64>> {
    let data = seconds_since_orc_base?;
    let mut nanoseconds = nanoseconds?;
    // Last 3 bits indicate how many trailing zeros were truncated
    let zeros = nanoseconds & 0x7;
    nanoseconds >>= 3;
    // Multiply by powers of 10 to get back the trailing zeros
    if zeros != 0 {
        nanoseconds *= 10_u64.pow(zeros as u32 + 1);
    }
    let seconds_since_epoch = data + base;
    // Timestamps below the UNIX epoch with nanoseconds > 999_999 need to be
    // adjusted to have 1 second subtracted due to ORC-763:
    // https://issues.apache.org/jira/browse/ORC-763
    let seconds = if seconds_since_epoch < 0 && nanoseconds > 999_999 {
        seconds_since_epoch - 1
    } else {
        seconds_since_epoch
    };
    // Convert into nanoseconds since epoch, which Arrow uses as native representation
    // of timestamps
    let nanoseconds_since_epoch = seconds * NANOSECONDS_IN_SECOND + (nanoseconds as i64);
    Ok(Some(nanoseconds_since_epoch))
}
