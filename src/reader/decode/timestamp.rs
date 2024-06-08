use crate::error::{DecodeTimestampSnafu, Result};

const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;

pub struct TimestampIterator {
    base_from_epoch: i64,
    data: Box<dyn Iterator<Item = Result<i64>> + Send>,
    secondary: Box<dyn Iterator<Item = Result<i64>> + Send>,
}

impl TimestampIterator {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn Iterator<Item = Result<i64>> + Send>,
        secondary: Box<dyn Iterator<Item = Result<i64>> + Send>,
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
    nanoseconds: Result<i64>,
) -> Result<Option<i64>> {
    let data = seconds_since_orc_base?;
    // TODO
    let mut nanoseconds = nanoseconds? as u64;
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
    // The timestamp may overflow as ORC encodes them as a pair of (seconds, nanoseconds)
    // while we encode them as a single i64 of nanoseconds in Arrow.
    let nanoseconds_since_epoch = seconds
        .checked_mul(NANOSECONDS_IN_SECOND)
        .and_then(|seconds_in_ns| seconds_in_ns.checked_add(nanoseconds as i64))
        .ok_or(())
        .or_else(|()| {
            DecodeTimestampSnafu {
                seconds,
                nanoseconds,
            }
            .fail()
        })?;
    Ok(Some(nanoseconds_since_epoch))
}
