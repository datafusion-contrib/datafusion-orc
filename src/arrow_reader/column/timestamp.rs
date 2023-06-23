use chrono::NaiveDateTime;
use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error::{self, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::rle_v2::RleReaderV2;

// TIMESTAMP_BASE is 1 January 2015, the base value for all timestamp values.
const TIMESTAMP_BASE: i64 = 1420070400;

pub struct TimestampIterator {
    data: Box<dyn Iterator<Item = Result<i64>>>,
    secondary: Box<dyn Iterator<Item = Result<i64>>>,
}

impl Iterator for TimestampIterator {
    type Item = Result<NaiveDateTime>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.data.next() {
            Some(Ok(data)) => match self.secondary.next() {
                Some(Ok(mut nanos)) => {
                    let zeros = nanos & 0x7;
                    nanos >>= 3;
                    if zeros != 0 {
                        for _ in 0..=zeros {
                            nanos *= 10;
                        }
                    }
                    let timestamp =
                        NaiveDateTime::from_timestamp_opt(data + TIMESTAMP_BASE, nanos as u32)
                            .context(error::InvalidTimestampSnafu);

                    Some(timestamp)
                }
                Some(Err(err)) => Some(Err(err)),
                None => None,
            },
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

pub fn new_timestamp_iter(column: &Column) -> Result<NullableIterator<NaiveDateTime>> {
    let present = new_present_iter(column)?.try_collect::<Vec<_>>()?;

    let data = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| Box::new(RleReaderV2::try_new(reader, true, true)))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    let secondary = column
        .stream(Kind::Secondary)
        .transpose()?
        .map(|reader| Box::new(RleReaderV2::try_new(reader, false, true)))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter: Box::new(TimestampIterator { data, secondary }),
    })
}
