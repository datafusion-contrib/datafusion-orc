use chrono::{Days, NaiveDate};
use snafu::OptionExt;

use super::present::new_present_iter;
use super::{Column, GenericIterator, NullableIterator};
use crate::error::{self, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::rle_v2::SignedRleV2Iter;

pub const UNIX_EPOCH_FROM_CE: i32 = 719_163;

pub struct DateIterator {
    data: Box<dyn Iterator<Item = Result<i64>>>,
}

pub fn convert_date(data: i64) -> Result<NaiveDate> {
    let date = NaiveDate::from_ymd_opt(1970, 1, 1)
        .context(error::InvalidDateSnafu)?
        .checked_add_days(Days::new(data as u64))
        .context(error::AddDaysSnafu)?;

    Ok(date)
}

impl Iterator for DateIterator {
    type Item = Result<NaiveDate>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.data.next() {
            Some(Ok(data)) => Some(convert_date(data)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

pub fn new_date_iter(column: &Column) -> Result<GenericIterator<NaiveDate>> {
    let present = new_present_iter(column)?.try_collect::<Vec<_>>()?;
    let rows: usize = present.iter().filter(|&p| *p).count();

    let data = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| Box::new(SignedRleV2Iter::new(reader, rows, vec![])))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter: Box::new(DateIterator { data }),
    })
}
