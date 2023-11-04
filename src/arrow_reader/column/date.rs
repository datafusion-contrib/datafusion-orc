use chrono::{Days, NaiveDate};
use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error::{self, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::rle_v2::RleReaderV2;

pub const UNIX_EPOCH_FROM_CE: i32 = 719_163;

pub struct DateIterator {
    data: Box<dyn Iterator<Item = Result<i64>> + Send>,
}

pub fn convert_date(data: i64) -> Result<NaiveDate> {
    let days = Days::new(data.unsigned_abs());
    // safe unwrap as is valid date
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = if data.is_negative() {
        epoch.checked_sub_days(days)
    } else {
        epoch.checked_add_days(days)
    };
    date.context(error::AddDaysSnafu)
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

pub fn new_date_iter(column: &Column) -> Result<NullableIterator<NaiveDate>> {
    let present = new_present_iter(column)?.collect::<Result<Vec<_>>>()?;

    let data = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| Box::new(RleReaderV2::try_new(reader, true, true)))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter: Box::new(DateIterator { data }),
    })
}
