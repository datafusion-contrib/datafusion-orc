use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::{InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::get_direct_signed_rle_reader;

pub fn new_i64_iter(column: &Column, stripe: &Stripe) -> Result<NullableIterator<i64>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let iter = stripe
        .stream_map
        .get(column, Kind::Data)
        .map(|reader| get_direct_signed_rle_reader(column, reader))
        .context(InvalidColumnSnafu { name: &column.name })??;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
