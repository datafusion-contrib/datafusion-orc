use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::{InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::byte_rle::ByteRleIter;

pub fn new_i8_iter(column: &Column, stripe: &Stripe) -> Result<NullableIterator<i8>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let iter = stripe
        .stream_map
        .get(column, Kind::Data)
        .map(|reader| {
            Box::new(ByteRleIter::new(reader).map(|value| value.map(|value| value as i8))) as _
        })
        .context(InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
