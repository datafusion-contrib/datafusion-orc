use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::byte_rle::ByteRleIter;

pub fn new_i8_iter(column: &Column, stripe: &Stripe) -> Result<NullableIterator<i8>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let reader = stripe.stream_map.get(column, Kind::Data)?;
    let iter = Box::new(ByteRleIter::new(reader).map(|value| value.map(|value| value as i8)));

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
