use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::{get_rle_reader, NInt};

pub fn new_int_iter<N: NInt>(column: &Column, stripe: &Stripe) -> Result<NullableIterator<N>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let reader = stripe.stream_map.get(column, Kind::Data)?;
    let iter = get_rle_reader::<N, _>(column, reader)?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
