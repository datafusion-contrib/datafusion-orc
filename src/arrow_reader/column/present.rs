use crate::arrow_reader::column::Column;
use crate::arrow_reader::Stripe;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::boolean_rle::BooleanIter;

pub fn new_present_iter(
    column: &Column,
    stripe: &Stripe,
) -> Result<Box<dyn Iterator<Item = Result<bool>>>> {
    // TODO: return None if no present stream
    let rows = column.number_of_rows as usize;
    let iter = stripe
        .stream_map
        .get_opt(column, Kind::Present)
        .map(|reader| Box::new(BooleanIter::new(reader)) as Box<dyn Iterator<Item = Result<bool>>>)
        .unwrap_or_else(|| Box::new(DummyPresentIter::new(rows)));

    Ok(iter)
}

/// Prefetch present stream for entire column in stripe.
///
/// Makes subsequent operations easier to handle.
pub fn get_present_vec(column: &Column, stripe: &Stripe) -> Result<Option<Vec<bool>>> {
    stripe
        .stream_map
        .get_opt(column, Kind::Present)
        .map(|reader| BooleanIter::new(reader).collect::<Result<Vec<_>>>())
        .transpose()
}

pub struct DummyPresentIter {
    index: usize,
    number_of_rows: usize,
}

impl DummyPresentIter {
    pub fn new(rows: usize) -> Self {
        Self {
            index: 0,
            number_of_rows: rows,
        }
    }
}

impl Iterator for DummyPresentIter {
    type Item = Result<bool>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.number_of_rows {
            self.index += 1;
            Some(Ok(true))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DummyPresentIter;

    #[test]
    fn test_dummy_present_iter() {
        let mut iter = DummyPresentIter::new(3);
        assert!(iter.next().unwrap().unwrap());
        assert!(iter.next().unwrap().unwrap());
        assert!(iter.next().unwrap().unwrap());
        assert!(iter.next().is_none());
    }
}
