use crate::arrow_reader::column::Column;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::boolean_rle::BooleanIter;

pub fn new_present_iter(column: &Column) -> Result<Box<dyn Iterator<Item = Result<bool>>>> {
    let rows = column.number_of_rows as usize;
    let iter = column
        .stream(Kind::Present)
        .transpose()?
        .map(|reader| {
            Box::new(BooleanIter::new(reader, rows)) as Box<dyn Iterator<Item = Result<bool>>>
        })
        .unwrap_or_else(|| Box::new(DummyPresentIter::new(rows)));

    Ok(iter)
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
