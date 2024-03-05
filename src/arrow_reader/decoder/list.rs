use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, FieldRef};
use snafu::ResultExt;

use crate::arrow_reader::column::present::get_present_vec;
use crate::arrow_reader::column::Column;
use crate::arrow_reader::decoder::{
    array_decoder_factory, merge_parent_present, ArrayBatchDecoder,
};
use crate::arrow_reader::Stripe;
use crate::proto::stream::Kind;
use crate::reader::decode::get_rle_reader;

use crate::error::{ArrowSnafu, Result};

pub struct ListArrayDecoder {
    inner: Box<dyn ArrayBatchDecoder>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
    field: FieldRef,
}

impl ListArrayDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

        let child = &column.children()[0];
        let inner = array_decoder_factory(child, stripe)?;

        let reader = stripe.stream_map.get(column, Kind::Length)?;
        let lengths = get_rle_reader(column, reader)?;

        let field = Arc::new(Field::from(child));

        Ok(Self {
            inner,
            present,
            lengths,
            field,
        })
    }
}

impl ArrayBatchDecoder for ListArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let present = match (&mut self.present, parent_present) {
            (Some(present), Some(parent_present)) => {
                let present = present.by_ref().take(batch_size);
                Some(merge_parent_present(parent_present, present))
            }
            (Some(present), None) => Some(present.by_ref().take(batch_size).collect::<Vec<_>>()),
            (None, Some(parent_present)) => Some(parent_present.to_vec()),
            (None, None) => None,
        };
        // How many lengths we need to fetch
        let elements_to_fetch = if let Some(present) = &present {
            present.iter().filter(|&&is_present| is_present).count()
        } else {
            batch_size
        };
        let lengths = self
            .lengths
            .by_ref()
            .take(elements_to_fetch)
            .collect::<Result<Vec<_>>>()?;
        debug_assert_eq!(
            lengths.len(),
            elements_to_fetch,
            "less lengths than expected in ListArray"
        );
        let total_length: u64 = lengths.iter().sum();
        // Fetch child array as one Array with total_length elements
        let child_array = self.inner.next_batch(total_length as usize, None)?;
        // Fix the lengths to account for nulls (represented as 0 length)
        let lengths = if let Some(present) = &present {
            let mut lengths_with_nulls = Vec::with_capacity(batch_size);
            let mut lengths = lengths.iter();
            for &is_present in present {
                if is_present {
                    let length = *lengths.next().unwrap();
                    lengths_with_nulls.push(length as usize);
                } else {
                    lengths_with_nulls.push(0);
                }
            }
            lengths_with_nulls
        } else {
            lengths.into_iter().map(|l| l as usize).collect()
        };
        let offsets = OffsetBuffer::from_lengths(lengths);
        let null_buffer = present.map(NullBuffer::from);

        let array = ListArray::try_new(self.field.clone(), offsets, child_array, null_buffer)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}
