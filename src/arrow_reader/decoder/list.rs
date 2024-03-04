use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, FieldRef};
use snafu::ResultExt;

use crate::arrow_reader::column::present::new_present_iter;
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
    present: Box<dyn Iterator<Item = bool> + Send>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
    field: FieldRef,
}

impl ListArrayDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
        // TODO: this is to make it Send, fix this?
        let present = Box::new(present.into_iter());

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
    ) -> Result<Option<ArrayRef>> {
        let present = self.present.by_ref().take(batch_size);
        let present = if let Some(parent_present) = parent_present {
            debug_assert_eq!(
                parent_present.len(),
                batch_size,
                "when provided, parent_present length must equal batch_size"
            );
            merge_parent_present(parent_present, present)
        } else {
            present.collect::<Vec<_>>()
        };
        // How many lengths we need to fetch
        let elements_to_fetch = present.iter().filter(|&&is_present| is_present).count();
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
        let child_array = self.inner.next_batch(total_length as usize, None)?.unwrap();
        // Fix the lengths to account for nulls (represented as 0 length)
        let mut lengths_with_nulls = Vec::with_capacity(batch_size);
        let mut lengths = lengths.iter();
        for &is_present in &present {
            if is_present {
                let length = *lengths.next().unwrap();
                lengths_with_nulls.push(length as usize);
            } else {
                lengths_with_nulls.push(0);
            }
        }
        let offsets = OffsetBuffer::from_lengths(lengths_with_nulls);
        let null_buffer = NullBuffer::from(present);

        let array = ListArray::try_new(self.field.clone(), offsets, child_array, Some(null_buffer))
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        if array.is_empty() {
            Ok(None)
        } else {
            Ok(Some(array))
        }
    }
}
