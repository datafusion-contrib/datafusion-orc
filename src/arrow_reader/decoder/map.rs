use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Fields};
use snafu::ResultExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::Column;
use crate::arrow_reader::decoder::{
    array_decoder_factory, merge_parent_present, ArrayBatchDecoder,
};
use crate::arrow_reader::Stripe;
use crate::error::{ArrowSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::get_rle_reader;

pub struct MapArrayDecoder {
    keys: Box<dyn ArrayBatchDecoder>,
    values: Box<dyn ArrayBatchDecoder>,
    present: Box<dyn Iterator<Item = bool> + Send>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
    fields: Fields,
}

impl MapArrayDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
        // TODO: this is to make it Send, fix this?
        let present = Box::new(present.into_iter());

        let keys_column = &column.children()[0];
        let keys = array_decoder_factory(keys_column, stripe)?;

        let values_column = &column.children()[1];
        let values = array_decoder_factory(values_column, stripe)?;

        let reader = stripe.stream_map.get(column, Kind::Length)?;
        let lengths = get_rle_reader(column, reader)?;

        let keys_field = Field::new("keys", keys_column.data_type().to_arrow_data_type(), false);
        let keys_field = Arc::new(keys_field);
        let values_field = Field::new(
            "values",
            values_column.data_type().to_arrow_data_type(),
            true,
        );
        let values_field = Arc::new(values_field);

        let fields = Fields::from(vec![keys_field, values_field]);

        Ok(Self {
            keys,
            values,
            present,
            lengths,
            fields,
        })
    }
}

impl ArrayBatchDecoder for MapArrayDecoder {
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
            "less lengths than expected in MapArray"
        );
        let total_length: u64 = lengths.iter().sum();
        // Fetch key and value arrays, each with total_length elements
        // Fetch child array as one Array with total_length elements
        let keys_array = self.keys.next_batch(total_length as usize, None)?.unwrap();
        let values_array = self
            .values
            .next_batch(total_length as usize, None)?
            .unwrap();
        // Compose the keys + values array into a StructArray with two entries
        let entries =
            StructArray::try_new(self.fields.clone(), vec![keys_array, values_array], None)
                .context(ArrowSnafu)?;
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

        let field = Arc::new(Field::new_struct("entries", self.fields.clone(), false));
        let array = MapArray::try_new(field, offsets, entries, Some(null_buffer), false)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        if array.is_empty() {
            Ok(None)
        } else {
            Ok(Some(array))
        }
    }
}
