use std::sync::Arc;

use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Fields};
use snafu::ResultExt;

use crate::arrow_reader::column::{get_present_vec, Column};
use crate::arrow_reader::decoder::{
    array_decoder_factory, derive_present_vec, populate_lengths_with_nulls, ArrayBatchDecoder,
};
use crate::arrow_reader::Stripe;
use crate::error::{ArrowSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::get_rle_reader;

pub struct MapArrayDecoder {
    keys: Box<dyn ArrayBatchDecoder>,
    values: Box<dyn ArrayBatchDecoder>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
    fields: Fields,
}

impl MapArrayDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

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
    ) -> Result<ArrayRef> {
        let present = derive_present_vec(&mut self.present, parent_present, batch_size);

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
            "less lengths than expected in MapArray"
        );
        let total_length: u64 = lengths.iter().sum();
        // Fetch key and value arrays, each with total_length elements
        // Fetch child array as one Array with total_length elements
        let keys_array = self.keys.next_batch(total_length as usize, None)?;
        let values_array = self.values.next_batch(total_length as usize, None)?;
        // Compose the keys + values array into a StructArray with two entries
        let entries =
            StructArray::try_new(self.fields.clone(), vec![keys_array, values_array], None)
                .context(ArrowSnafu)?;
        let lengths = populate_lengths_with_nulls(lengths, batch_size, &present);
        let offsets = OffsetBuffer::from_lengths(lengths);
        let null_buffer = present.map(NullBuffer::from);

        let field = Arc::new(Field::new_struct("entries", self.fields.clone(), false));
        let array =
            MapArray::try_new(field, offsets, entries, null_buffer, false).context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}
