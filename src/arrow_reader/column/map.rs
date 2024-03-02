use arrow::array::MapBuilder;
use snafu::ResultExt;

use super::present::new_present_iter;
use super::Column;
use crate::arrow_reader::decoder::{reader_factory, Decoder};
use crate::arrow_reader::{BoxedArrayBuilder, Stripe};
use crate::error::{self, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::get_rle_reader;

pub struct MapDecoder {
    pub(crate) key: Box<Decoder>,
    pub(crate) value: Box<Decoder>,
    present: Box<dyn Iterator<Item = bool> + Send>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl MapDecoder {
    fn append_value(
        &mut self,
        root_builder: &mut MapBuilder<BoxedArrayBuilder, BoxedArrayBuilder>,
    ) -> Result<()> {
        let len = self.lengths.next().unwrap()?;
        self.key
            .append_value(&mut root_builder.keys().builder, len as usize)?;
        self.value
            .append_value(&mut root_builder.values().builder, len as usize)?;
        Ok(())
    }

    fn next(
        &mut self,
        root_builder: &mut MapBuilder<BoxedArrayBuilder, BoxedArrayBuilder>,
    ) -> Option<Result<()>> {
        match self.present.next() {
            Some(present) => {
                if present {
                    if let Err(err) = self.append_value(root_builder) {
                        return Some(Err(err));
                    }
                }
                if let Err(err) = root_builder.append(present).context(error::MapBuilderSnafu) {
                    return Some(Err(err));
                };
            }
            None => return None,
        }

        Some(Ok(()))
    }

    pub fn collect_chunk(
        &mut self,
        root_builder: &mut MapBuilder<BoxedArrayBuilder, BoxedArrayBuilder>,
        chunk: usize,
    ) -> Option<Result<()>> {
        for _ in 0..chunk {
            match self.next(root_builder) {
                Some(Ok(_)) => {
                    // continue
                }
                Some(Err(err)) => return Some(Err(err)),
                None => break,
            }
        }

        Some(Ok(()))
    }
}

pub fn new_map_iter(column: &Column, stripe: &Stripe) -> Result<MapDecoder> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
    let reader = stripe.stream_map.get(column, Kind::Length)?;
    let lengths = get_rle_reader(column, reader)?;

    let children = column.children();
    let key = &children[0];
    let value = &children[1];

    let key = reader_factory(key, stripe)?;
    let value = reader_factory(value, stripe)?;

    Ok(MapDecoder {
        key: Box::new(key),
        value: Box::new(value),
        present: Box::new(present.into_iter()),
        lengths,
    })
}
