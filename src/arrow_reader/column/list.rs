use arrow::array::ListBuilder;
use snafu::OptionExt;

use crate::{
    arrow_reader::{reader_factory, BoxedArrayBuilder, Decoder, Stripe},
    error,
};
use crate::{proto::stream::Kind, reader::decode::RleVersion};

use super::{present::new_present_iter, Column};
use crate::error::Result;

pub struct ListDecoder {
    pub(crate) inner: Box<Decoder>,
    present: Box<dyn Iterator<Item = bool> + Send>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl ListDecoder {
    fn append_value(&mut self, root_builder: &mut ListBuilder<BoxedArrayBuilder>) -> Result<()> {
        let len = self.lengths.next().unwrap()?;
        let _ = self
            .inner
            .append_value(&mut root_builder.values().builder, len as usize);

        Ok(())
    }

    fn next(&mut self, root_builder: &mut ListBuilder<BoxedArrayBuilder>) -> Option<Result<()>> {
        match self.present.next() {
            Some(present) => {
                if present {
                    if let Err(err) = self.append_value(root_builder) {
                        return Some(Err(err));
                    }
                }
                root_builder.append(present);
            }
            None => return None,
        }

        Some(Ok(()))
    }

    pub fn collect_chunk(
        &mut self,
        root_builder: &mut ListBuilder<BoxedArrayBuilder>,
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

pub fn new_list_iter(column: &Column, stripe: &Stripe) -> Result<ListDecoder> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
    let version: RleVersion = column.encoding().kind().into();
    let lengths = stripe
        .stream_map
        .get(column, Kind::Length)
        .map(|reader| version.get_unsigned_rle_reader(reader))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    let children = column.children();
    let child = &children[0];

    let decoder = reader_factory(child, stripe)?;

    Ok(ListDecoder {
        lengths,
        inner: Box::new(decoder),
        present: Box::new(present.into_iter()),
    })
}
