use std::sync::Arc;

use bytes::Bytes;
use snafu::ResultExt;

use crate::error::{IoSnafu, Result};
use crate::proto::stream::Kind;
use crate::proto::{ColumnEncoding, StripeFooter};
use crate::reader::decode::boolean_rle::BooleanIter;
use crate::reader::ChunkReader;
use crate::schema::DataType;
use crate::stripe::Stripe;

#[derive(Clone, Debug)]
pub struct Column {
    number_of_rows: u64,
    footer: Arc<StripeFooter>,
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(
        name: &str,
        data_type: &DataType,
        footer: &Arc<StripeFooter>,
        number_of_rows: u64,
    ) -> Self {
        Self {
            number_of_rows,
            footer: footer.clone(),
            data_type: data_type.clone(),
            name: name.to_string(),
        }
    }

    pub fn dictionary_size(&self) -> usize {
        let column = self.data_type.column_index();
        self.footer.columns[column]
            .dictionary_size
            .unwrap_or_default() as usize
    }

    pub fn encoding(&self) -> ColumnEncoding {
        let column = self.data_type.column_index();
        self.footer.columns[column].clone()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_id(&self) -> u32 {
        self.data_type.column_index() as u32
    }

    pub fn children(&self) -> Vec<Column> {
        match &self.data_type {
            DataType::Boolean { .. }
            | DataType::Byte { .. }
            | DataType::Short { .. }
            | DataType::Int { .. }
            | DataType::Long { .. }
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::String { .. }
            | DataType::Varchar { .. }
            | DataType::Char { .. }
            | DataType::Binary { .. }
            | DataType::Decimal { .. }
            | DataType::Timestamp { .. }
            | DataType::TimestampWithLocalTimezone { .. }
            | DataType::Date { .. } => vec![],
            DataType::Struct { children, .. } => children
                .iter()
                .map(|col| Column {
                    number_of_rows: self.number_of_rows,
                    footer: self.footer.clone(),
                    name: col.name().to_string(),
                    data_type: col.data_type().clone(),
                })
                .collect(),
            DataType::List { child, .. } => {
                vec![Column {
                    number_of_rows: self.number_of_rows,
                    footer: self.footer.clone(),
                    name: "item".to_string(),
                    data_type: *child.clone(),
                }]
            }
            DataType::Map { key, value, .. } => {
                vec![
                    Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: "key".to_string(),
                        data_type: *key.clone(),
                    },
                    Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: "value".to_string(),
                        data_type: *value.clone(),
                    },
                ]
            }
            DataType::Union { variants, .. } => {
                // TODO: might need corrections
                variants
                    .iter()
                    .enumerate()
                    .map(|(index, data_type)| Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: format!("{index}"),
                        data_type: data_type.clone(),
                    })
                    .collect()
            }
        }
    }

    pub fn read_stream<R: ChunkReader>(reader: &mut R, start: u64, length: u64) -> Result<Bytes> {
        reader.get_bytes(start, length).context(IoSnafu)
    }

    #[cfg(feature = "async")]
    pub async fn read_stream_async<R: crate::reader::AsyncChunkReader>(
        reader: &mut R,
        start: u64,
        length: u64,
    ) -> Result<Bytes> {
        reader.get_bytes(start, length).await.context(IoSnafu)
    }
}

/// Prefetch present stream for entire column in stripe.
///
/// Makes subsequent operations easier to handle.
pub fn get_present_vec(column: &Column, stripe: &Stripe) -> Result<Option<Vec<bool>>> {
    stripe
        .stream_map()
        .get_opt(column, Kind::Present)
        .map(|reader| BooleanIter::new(reader).collect::<Result<Vec<_>>>())
        .transpose()
}
