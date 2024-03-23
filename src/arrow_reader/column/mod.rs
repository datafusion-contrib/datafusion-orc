use std::sync::Arc;

use arrow::datatypes::Field;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit, UnionMode};
use bytes::Bytes;
use snafu::ResultExt;

use crate::error::{IoSnafu, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::proto::{ColumnEncoding, StripeFooter};
use crate::reader::decode::boolean_rle::BooleanIter;
#[cfg(feature = "async")]
use crate::reader::AsyncChunkReader;
use crate::reader::ChunkReader;
use crate::schema::DataType;
use crate::stripe::Stripe;

pub mod timestamp;

#[derive(Debug)]
pub struct Column {
    number_of_rows: u64,
    footer: Arc<StripeFooter>,
    name: String,
    data_type: DataType,
}

impl From<Column> for Field {
    fn from(value: Column) -> Self {
        let dt = value.arrow_data_type();
        Field::new(value.name, dt, true)
    }
}

impl From<&Column> for Field {
    fn from(value: &Column) -> Self {
        let dt = value.arrow_data_type();
        Field::new(value.name.clone(), dt, true)
    }
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

    pub fn arrow_data_type(&self) -> ArrowDataType {
        let value_type = match self.data_type {
            DataType::Boolean { .. } => ArrowDataType::Boolean,
            DataType::Byte { .. } => ArrowDataType::Int8,
            DataType::Short { .. } => ArrowDataType::Int16,
            DataType::Int { .. } => ArrowDataType::Int32,
            DataType::Long { .. } => ArrowDataType::Int64,
            DataType::Float { .. } => ArrowDataType::Float32,
            DataType::Double { .. } => ArrowDataType::Float64,
            DataType::String { .. } | DataType::Varchar { .. } | DataType::Char { .. } => {
                ArrowDataType::Utf8
            }
            DataType::Binary { .. } => ArrowDataType::Binary,
            DataType::Decimal {
                precision, scale, ..
            } => ArrowDataType::Decimal128(precision as u8, scale as i8),
            DataType::Timestamp { .. } => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::TimestampWithLocalTimezone { .. } => {
                // TODO: get writer timezone
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            DataType::Date { .. } => ArrowDataType::Date32,
            DataType::Struct { .. } => {
                let children = self
                    .children()
                    .into_iter()
                    .map(|col| {
                        let dt = col.arrow_data_type();
                        Field::new(col.name(), dt, true)
                    })
                    .collect();
                ArrowDataType::Struct(children)
            }
            DataType::List { .. } => {
                let children = self.children();
                assert_eq!(children.len(), 1);
                ArrowDataType::new_list(children[0].arrow_data_type(), true)
            }
            DataType::Map { .. } => {
                let children = self.children();
                assert_eq!(children.len(), 2);
                let key = &children[0];
                let key = key.arrow_data_type();
                let key = Field::new("key", key, false);
                let value = &children[1];
                let value = value.arrow_data_type();
                let value = Field::new("value", value, true);

                let dt = ArrowDataType::Struct(vec![key, value].into());
                let dt = Arc::new(Field::new("entries", dt, true));
                ArrowDataType::Map(dt, false)
            }
            DataType::Union { .. } => {
                let fields = self
                    .children()
                    .iter()
                    .enumerate()
                    .map(|(index, variant)| {
                        // Should be safe as limited to 256 variants total (in from_proto)
                        let index = index as u8 as i8;
                        let arrow_dt = variant.arrow_data_type();
                        // Name shouldn't matter here (only ORC struct types give names to subtypes anyway)
                        let field = Arc::new(Field::new(format!("{index}"), arrow_dt, true));
                        (index, field)
                    })
                    .collect();
                ArrowDataType::Union(fields, UnionMode::Sparse)
            }
        };

        match self.encoding().kind() {
            ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => value_type,
            ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
                ArrowDataType::Dictionary(Box::new(ArrowDataType::UInt64), Box::new(value_type))
            }
        }
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
    pub async fn read_stream_async<R: AsyncChunkReader>(
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
        .stream_map
        .get_opt(column, Kind::Present)
        .map(|reader| BooleanIter::new(reader).collect::<Result<Vec<_>>>())
        .transpose()
}
