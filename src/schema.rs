// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use snafu::{ensure, OptionExt};

use crate::error::{NoTypesSnafu, Result, UnexpectedSnafu};
use crate::projection::ProjectionMask;
use crate::proto;

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit, UnionMode};

/// Represents the root data type of the ORC file. Contains multiple named child types
/// which map to the columns available. Allows projecting only specific columns from
/// the base schema.
///
/// This is essentially a Struct type, but with special handling such as for projection
/// and transforming into an Arrow schema.
///
/// Note that the ORC spec states the root type does not necessarily have to be a Struct.
/// Currently we only support having a Struct as the root data type.
///
/// See: <https://orc.apache.org/docs/types.html>
#[derive(Debug, Clone)]
pub struct RootDataType {
    children: Vec<NamedColumn>,
}

impl RootDataType {
    /// Root column index is always 0.
    pub fn column_index(&self) -> usize {
        0
    }

    /// Base columns of the file.
    pub fn children(&self) -> &[NamedColumn] {
        &self.children
    }

    /// Convert into an Arrow schema.
    pub fn create_arrow_schema(&self, user_metadata: &HashMap<String, String>) -> Schema {
        let fields = self
            .children
            .iter()
            .map(|col| {
                let dt = col.data_type().to_arrow_data_type();
                Field::new(col.name(), dt, true)
            })
            .collect::<Vec<_>>();
        Schema::new_with_metadata(fields, user_metadata.clone())
    }

    /// Create new root data type based on mask of columns to project.
    pub fn project(&self, mask: &ProjectionMask) -> Self {
        // TODO: fix logic here to account for nested projection
        let children = self
            .children
            .iter()
            .filter(|col| mask.is_index_projected(col.data_type().column_index()))
            .map(|col| col.to_owned())
            .collect::<Vec<_>>();
        Self { children }
    }

    /// Construct from protobuf types.
    pub(crate) fn from_proto(types: &[proto::Type]) -> Result<Self> {
        ensure!(!types.is_empty(), NoTypesSnafu {});
        let children = parse_struct_children_from_proto(types, 0)?;
        Ok(Self { children })
    }
}

impl Display for RootDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROOT")?;
        for child in &self.children {
            write!(f, "\n  {child}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct NamedColumn {
    name: String,
    data_type: DataType,
}

impl NamedColumn {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl Display for NamedColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name(), self.data_type())
    }
}

/// Helper function since this is duplicated for [`RootDataType`] and [`DataType::Struct`]
/// parsing from proto.
fn parse_struct_children_from_proto(
    types: &[proto::Type],
    column_index: usize,
) -> Result<Vec<NamedColumn>> {
    // These pre-conditions should always be upheld, especially as this is a private function
    assert!(column_index < types.len());
    let ty = &types[column_index];
    assert!(ty.kind() == proto::r#type::Kind::Struct);
    ensure!(
        ty.subtypes.len() == ty.field_names.len(),
        UnexpectedSnafu {
            msg: format!(
                "Struct type for column index {} must have matching lengths for subtypes and field names lists",
                column_index,
            )
        }
    );
    let children = ty
        .subtypes
        .iter()
        .zip(ty.field_names.iter())
        .map(|(&index, name)| {
            let index = index as usize;
            let name = name.to_owned();
            let data_type = DataType::from_proto(types, index)?;
            Ok(NamedColumn { name, data_type })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(children)
}

/// Represents the exact data types supported by ORC.
///
/// Each variant holds the column index in order to associate the type
/// with the specific column data present in the stripes.
#[derive(Debug, Clone)]
pub enum DataType {
    /// 1 bit packed data.
    Boolean { column_index: usize },
    /// 8 bit integer, also called TinyInt.
    Byte { column_index: usize },
    /// 16 bit integer, also called SmallInt.
    Short { column_index: usize },
    /// 32 bit integer.
    Int { column_index: usize },
    /// 64 bit integer, also called BigInt.
    Long { column_index: usize },
    /// 32 bit floating-point number.
    Float { column_index: usize },
    /// 64 bit floating-point number.
    Double { column_index: usize },
    /// UTF-8 encoded strings.
    String { column_index: usize },
    /// UTF-8 encoded strings, with an upper length limit on values.
    Varchar {
        column_index: usize,
        max_length: u32,
    },
    /// UTF-8 encoded strings, with an upper length limit on values.
    Char {
        column_index: usize,
        max_length: u32,
    },
    /// Arbitrary byte array values.
    Binary { column_index: usize },
    /// Decimal numbers with a fixed precision and scale.
    Decimal {
        column_index: usize,
        // TODO: narrow to u8
        precision: u32,
        scale: u32,
    },
    /// Represents specific date and time, down to the nanosecond, as offset
    /// since 1st January 2015, with no timezone.
    ///
    /// The date and time represented by values of this column does not change
    /// based on the reader's timezone.
    Timestamp { column_index: usize },
    /// Represents specific date and time, down to the nanosecond, as offset
    /// since 1st January 2015, with timezone.
    ///
    /// The date and time represented by values of this column changes based
    /// on the reader's timezone (is a fixed instant in time).
    TimestampWithLocalTimezone { column_index: usize },
    /// Represents specific date (without time) as days since the UNIX epoch
    /// (1st January 1970 UTC).
    Date { column_index: usize },
    /// Compound type with named child subtypes, representing a structured
    /// collection of children types.
    Struct {
        column_index: usize,
        children: Vec<NamedColumn>,
    },
    /// Compound type where each value in the column is a list of values
    /// of another type, specified by the child type.
    List {
        column_index: usize,
        child: Box<DataType>,
    },
    /// Compound type with two children subtypes, key and value, representing
    /// key-value pairs for column values.
    Map {
        column_index: usize,
        key: Box<DataType>,
        value: Box<DataType>,
    },
    /// Compound type which can represent multiple types of data within
    /// the same column.
    ///
    /// It's variants represent which types it can be (where each value in
    /// the column can only be one of these types).
    Union {
        column_index: usize,
        variants: Vec<DataType>,
    },
}

impl DataType {
    /// Retrieve the column index of this data type, used for getting the specific column
    /// streams/statistics in the file.
    pub fn column_index(&self) -> usize {
        match self {
            DataType::Boolean { column_index } => *column_index,
            DataType::Byte { column_index } => *column_index,
            DataType::Short { column_index } => *column_index,
            DataType::Int { column_index } => *column_index,
            DataType::Long { column_index } => *column_index,
            DataType::Float { column_index } => *column_index,
            DataType::Double { column_index } => *column_index,
            DataType::String { column_index } => *column_index,
            DataType::Varchar { column_index, .. } => *column_index,
            DataType::Char { column_index, .. } => *column_index,
            DataType::Binary { column_index } => *column_index,
            DataType::Decimal { column_index, .. } => *column_index,
            DataType::Timestamp { column_index } => *column_index,
            DataType::TimestampWithLocalTimezone { column_index } => *column_index,
            DataType::Date { column_index } => *column_index,
            DataType::Struct { column_index, .. } => *column_index,
            DataType::List { column_index, .. } => *column_index,
            DataType::Map { column_index, .. } => *column_index,
            DataType::Union { column_index, .. } => *column_index,
        }
    }

    /// All children column indices.
    pub fn children_indices(&self) -> Vec<usize> {
        match self {
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
                .flat_map(|col| col.data_type().children_indices())
                .collect(),
            DataType::List { child, .. } => child.all_indices(),
            DataType::Map { key, value, .. } => {
                let mut indices = key.children_indices();
                indices.extend(value.children_indices());
                indices
            }
            DataType::Union { variants, .. } => variants
                .iter()
                .flat_map(|dt| dt.children_indices())
                .collect(),
        }
    }

    /// Includes self index and all children column indices.
    pub fn all_indices(&self) -> Vec<usize> {
        let mut indices = vec![self.column_index()];
        indices.extend(self.children_indices());
        indices
    }

    fn from_proto(types: &[proto::Type], column_index: usize) -> Result<Self> {
        use proto::r#type::Kind;

        let ty = types.get(column_index).context(UnexpectedSnafu {
            msg: format!("Column index out of bounds: {column_index}"),
        })?;
        let dt = match ty.kind() {
            Kind::Boolean => Self::Boolean { column_index },
            Kind::Byte => Self::Byte { column_index },
            Kind::Short => Self::Short { column_index },
            Kind::Int => Self::Int { column_index },
            Kind::Long => Self::Long { column_index },
            Kind::Float => Self::Float { column_index },
            Kind::Double => Self::Double { column_index },
            Kind::String => Self::String { column_index },
            Kind::Binary => Self::Binary { column_index },
            Kind::Timestamp => Self::Timestamp { column_index },
            Kind::List => {
                ensure!(
                    ty.subtypes.len() == 1,
                    UnexpectedSnafu {
                        msg: format!(
                            "List type for column index {} must have 1 sub type, found {}",
                            column_index,
                            ty.subtypes.len()
                        )
                    }
                );
                let child = ty.subtypes[0] as usize;
                let child = Box::new(Self::from_proto(types, child)?);
                Self::List {
                    column_index,
                    child,
                }
            }
            Kind::Map => {
                ensure!(
                    ty.subtypes.len() == 2,
                    UnexpectedSnafu {
                        msg: format!(
                            "Map type for column index {} must have 2 sub types, found {}",
                            column_index,
                            ty.subtypes.len()
                        )
                    }
                );
                let key = ty.subtypes[0] as usize;
                let key = Box::new(Self::from_proto(types, key)?);
                let value = ty.subtypes[1] as usize;
                let value = Box::new(Self::from_proto(types, value)?);
                Self::Map {
                    column_index,
                    key,
                    value,
                }
            }
            Kind::Struct => {
                let children = parse_struct_children_from_proto(types, column_index)?;
                Self::Struct {
                    column_index,
                    children,
                }
            }
            Kind::Union => {
                // TODO: bump this limit up to 256
                ensure!(
                    ty.subtypes.len() <= 127,
                    UnexpectedSnafu {
                        msg: format!(
                            "Union type for column index {} cannot exceed 127 variants, found {}",
                            column_index,
                            ty.subtypes.len()
                        )
                    }
                );
                let variants = ty
                    .subtypes
                    .iter()
                    .map(|&index| {
                        let index = index as usize;
                        Self::from_proto(types, index)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::Union {
                    column_index,
                    variants,
                }
            }
            Kind::Decimal => Self::Decimal {
                column_index,
                precision: ty.precision(),
                scale: ty.scale(),
            },
            Kind::Date => Self::Date { column_index },
            Kind::Varchar => Self::Varchar {
                column_index,
                max_length: ty.maximum_length(),
            },
            Kind::Char => Self::Char {
                column_index,
                max_length: ty.maximum_length(),
            },
            Kind::TimestampInstant => Self::TimestampWithLocalTimezone { column_index },
        };
        Ok(dt)
    }

    pub fn to_arrow_data_type(&self) -> ArrowDataType {
        match self {
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
            } => ArrowDataType::Decimal128(*precision as u8, *scale as i8), // TODO: safety of cast?
            DataType::Timestamp { .. } => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::TimestampWithLocalTimezone { .. } => {
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            DataType::Date { .. } => ArrowDataType::Date32,
            DataType::Struct { children, .. } => {
                let children = children
                    .iter()
                    .map(|col| {
                        let dt = col.data_type().to_arrow_data_type();
                        Field::new(col.name(), dt, true)
                    })
                    .collect();
                ArrowDataType::Struct(children)
            }
            DataType::List { child, .. } => {
                let child = child.to_arrow_data_type();
                ArrowDataType::new_list(child, true)
            }
            DataType::Map { key, value, .. } => {
                // TODO: this needs to be kept in sync with MapArrayDecoder
                //       move to common location?
                // TODO: should it be "keys" and "values" (like arrow-rs)
                //       or "key" and "value" like PyArrow and in Schema.fbs?
                let key = key.to_arrow_data_type();
                let key = Field::new("keys", key, false);
                let value = value.to_arrow_data_type();
                let value = Field::new("values", value, true);

                let dt = ArrowDataType::Struct(vec![key, value].into());
                let dt = Arc::new(Field::new("entries", dt, false));
                ArrowDataType::Map(dt, false)
            }
            DataType::Union { variants, .. } => {
                let fields = variants
                    .iter()
                    .enumerate()
                    .map(|(index, variant)| {
                        // Limited to 127 variants max (in from_proto)
                        // TODO: Support up to including 256
                        //       Need to do Union within Union
                        let index = index as u8 as i8;
                        let arrow_dt = variant.to_arrow_data_type();
                        // Name shouldn't matter here (only ORC struct types give names to subtypes anyway)
                        // Using naming convention following PyArrow for easier comparison
                        let field = Arc::new(Field::new(format!("_union_{index}"), arrow_dt, true));
                        (index, field)
                    })
                    .collect();
                ArrowDataType::Union(fields, UnionMode::Sparse)
            }
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean { column_index: _ } => write!(f, "BOOLEAN"),
            DataType::Byte { column_index: _ } => write!(f, "BYTE"),
            DataType::Short { column_index: _ } => write!(f, "SHORT"),
            DataType::Int { column_index: _ } => write!(f, "INTEGER"),
            DataType::Long { column_index: _ } => write!(f, "LONG"),
            DataType::Float { column_index: _ } => write!(f, "FLOAT"),
            DataType::Double { column_index: _ } => write!(f, "DOUBLE"),
            DataType::String { column_index: _ } => write!(f, "STRING"),
            DataType::Varchar {
                column_index: _,
                max_length,
            } => write!(f, "VARCHAR({max_length})"),
            DataType::Char {
                column_index: _,
                max_length,
            } => write!(f, "CHAR({max_length})"),
            DataType::Binary { column_index: _ } => write!(f, "BINARY"),
            DataType::Decimal {
                column_index: _,
                precision,
                scale,
            } => write!(f, "DECIMAL({precision}, {scale})"),
            DataType::Timestamp { column_index: _ } => write!(f, "TIMESTAMP"),
            DataType::TimestampWithLocalTimezone { column_index: _ } => {
                write!(f, "TIMESTAMP INSTANT")
            }
            DataType::Date { column_index: _ } => write!(f, "DATE"),
            DataType::Struct {
                column_index: _,
                children,
            } => {
                write!(f, "STRUCT")?;
                for child in children {
                    write!(f, "\n  {child}")?;
                }
                Ok(())
            }
            DataType::List {
                column_index: _,
                child,
            } => write!(f, "LIST\n  {child}"),
            DataType::Map {
                column_index: _,
                key,
                value,
            } => write!(f, "MAP\n  {key}\n  {value}"),
            DataType::Union {
                column_index: _,
                variants,
            } => {
                write!(f, "UNION")?;
                for variant in variants {
                    write!(f, "\n  {variant}")?;
                }
                Ok(())
            }
        }
    }
}
