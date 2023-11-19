use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use snafu::{ensure, OptionExt};

use crate::error::{NoTypesSnafu, Result, UnexpectedSnafu};
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
    children: Vec<(String, DataType)>,
}

impl RootDataType {
    /// Root column index is always 0.
    pub fn column_index(&self) -> usize {
        0
    }

    /// Base columns of the file.
    pub fn children(&self) -> &[(String, DataType)] {
        &self.children
    }

    /// Convert into an Arrow schema.
    pub fn create_arrow_schema(&self, user_metadata: &HashMap<String, String>) -> Schema {
        let fields = self
            .children
            .iter()
            .map(|(name, dt)| {
                let dt = dt.to_arrow_data_type();
                Field::new(name, dt, true)
            })
            .collect::<Vec<_>>();
        Schema::new_with_metadata(fields, user_metadata.clone())
    }

    /// Project only specific columns from the root type by column name.
    pub fn project<T: AsRef<str>>(&self, fields: &[T]) -> Self {
        // TODO: change project to accept project mask (vec of bools) instead of relying on col names?
        // TODO: be able to nest project? (i.e. project child struct data type) unsure if actually desirable
        let fields = fields.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let children = self
            .children
            .iter()
            .filter(|c| fields.contains(&c.0.as_str()))
            .map(|c| c.to_owned())
            .collect::<Vec<_>>();
        Self { children }
    }

    /// Construct from protobuf types.
    pub fn from_proto(types: &[proto::Type]) -> Result<Self> {
        ensure!(!types.is_empty(), NoTypesSnafu {});
        let children = parse_struct_children_from_proto(types, 0)?;
        Ok(Self { children })
    }
}

impl Display for RootDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROOT")?;
        for child in &self.children {
            write!(f, "\n  {} {}", child.0, child.1)?;
        }
        Ok(())
    }
}

/// Helper function since this is duplicated for [`RootDataType`] and [`DataType::Struct`]
/// parsing from proto.
fn parse_struct_children_from_proto(
    types: &[proto::Type],
    column_index: usize,
) -> Result<Vec<(String, DataType)>> {
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
            let dt = DataType::from_proto(types, index)?;
            Ok((name, dt))
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
        children: Vec<(String, DataType)>,
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

    fn from_proto(types: &[proto::Type], column_index: usize) -> Result<Self> {
        let ty = types.get(column_index).context(UnexpectedSnafu {
            msg: format!("Column index out of bounds: {column_index}"),
        })?;
        let dt = match ty.kind() {
            proto::r#type::Kind::Boolean => Self::Boolean { column_index },
            proto::r#type::Kind::Byte => Self::Byte { column_index },
            proto::r#type::Kind::Short => Self::Short { column_index },
            proto::r#type::Kind::Int => Self::Int { column_index },
            proto::r#type::Kind::Long => Self::Long { column_index },
            proto::r#type::Kind::Float => Self::Float { column_index },
            proto::r#type::Kind::Double => Self::Double { column_index },
            proto::r#type::Kind::String => Self::String { column_index },
            proto::r#type::Kind::Binary => Self::Binary { column_index },
            proto::r#type::Kind::Timestamp => Self::Timestamp { column_index },
            proto::r#type::Kind::List => {
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
            proto::r#type::Kind::Map => {
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
            proto::r#type::Kind::Struct => {
                let children = parse_struct_children_from_proto(types, column_index)?;
                Self::Struct {
                    column_index,
                    children,
                }
            }
            proto::r#type::Kind::Union => {
                ensure!(
                    ty.subtypes.len() <= 256,
                    UnexpectedSnafu {
                        msg: format!(
                            "Union type for column index {} cannot exceed 256 variants, found {}",
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
            proto::r#type::Kind::Decimal => Self::Decimal {
                column_index,
                precision: ty.precision(),
                scale: ty.scale(),
            },
            proto::r#type::Kind::Date => Self::Date { column_index },
            proto::r#type::Kind::Varchar => Self::Varchar {
                column_index,
                max_length: ty.maximum_length(),
            },
            proto::r#type::Kind::Char => Self::Char {
                column_index,
                max_length: ty.maximum_length(),
            },
            proto::r#type::Kind::TimestampInstant => {
                Self::TimestampWithLocalTimezone { column_index }
            }
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
            } => ArrowDataType::Decimal128(*precision as u8, *scale as i8),
            DataType::Timestamp { .. } => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::TimestampWithLocalTimezone { .. } => {
                // TODO: get writer timezone
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            DataType::Date { .. } => ArrowDataType::Date32,
            DataType::Struct { children, .. } => {
                let children = children
                    .iter()
                    .map(|(name, dt)| {
                        let dt = dt.to_arrow_data_type();
                        Field::new(name, dt, true)
                    })
                    .collect();
                ArrowDataType::Struct(children)
            }
            DataType::List { child, .. } => {
                let child = child.to_arrow_data_type();
                ArrowDataType::new_list(child, true)
            }
            DataType::Map { key, value, .. } => {
                let key = key.to_arrow_data_type();
                let key = Field::new("key", key, true);
                let value = value.to_arrow_data_type();
                let value = Field::new("value", value, true);

                let dt = ArrowDataType::Struct(vec![key, value].into());
                let dt = Arc::new(Field::new("item", dt, true));
                ArrowDataType::Map(dt, true)
            }
            DataType::Union { variants, .. } => {
                let fields = variants
                    .iter()
                    .enumerate()
                    .map(|(index, variant)| {
                        // Should be safe as limited to 256 variants total (in from_proto)
                        let index = index as u8 as i8;
                        let arrow_dt = variant.to_arrow_data_type();
                        // Name shouldn't matter here (only ORC struct types give names to subtypes anyway)
                        let field = Arc::new(Field::new(format!("{index}"), arrow_dt, true));
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
                    write!(f, "\n  {} {}", child.0, child.1)?;
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
