use std::sync::{Arc, Mutex, Weak};

use arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use lazy_static::lazy_static;
use snafu::ensure;

use crate::error::{self, Result};
use crate::proto::r#type::Kind;
use crate::proto::Type;

#[derive(Debug, Clone)]
pub struct Category {
    name: String,
    is_primitive: bool,
    kind: Kind,
}

impl Category {
    pub fn new(name: &str, is_primitive: bool, kind: Kind) -> Self {
        Self {
            name: name.to_string(),
            is_primitive,
            kind,
        }
    }

    pub fn primitive(&self) -> bool {
        self.is_primitive
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub fn create_field((name, typ): (&str, &Arc<TypeDescription>)) -> Field {
    let kind = typ.kind();
    match kind {
        Kind::Boolean => Field::new(name, DataType::Boolean, true),
        Kind::Byte => Field::new(name, DataType::Int8, true),
        Kind::Short => Field::new(name, DataType::Int16, true),
        Kind::Int => Field::new(name, DataType::Int32, true),
        Kind::Long => Field::new(name, DataType::Int64, true),
        Kind::Float => Field::new(name, DataType::Float32, true),
        Kind::Double => Field::new(name, DataType::Float64, true),
        Kind::String => Field::new(name, DataType::Utf8, true),
        Kind::Binary => Field::new(name, DataType::LargeBinary, true),
        // TODO(weny): handle tz
        Kind::Timestamp => Field::new(
            name,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Kind::List => {
            let children = typ.children();
            assert_eq!(children.len(), 1);

            let (_, typ) = &children[0];
            let value = create_field((name, typ));

            Field::new(name, DataType::List(Arc::new(value)), true)
        }
        Kind::Map => {
            let children = typ.children();
            assert_eq!(children.len(), 2);

            let (name, typ) = &children[1];
            let value = create_field((name, typ));

            Field::new(name, DataType::Map(Arc::new(value), false), true)
        }
        Kind::Struct => {
            let children = typ.children();
            let mut fields = Vec::with_capacity(children.len());
            for (name, child) in &children {
                fields.push(create_field((name, child)));
            }

            Field::new(name, DataType::Struct(Fields::from(fields)), true)
        }
        Kind::Union => {
            let children = typ.children();
            let mut fields = Vec::with_capacity(children.len());
            for (idx, (name, child)) in children.iter().enumerate() {
                fields.push((idx as i8, Arc::new(create_field((name, child)))));
            }

            Field::new(
                name,
                DataType::Union(UnionFields::from_iter(fields), UnionMode::Sparse),
                true,
            )
        }
        Kind::Decimal => {
            let inner = typ.inner.lock().unwrap();
            Field::new(
                name,
                DataType::Decimal128(inner.precision as u8, inner.scale as i8),
                true,
            )
        }
        Kind::Date => Field::new(name, DataType::Date32, true),
        Kind::Varchar => Field::new(name, DataType::Utf8, true),
        Kind::Char => Field::new(name, DataType::Utf8, true),
        Kind::TimestampInstant => todo!(),
    }
}

lazy_static! {
    static ref BOOLEAN: Category = Category::new("boolean", true, Kind::Boolean);
    static ref TINYINT: Category = Category::new("tinyint", true, Kind::Byte);
    static ref SMALLINT: Category = Category::new("smallint", true, Kind::Short);
    static ref INT: Category = Category::new("int", true, Kind::Int);
    static ref BIGINT: Category = Category::new("bigint", true, Kind::Long);
    static ref FLOAT: Category = Category::new("float", true, Kind::Float);
    static ref DOUBLE: Category = Category::new("double", true, Kind::Double);
    static ref STRING: Category = Category::new("string", true, Kind::String);
    static ref DATE: Category = Category::new("date", true, Kind::Date);
    static ref TIMESTAMP: Category = Category::new("timestamp", true, Kind::Timestamp);
    static ref BINARY: Category = Category::new("binary", true, Kind::Binary);
    static ref DECIMAL: Category = Category::new("decimal", true, Kind::Decimal);
    static ref VARCHAR: Category = Category::new("varchar", true, Kind::Varchar);
    static ref CHAR: Category = Category::new("char", true, Kind::Char);
    static ref ARRAY: Category = Category::new("array", false, Kind::List);
    static ref MAP: Category = Category::new("map", false, Kind::Map);
    static ref STRUCT: Category = Category::new("struct", false, Kind::Struct);
    static ref UNIONTYPE: Category = Category::new("uniontype", false, Kind::Union);
}

#[derive(Debug)]
pub struct TypeDescription {
    inner: Mutex<TypeDescriptionInner>,
}

impl TypeDescription {
    pub fn new(category: Category, column: usize) -> Self {
        Self {
            inner: Mutex::new(TypeDescriptionInner::new(category, column)),
        }
    }

    pub fn set_parent(self: &Arc<Self>, parent: Weak<TypeDescription>) {
        self.inner.lock().unwrap().set_parent(parent);
    }

    pub fn add_field(self: &Arc<Self>, name: String, td: Arc<TypeDescription>) {
        let mut inner = self.inner.lock().unwrap();
        inner.add_field(name, td.clone());
        let parent = Arc::downgrade(self);
        td.set_parent(parent);
    }

    pub fn field(&self, name: &str) -> Option<Arc<TypeDescription>> {
        self.inner.lock().unwrap().get_field(name)
    }

    pub fn column_id(&self) -> usize {
        self.inner.lock().unwrap().column
    }

    pub fn kind(&self) -> Kind {
        self.inner.lock().unwrap().category.kind
    }

    pub fn children(&self) -> Vec<(String, Arc<TypeDescription>)> {
        let inner = self.inner.lock().unwrap();

        let children = inner.children.clone().unwrap_or_default();

        let names = inner.field_names.clone();

        names.into_iter().zip(children).collect()
    }
}

#[derive(Debug)]

pub struct TypeDescriptionInner {
    category: Category,
    parent: Option<Weak<TypeDescription>>,
    children: Option<Vec<Arc<TypeDescription>>>,
    field_names: Vec<String>,
    precision: usize,
    scale: usize,
    // column index
    column: usize,
}

const DEFAULT_SCALE: usize = 10;
const DEFAULT_PRECISION: usize = 38;

impl TypeDescriptionInner {
    pub fn new(category: Category, column: usize) -> Self {
        Self {
            category,
            parent: None,
            children: None,
            field_names: Vec::new(),
            precision: DEFAULT_PRECISION,
            scale: DEFAULT_SCALE,
            column,
        }
    }

    pub fn set_parent(&mut self, parent: Weak<TypeDescription>) {
        self.parent = Some(parent);
    }

    pub fn add_field(&mut self, name: String, td: Arc<TypeDescription>) {
        self.field_names.push(name);
        if self.children.is_none() {
            self.children = Some(Vec::new());
        }
        self.children.as_mut().unwrap().push(td);
    }

    pub fn get_field(&self, name: &str) -> Option<Arc<TypeDescription>> {
        let idx = self.field_names.iter().position(|f| f.eq(name));
        idx.and_then(|idx| self.children.as_ref().unwrap().get(idx).cloned())
    }
}

pub fn create_schema(types: &[Type], root_column: usize) -> Result<Arc<TypeDescription>> {
    if types.is_empty() {
        return error::NoTypesSnafu {}.fail();
    }

    let root = &types[root_column];

    match root.kind() {
        Kind::Struct => {
            let td = Arc::new(TypeDescription::new(STRUCT.clone(), root_column));
            let sub_types = &root.subtypes;
            let fields = &root.field_names;
            for (idx, column) in sub_types.iter().enumerate() {
                let child = create_schema(types, *column as usize)?;
                td.add_field(fields[idx].to_string(), child);
            }
            Ok(td)
        }

        Kind::Boolean => Ok(Arc::new(TypeDescription::new(BOOLEAN.clone(), root_column))),

        // 8,16,32,64
        Kind::Byte => Ok(Arc::new(TypeDescription::new(TINYINT.clone(), root_column))),
        Kind::Short => Ok(Arc::new(TypeDescription::new(
            SMALLINT.clone(),
            root_column,
        ))),
        Kind::Int => Ok(Arc::new(TypeDescription::new(INT.clone(), root_column))),
        Kind::Long => Ok(Arc::new(TypeDescription::new(BIGINT.clone(), root_column))),

        // f32/f64
        Kind::Float => Ok(Arc::new(TypeDescription::new(FLOAT.clone(), root_column))),
        Kind::Double => Ok(Arc::new(TypeDescription::new(DOUBLE.clone(), root_column))),

        // String
        Kind::String => Ok(Arc::new(TypeDescription::new(STRING.clone(), root_column))),
        Kind::Varchar => Ok(Arc::new(TypeDescription::new(VARCHAR.clone(), root_column))),
        Kind::Char => Ok(Arc::new(TypeDescription::new(CHAR.clone(), root_column))),

        // Timestamp/Date
        Kind::Timestamp => Ok(Arc::new(TypeDescription::new(
            TIMESTAMP.clone(),
            root_column,
        ))),
        Kind::Date => Ok(Arc::new(TypeDescription::new(DATE.clone(), root_column))),

        // FIXME(weny): Test propose
        Kind::Binary => Ok(Arc::new(TypeDescription::new(BINARY.clone(), root_column))),
        Kind::List => {
            let sub_types = &root.subtypes;
            ensure!(
                sub_types.len() == 1,
                error::UnexpectedSnafu {
                    msg: format!("unexpected number of subtypes for list: {:?}", sub_types)
                }
            );

            let td = Arc::new(TypeDescription::new(ARRAY.clone(), root_column));

            let column = sub_types[0];
            let child = create_schema(types, column as usize)?;
            // TODO(weny): remove dummy name.
            td.add_field("root".to_string(), child);

            Ok(td)
        }
        Kind::Map => {
            let sub_types = &root.subtypes;
            ensure!(
                sub_types.len() == 2,
                error::UnexpectedSnafu {
                    msg: format!("unexpected number of subtypes for map: {:?}", sub_types)
                }
            );

            let td = Arc::new(TypeDescription::new(MAP.clone(), root_column));
            let fields = &root.field_names;
            for (idx, column) in sub_types.iter().enumerate() {
                let child = create_schema(types, *column as usize)?;
                td.add_field(fields[idx].to_string(), child);
            }

            Ok(td)
        }
        Kind::Union => {
            let td = Arc::new(TypeDescription::new(UNIONTYPE.clone(), root_column));

            let sub_types = &root.subtypes;
            let fields = &root.field_names;
            for (idx, column) in sub_types.iter().enumerate() {
                let child = create_schema(types, *column as usize)?;
                td.add_field(fields[idx].to_string(), child);
            }

            Ok(td)
        }
        Kind::Decimal => Ok(Arc::new(TypeDescription::new(DECIMAL.clone(), root_column))),
        Kind::TimestampInstant => todo!(),
    }
}
