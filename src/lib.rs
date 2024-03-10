pub mod arrow_reader;
#[cfg(feature = "async")]
pub mod async_arrow_reader;
pub(crate) mod builder;
pub mod error;
pub mod projection;
pub mod proto;
pub mod reader;
pub mod schema;
pub mod statistics;
pub mod stripe;

pub use arrow_reader::{ArrowReader, ArrowReaderBuilder};
#[cfg(feature = "async")]
pub use async_arrow_reader::ArrowStreamReader;
