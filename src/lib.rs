//! A native Rust implementation of the [Apache ORC](https://orc.apache.org) file format,
//! providing API's to read data into [Apache Arrow](https://arrow.apache.org) in-memory arrays.
//!
//! # Example usage
//!
//! ```no_run
//! # use std::fs::File;
//! # use orc_rust::arrow_reader::{ArrowReader, ArrowReaderBuilder};
//! let file = File::open("/path/to/file.orc").unwrap();
//! let reader = ArrowReaderBuilder::try_new(file).unwrap().build();
//! let record_batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
//! ```

pub mod arrow_reader;
#[cfg(feature = "async")]
pub mod async_arrow_reader;
pub mod error;
pub mod projection;
mod proto;
pub mod reader;
pub mod schema;
pub mod statistics;
pub mod stripe;

pub use arrow_reader::{ArrowReader, ArrowReaderBuilder};
#[cfg(feature = "async")]
pub use async_arrow_reader::ArrowStreamReader;

#[cfg(feature = "datafusion")]
pub mod datafusion;
