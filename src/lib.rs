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
//!
//! See the [`datafusion`] module for information on how to integrate with
//! [Apache DataFusion](https://datafusion.apache.org/).

mod array_decoder;
pub mod arrow_reader;
pub mod arrow_writer;
#[cfg(feature = "async")]
pub mod async_arrow_reader;
mod column;
pub mod compression;
mod encoding;
pub mod error;
mod memory;
pub mod projection;
mod proto;
pub mod reader;
pub mod schema;
pub mod statistics;
pub mod stripe;
mod writer;

pub use arrow_reader::{ArrowReader, ArrowReaderBuilder};
pub use arrow_writer::{ArrowWriter, ArrowWriterBuilder};
#[cfg(feature = "async")]
pub use async_arrow_reader::ArrowStreamReader;

#[cfg(feature = "datafusion")]
pub mod datafusion;
