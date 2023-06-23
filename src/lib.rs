#![feature(trait_upcasting)]
#![allow(incomplete_features)]
#![feature(iterator_try_collect)]
#![feature(iter_next_chunk)]

pub mod arrow_reader;
pub mod async_arrow_reader;
pub mod error;
pub mod proto;
pub mod reader;

pub use arrow_reader::{ArrowReader, Cursor};
pub use async_arrow_reader::ArrowStreamReader;
pub use reader::Reader;
