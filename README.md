# datafusion-orc
Implementation of ORC file format

[![test](https://github.com/datafusion-contrib/datafusion-orc/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-orc/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/WenyXu/orc-rs/branch/main/graph/badge.svg?token=2CSHZX02XM)](https://codecov.io/gh/WenyXu/orc-rs)
[![Crates.io](https://img.shields.io/crates/v/orc-rust)](https://crates.io/crates/orc-rust)
[![Crates.io](https://img.shields.io/crates/d/orc-rust)](https://crates.io/crates/orc-rust)

Read [Apache ORC](https://orc.apache.org/) in Rust.

* Read ORC files
* Read stripes (the conversion from proto metadata to memory regions)
* Decode stripes (the math of decode stripes into e.g. booleans, runs of RLE, etc.)
* Decode ORC data to [Arrow Datatypes](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) (Async/Sync)


## Current Support

| Column Encoding           | Read | Write | Rust Type             | Arrow  DataType         |
| ------------------------- | ---- | ----- | --------------------- | ----------------------- |
| SmallInt, Int, BigInt     | ✓    |       | i16, i32, i64         | Int16, Int32, Int64     |
| Float, Double             | ✓    |       | f32, f64              | Float32, Float64        |
| String, Char, and VarChar | ✓    |       | string                | Utf8                    |
| Boolean                   | ✓    |       | bool                  | Boolean                 |
| TinyInt                   | ✗    |       |                       |                         |
| Binary                    | ✓    |       | Vec\<u8\>             | Binary                  |
| Decimal                   | ✗    |       |                       |                         |
| Date                      | ✓    |       | chrono::NaiveDate     | Date32                  |
| Timestamp                 | ✓    |       | chrono::NaiveDateTime | Timestamp(Nanosecond,_) |
| Timestamp instant         | ✗    |       |                       |                         |
| Struct                    | ✗    |       |                       |                         |
| List                      | ✗    |       |                       |                         |
| Map                       | ✗    |       |                       |                         |
| Union                     | ✗    |       |                       |                         |


## Compression Support

| Compression | Support |
| ----------- | ------- |
| None        | ✓       |
| ZLIB        | ✓       |
| SNAPPY      | ✗       |
| LZO         | ✗       |
| LZ4         | ✗       |
| ZSTD        | ✓       |




