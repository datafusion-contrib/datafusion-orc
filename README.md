# datafusion-orc
Implementation of ORC file format read/write with Arrow in-memory format

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

| Column Encoding           | Read | Write | Arrow DataType             |
| ------------------------- | ---- | ----- | -------------------------- |
| SmallInt, Int, BigInt     | ✓    |       | Int16, Int32, Int64        |
| Float, Double             | ✓    |       | Float32, Float64           |
| String, Char, and VarChar | ✓    |       | Utf8                       |
| Boolean                   | ✓    |       | Boolean                    |
| TinyInt                   | ✓    |       | Int8                       |
| Binary                    | ✓    |       | Binary                     |
| Decimal                   | ✓    |       | Decimal128                 |
| Date                      | ✓    |       | Date32                     |
| Timestamp                 | *    |       | Timestamp(Nanosecond,_)    |
| Timestamp instant         | ✓    |       | Timestamp(Nanosecond, UTC) |
| Struct                    | ✓    |       | Struct                     |
| List                      | ✓    |       | List                       |
| Map                       | ✓    |       | Map                        |
| Union                     | ✗    |       |                            |

- *Timestamp support is still being worked on


## Compression Support

| Compression | Read | Write |
| ----------- | ---- | ----- |
| None        | ✓    | ✗     |
| ZLIB        | ✓    | ✗     |
| SNAPPY      | ✓    | ✗     |
| LZO         | ✓    | ✗     |
| LZ4         | ✓    | ✗     |
| ZSTD        | ✓    | ✗     |

## Benchmark

Run `cargo bench` for simple benchmarks.

