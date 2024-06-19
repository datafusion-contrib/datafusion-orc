[![test](https://github.com/datafusion-contrib/datafusion-orc/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-orc/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/WenyXu/orc-rs/branch/main/graph/badge.svg?token=2CSHZX02XM)](https://codecov.io/gh/WenyXu/orc-rs)
[![Crates.io](https://img.shields.io/crates/v/orc-rust)](https://crates.io/crates/orc-rust)
[![Crates.io](https://img.shields.io/crates/d/orc-rust)](https://crates.io/crates/orc-rust)

# orc-rust

A native Rust implementation of the [Apache ORC](https://orc.apache.org) file format,
providing API's to read data into [Apache Arrow](https://arrow.apache.org) in-memory arrays.

See the [documentation](https://docs.rs/orc-rust/latest/orc_rust/) for examples on how to use this crate.

## Supported features

This crate currently only supports reading ORC files into Arrow arrays. Write support is planned
(see [Roadmap](#roadmap)). The below features listed relate only to reading ORC files.
At this time, we aim to support the [ORCv1](https://orc.apache.org/specification/ORCv1/) specification only.

- Read synchronously & asynchronously (using Tokio)
- All compression types (Zlib, Snappy, Lzo, Lz4, Zstd)
- All ORC data types
- All encodings
- Rudimentary support for retrieving statistics
- Retrieving user metadata into Arrow schema metadata

## Roadmap

The long term vision for this crate is to be feature complete enough to be donated to the
[arrow-rs](https://github.com/apache/arrow-rs) project.

The following lists the rough roadmap for features to be implemented, from highest to lowest priority.

- Performance enhancements
- DataFusion integration
- Predicate pushdown
- Row indices
- Bloom filters
- Write from Arrow arrays
- Encryption

A non-Arrow API interface is not planned at the moment. Feel free to raise an issue if there is such
a use case.

## Version compatibility

No guarantees are provided about stability across versions. We will endeavour to keep the top level API's
(`ArrowReader` and `ArrowStreamReader`) as stable as we can, but other API's provided may change as we
explore the interface we want the library to expose.

Versions will be released on an ad-hoc basis (with no fixed schedule).

## Mapping ORC types to Arrow types

The following table lists how ORC data types are read into Arrow data types:

| ORC Data Type     | Arrow Data Type             | Notes |
| ----------------- | --------------------------  | ----- |
| Boolean           | Boolean                     |       |
| TinyInt           | Int8                        |       |
| SmallInt          | Int16                       |       |
| Int               | Int32                       |       |
| BigInt            | Int64                       |       |
| Float             | Float32                     |       |
| Double            | Float64                     |       |
| String            | Utf8                        |       |
| Char              | Utf8                        |       |
| VarChar           | Utf8                        |       |
| Binary            | Binary                      |       |
| Decimal           | Decimal128                  |       |
| Date              | Date32                      |       |
| Timestamp         | Timestamp(Nanosecond, None) | ¹     |
| Timestamp instant | Timestamp(Nanosecond, UTC)  | ¹     |
| Struct            | Struct                      |       |
| List              | List                        |       |
| Map               | Map                         |       |
| Union             | Union(_, Sparse)            | ²     |

¹: `ArrowReaderBuilder::with_schema` allows configuring different time units or decoding to
`Decimal128(38, 9)` (i128 of non-leap nanoseconds since UNIX epoch).
Overflows may happen while decoding to a non-Seconds time unit, and results in `OrcError`.
Loss of precision may happen while decoding to a non-Nanosecond time unit, and results in `OrcError`.
`Decimal128(38, 9)` avoids both overflows and loss of precision.

²: Currently only supports a maximum of 127 variants

## Contributing

All contributions are welcome! Feel free to raise an issue if you have a feature request, bug report,
or a question. Feel free to raise a Pull Request without raising an issue first, as long as the Pull
Request is descriptive enough.

Some tools we use in addition to the standard `cargo` that require installation are:

- [taplo](https://taplo.tamasfe.dev/)
- [typos](https://crates.io/crates/typos)

```shell
cargo install typos-cli
cargo install taplo-cli
```

```shell
# Building the crate
cargo build

# Running the test suite
cargo test

# Simple benchmarks
cargo bench

# Formatting TOML files
taplo format

# Detect any typos in the codebase
typos
```

To regenerate/update the [proto.rs](src/proto.rs) file, execute the [regen.sh](regen.sh) script.

```shell
./regen.sh
```

