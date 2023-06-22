# datafusion-orc
Implementation of ORC file format

[![test](https://github.com/wenyxu/orc-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/wenyxu/orc-rs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/WenyXu/orc-rs/branch/main/graph/badge.svg?token=2CSHZX02XM)](https://codecov.io/gh/WenyXu/orc-rs)

Read [Apache ORC](https://orc.apache.org/) in Rust.

This repository providing a toolkit to:

* Read ORC files (proto structures)
* Read stripes (the conversion from proto metadata to memory regions)
* Decode stripes (the math of decode stripes into e.g. booleans, runs of RLE, etc.)

It currently reads the following (logical) types:

* Booleans
* Strings
* Integers
* Floats

And non-native logical types:
* Timestamp
* Date

What is not yet implemented:

* Snappy, LZO decompression
* RLE v1 decoding
* Utility functions to decode non-native logical types:
    * Decimal
    * Struct
    * List
    * Union


