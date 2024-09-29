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

use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Field, Schema, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::util::pretty;
#[cfg(feature = "async")]
use futures_util::TryStreamExt;
use orc_rust::arrow_reader::{ArrowReader, ArrowReaderBuilder};
#[cfg(feature = "async")]
use orc_rust::async_arrow_reader::ArrowStreamReader;
use orc_rust::projection::ProjectionMask;

use crate::misc::{LONG_BOOL_EXPECTED, LONG_STRING_DICT_EXPECTED, LONG_STRING_EXPECTED};

mod misc;

fn new_arrow_reader(path: &str, fields: &[&str]) -> ArrowReader<File> {
    let f = File::open(path).expect("no file found");
    let builder = ArrowReaderBuilder::try_new(f).unwrap();
    let projection = ProjectionMask::named_roots(builder.file_metadata().root_data_type(), fields);
    builder.with_projection(projection).build()
}

#[cfg(feature = "async")]
async fn new_arrow_stream_reader_root(path: &str) -> ArrowStreamReader<tokio::fs::File> {
    let f = tokio::fs::File::open(path).await.unwrap();
    ArrowReaderBuilder::try_new_async(f)
        .await
        .unwrap()
        .build_async()
}

#[cfg(feature = "async")]
async fn new_arrow_stream_reader_range(
    path: &str,
    range: Range<usize>,
) -> ArrowStreamReader<tokio::fs::File> {
    let f = tokio::fs::File::open(path).await.unwrap();
    ArrowReaderBuilder::try_new_async(f)
        .await
        .unwrap()
        .with_file_byte_range(range)
        .build_async()
}

fn new_arrow_reader_root(path: &str) -> ArrowReader<File> {
    let f = File::open(path).expect("no file found");
    ArrowReaderBuilder::try_new(f).unwrap().build()
}

fn new_arrow_reader_range(path: &str, range: Range<usize>) -> ArrowReader<File> {
    let f = File::open(path).expect("no file found");
    ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_file_byte_range(range)
        .build()
}

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{}/tests/basic/data/{}", dir, path)
}

#[test]
pub fn test_read_long_bool() {
    let path = basic_path("long_bool.orc");
    let reader = new_arrow_reader(&path, &["long"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(32, batch[0].column(0).len());
    assert_eq!(
        LONG_BOOL_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_long_bool_gzip() {
    let path = basic_path("long_bool_gzip.orc");
    let reader = new_arrow_reader(&path, &["long"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(32, batch[0].column(0).len());
    assert_eq!(
        LONG_BOOL_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_long_string() {
    let path = basic_path("string_long.orc");
    let reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(64, batch[0].column(0).len());

    assert_eq!(
        LONG_STRING_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_string_dirt() {
    let path = basic_path("string_dict.orc");
    let reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(64, batch[0].column(0).len());
    assert_eq!(
        LONG_STRING_DICT_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_string_dirt_gzip() {
    let path = basic_path("string_dict_gzip.orc");
    let reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(64, batch[0].column(0).len());
    assert_eq!(
        LONG_STRING_DICT_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_string_long_long() {
    let path = basic_path("string_long_long.orc");
    let reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(8192, batch[0].column(0).len());
    assert_eq!(10_000 - 8192, batch[1].column(0).len());
}

#[test]
pub fn test_read_f32_long_long_gzip() {
    let path = basic_path("f32_long_long_gzip.orc");
    let reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let total: usize = batch.iter().map(|c| c.column(0).len()).sum();

    assert_eq!(total, 1_000_000);
}

#[test]
pub fn test_read_string_long_long_gzip() {
    let path = basic_path("string_long_long_gzip.orc");
    let reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(8192, batch[0].column(0).len());
    assert_eq!(10_000 - 8192, batch[1].column(0).len());
}

#[test]
pub fn basic_test() {
    let path = basic_path("test.orc");
    let reader = new_arrow_reader(&path, &["a", "b", "str_direct", "d", "e", "f"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+-----+-------+------------+-----+-----+-------+",
        "| a   | b     | str_direct | d   | e   | f     |",
        "+-----+-------+------------+-----+-----+-------+",
        "| 1.0 | true  | a          | a   | ddd | aaaaa |",
        "| 2.0 | false | cccccc     | bb  | cc  | bbbbb |",
        "|     |       |            |     |     |       |",
        "| 4.0 | true  | ddd        | ccc | bb  | ccccc |",
        "| 5.0 | false | ee         | ddd | a   | ddddd |",
        "+-----+-------+------------+-----+-----+-------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_2() {
    let path = basic_path("test.orc");
    let reader = new_arrow_reader(
        &path,
        &[
            "int_short_repeated",
            "int_neg_short_repeated",
            "int_delta",
            "int_neg_delta",
            "int_direct",
            "int_neg_direct",
            "bigint_direct",
            "bigint_neg_direct",
            "bigint_other",
            "utf8_increase",
            "utf8_decrease",
        ],
    );
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+",
        "| int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease |",
        "+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+",
        "| 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         |",
        "| 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          |",
        "|                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           |",
        "| 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            |",
        "| 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             |",
        "+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_3() {
    let path = basic_path("test.orc");
    let reader = new_arrow_reader(&path, &["timestamp_simple", "date_simple"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+----------------------------+-------------+",
        "| timestamp_simple           | date_simple |",
        "+----------------------------+-------------+",
        "| 2023-04-01T20:15:30.002    | 2023-04-01  |",
        "| 2021-08-22T07:26:44.525777 | 2023-03-01  |",
        "| 2023-01-01T00:00:00        | 2023-01-01  |",
        "| 2023-02-01T00:00:00        | 2023-02-01  |",
        "| 2023-03-01T00:00:00        | 2023-03-01  |",
        "+----------------------------+-------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_nested_struct() {
    let path = basic_path("nested_struct.orc");
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let expected = [
        "+-------------------+",
        "| nest              |",
        "+-------------------+",
        "| {a: 1.0, b: true} |",
        "| {a: 3.0, b: }     |",
        "| {a: , b: }        |",
        "|                   |",
        "| {a: -3.0, b: }    |",
        "+-------------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_nested_array() {
    let path = basic_path("nested_array.orc");
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+--------------------+",
        "| value              |",
        "+--------------------+",
        "| [1, , 3, 43, 5]    |",
        "| [5, , 32, 4, 15]   |",
        "| [16, , 3, 4, 5, 6] |",
        "|                    |",
        "| [3, ]              |",
        "+--------------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_nested_array_float() {
    let path = basic_path("nested_array_float.orc");
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+------------+",
        "| value      |",
        "+------------+",
        "| [1.0, 3.0] |",
        "| [, 2.0]    |",
        "+------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_nested_array_struct() {
    let path = basic_path("nested_array_struct.orc");
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+------------------------------------------------+",
        "| value                                          |",
        "+------------------------------------------------+",
        "| [{a: 1.0, b: 1, c: 01}, {a: 2.0, b: 2, c: 02}] |",
        "| [, {a: 3.0, b: 3, c: 03}]                      |",
        "+------------------------------------------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_nested_map_struct() {
    let path = basic_path("nested_map_struct.orc");
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+--------------------------------------------------------+",
        "| value                                                  |",
        "+--------------------------------------------------------+",
        "| {01: {a: 1.0, b: 1, c: 01}, 02: {a: 2.0, b: 1, c: 02}} |",
        "|                                                        |",
        "| {03: {a: 3.0, b: 3, c: 03}, 04: {a: 4.0, b: 4, c: 04}} |",
        "+--------------------------------------------------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_nested_map() {
    let path = basic_path("nested_map.orc");
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+--------------------------+",
        "| map                      |",
        "+--------------------------+",
        "| {zero: 0, one: 1}        |",
        "|                          |",
        "| {two: 2, tree: 3}        |",
        "| {one: 1, two: 2, nill: } |",
        "+--------------------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_0() {
    let path = basic_path("test.orc");
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+",
        "| a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple | tinyint_simple |",
        "+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+",
        "| 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  | -1             |",
        "| 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |                |",
        "|     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  | 1              |",
        "| 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  | 127            |",
        "| 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  | -127           |",
        "+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn basic_test_with_range() {
    let path = basic_path("test.orc");
    let reader = new_arrow_reader_range(&path, 0..2000);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(5, batch[0].column(0).len());
}

#[test]
pub fn basic_test_with_range_without_data() {
    let path = basic_path("test.orc");
    let reader = new_arrow_reader_range(&path, 100..2000);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(0, batch.len());
}

#[cfg(feature = "async")]
#[tokio::test]
pub async fn async_basic_test_with_range() {
    let path = basic_path("test.orc");
    let reader = new_arrow_stream_reader_range(&path, 0..2000).await;
    let batch = reader.try_collect::<Vec<_>>().await.unwrap();

    assert_eq!(5, batch[0].column(0).len());
}

#[cfg(feature = "async")]
#[tokio::test]
pub async fn async_basic_test_with_range_without_data() {
    let path = basic_path("test.orc");
    let reader = new_arrow_stream_reader_range(&path, 100..2000).await;
    let batch = reader.try_collect::<Vec<_>>().await.unwrap();

    assert_eq!(0, batch.len());
}

#[cfg(feature = "async")]
#[tokio::test]
pub async fn async_basic_test_0() {
    let path = basic_path("test.orc");
    let reader = new_arrow_stream_reader_root(&path).await;
    let batch = reader.try_collect::<Vec<_>>().await.unwrap();

    let expected = [
        "+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+",
        "| a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple | tinyint_simple |",
        "+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+",
        "| 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  | -1             |",
        "| 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |                |",
        "|     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  | 1              |",
        "| 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  | 127            |",
        "| 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  | -127           |",
        "+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn v0_file_test() {
    let path = basic_path("demo-11-zlib.orc");
    let reader = new_arrow_reader_root(&path);
    let expected_row_count = reader.total_row_count();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(expected_row_count as usize, total_rows);
}

#[test]
pub fn v1_file_test() {
    let path = basic_path("demo-12-zlib.orc");
    let reader = new_arrow_reader_root(&path);
    let expected_row_count = reader.total_row_count();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(expected_row_count as usize, total_rows);
}

#[cfg(feature = "async")]
#[tokio::test]
pub async fn v0_file_test_async() {
    let path = basic_path("demo-11-zlib.orc");
    let reader = new_arrow_stream_reader_root(&path).await;
    let batches = reader.try_collect::<Vec<_>>().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(1_920_800, total_rows);
}

#[test]
pub fn alltypes_test() {
    let compressions = ["none", "snappy", "zlib", "lzo", "zstd", "lz4"];
    for compression in compressions {
        let path = basic_path(&format!("alltypes.{compression}.orc"));
        let reader = new_arrow_reader_root(&path);
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

        let expected = [
            "+---------+------+--------+-------------+----------------------+------------+----------------+------------------+--------------------------+----------+------------+",
            "| boolean | int8 | int16  | int32       | int64                | float32    | float64        | decimal          | binary                   | utf8     | date32     |",
            "+---------+------+--------+-------------+----------------------+------------+----------------+------------------+--------------------------+----------+------------+",
            "|         |      |        |             |                      |            |                |                  |                          |          |            |",
            "| true    | 0    | 0      | 0           | 0                    | 0.0        | 0.0            | 0.00000          |                          |          | 1970-01-01 |",
            "| false   | 1    | 1      | 1           | 1                    | 1.0        | 1.0            | 1.00000          | 61                       | a        | 1970-01-02 |",
            "| false   | -1   | -1     | -1          | -1                   | -1.0       | -1.0           | -1.00000         | 20                       |          | 1969-12-31 |",
            "| true    | 127  | 32767  | 2147483647  | 9223372036854775807  | inf        | inf            | 123456789.12345  | 656e636f6465             | encode   | 9999-12-31 |",
            "| true    | -128 | -32768 | -2147483648 | -9223372036854775808 | -inf       | -inf           | -999999999.99999 | 6465636f6465             | decode   | 1582-10-15 |",
            "| true    | 50   | 50     | 50          | 50                   | 3.1415927  | 3.14159265359  | -31256.12300     | e5a4a7e7868ae5928ce5a58f | 大熊和奏 | 1582-10-16 |",
            "| true    | 51   | 51     | 51          | 51                   | -3.1415927 | -3.14159265359 | 1241000.00000    | e69689e897a4e69cb1e5a48f | 斉藤朱夏 | 2000-01-01 |",
            "| true    | 52   | 52     | 52          | 52                   | 1.1        | 1.1            | 1.10000          | e988b4e58e9fe5b88ce5ae9f | 鈴原希実 | 3000-12-31 |",
            "| false   | 53   | 53     | 53          | 53                   | -1.1       | -1.1           | 0.99999          | f09fa494                 | 🤔       | 1900-01-01 |",
            "|         |      |        |             |                      |            |                |                  |                          |          |            |",
            "+---------+------+--------+-------------+----------------------+------------+----------------+------------------+--------------------------+----------+------------+",
        ];
        assert_batches_eq(&batches, &expected);
    }
}

#[test]
pub fn timestamps_test() {
    let path = basic_path("pyarrow_timestamps.orc");
    let reader = new_arrow_reader_root(&path);
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = [
        "+---------------------+----------------------+",
        "| timestamp_notz      | timestamp_utc        |",
        "+---------------------+----------------------+",
        "|                     |                      |",
        "| 1970-01-01T00:00:00 | 1970-01-01T00:00:00Z |",
        "| 1970-01-02T23:59:59 | 1970-01-02T23:59:59Z |",
        "| 1969-12-31T23:59:59 | 1969-12-31T23:59:59Z |",
        "| 2262-04-11T11:47:16 | 2262-04-11T11:47:16Z |",
        "| 2001-04-13T02:14:00 | 2001-04-13T02:14:00Z |",
        "| 2000-01-01T23:10:10 | 2000-01-01T23:10:10Z |",
        "| 1900-01-01T14:25:14 | 1900-01-01T14:25:14Z |",
        "+---------------------+----------------------+",
    ];
    assert_batches_eq(&batches, &expected);

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_notz",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            "timestamp_utc",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]));
    assert_eq!(schema, expected_schema);
    for batch in &batches {
        assert_eq!(batch.schema(), expected_schema);
    }
}

#[test]
pub fn overflowing_timestamps_test() {
    let path = basic_path("overflowing_timestamps.orc");
    let reader = new_arrow_reader_root(&path);
    assert!(reader.collect::<Result<Vec<_>, _>>().is_err());
}

#[test]
pub fn second_timestamps_test() {
    custom_precision_timestamps_test(TimeUnit::Second)
}

#[test]
pub fn millisecond_timestamps_test() {
    custom_precision_timestamps_test(TimeUnit::Millisecond)
}

#[test]
pub fn microsecond_timestamps_test() {
    custom_precision_timestamps_test(TimeUnit::Microsecond)
}

fn custom_precision_timestamps_test(time_unit: TimeUnit) {
    let path = basic_path("overflowing_timestamps.orc");
    let f = File::open(path).expect("no file found");
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(time_unit, None), true),
        ])))
        .build();
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let expected = [
        "+----+---------------------+",
        "| id | ts                  |",
        "+----+---------------------+",
        "| 1  | 1970-05-23T21:21:18 |",
        "| 2  | 0001-01-01T00:00:00 |",
        "| 3  | 1970-05-23T21:21:18 |",
        "+----+---------------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

#[test]
pub fn decimal128_timestamps_test() {
    let path = basic_path("overflowing_timestamps.orc");
    let f = File::open(path).expect("no file found");
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new(
                "ts",
                DataType::Decimal128(Decimal128Type::MAX_PRECISION, 9),
                true,
            ),
        ])))
        .build();
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();
    println!("{:?}", batch[0].column(1));
    let expected = [
        "+----+------------------------+",
        "| id | ts                     |",
        "+----+------------------------+",
        "| 1  | 12345678.000000000     |",
        "| 2  | -62135596800.000000000 |",
        "| 3  | 12345678.000000000     |",
        "+----+------------------------+",
    ];
    assert_batches_eq(&batch, &expected);
}

// From https://github.com/apache/arrow-rs/blob/7705acad845e8b2a366a08640f7acb4033ed7049/arrow-flight/src/sql/metadata/mod.rs#L67-L75
pub fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
    let formatted = pretty::pretty_format_batches(batches).unwrap().to_string();
    let actual_lines: Vec<_> = formatted.trim().lines().collect();
    assert_eq!(
        &actual_lines, expected_lines,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected_lines, actual_lines
    );
}
