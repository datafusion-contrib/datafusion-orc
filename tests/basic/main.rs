#![feature(iterator_try_collect)]

use std::fs::File;

use arrow::util::pretty;
use orc_rust::reader::arrow::ArrowReader;
use orc_rust::reader::{Cursor, Reader};

use crate::misc::{LONG_BOOL_EXPECTED, LONG_STRING_DICT_EXPECTED, LONG_STRING_EXPECTED};

mod misc;

fn new_arrow_reader(path: &str, fields: &[&str]) -> ArrowReader<File> {
    let f = File::open(path).expect("no file found");

    let reader = Reader::new(f).unwrap();

    let cursor = Cursor::new(reader, fields).unwrap();

    ArrowReader::new(cursor, None)
}

fn new_arrow_reader_root(path: &str) -> ArrowReader<File> {
    let f = File::open(path).expect("no file found");

    let reader = Reader::new(f).unwrap();

    let cursor = Cursor::root(reader).unwrap();

    ArrowReader::new(cursor, None)
}

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{}/tests/basic/data/{}", dir, path)
}

#[test]
pub fn test_read_long_bool() {
    let path = basic_path("long_bool.orc");
    let mut reader = new_arrow_reader(&path, &["long"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    assert_eq!(32, batch[0].column(0).len());
    assert_eq!(
        LONG_BOOL_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_long_bool_gzip() {
    let path = basic_path("long_bool_gzip.orc");
    let mut reader = new_arrow_reader(&path, &["long"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    assert_eq!(32, batch[0].column(0).len());
    assert_eq!(
        LONG_BOOL_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_long_string() {
    let path = basic_path("string_long.orc");
    let mut reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    assert_eq!(64, batch[0].column(0).len());

    assert_eq!(
        LONG_STRING_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_string_dirt() {
    let path = basic_path("string_dict.orc");
    let mut reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    assert_eq!(64, batch[0].column(0).len());
    assert_eq!(
        LONG_STRING_DICT_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_string_dirt_gzip() {
    let path = basic_path("string_dict_gzip.orc");
    let mut reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    assert_eq!(64, batch[0].column(0).len());
    assert_eq!(
        LONG_STRING_DICT_EXPECTED,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn test_read_string_long_long() {
    let path = basic_path("string_long_long.orc");
    let mut reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    assert_eq!(8192, batch[0].column(0).len());
    assert_eq!(10_000 - 8192, batch[1].column(0).len());
}

#[test]
pub fn test_read_f32_long_long_gzip() {
    let path = basic_path("f32_long_long_gzip.orc");
    let mut reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    let total: usize = batch.iter().map(|c| c.column(0).len()).sum();

    assert_eq!(total, 1_000_000);
}

#[test]
pub fn test_read_string_long_long_gzip() {
    let path = basic_path("string_long_long_gzip.orc");
    let mut reader = new_arrow_reader(&path, &["dict"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    assert_eq!(8192, batch[0].column(0).len());
    assert_eq!(10_000 - 8192, batch[1].column(0).len());
}

#[test]
pub fn basic_test() {
    let path = basic_path("test.orc");
    let mut reader = new_arrow_reader(&path, &["a", "b", "str_direct", "d", "e", "f"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    let expected = r#"+-----+-------+------------+-----+-----+-------+
| a   | b     | str_direct | d   | e   | f     |
+-----+-------+------------+-----+-----+-------+
| 1.0 | true  | a          | a   | ddd | aaaaa |
| 2.0 | false | cccccc     | bb  | cc  | bbbbb |
|     |       |            |     |     |       |
| 4.0 | true  | ddd        | ccc | bb  | ccccc |
| 5.0 | false | ee         | ddd | a   | ddddd |
+-----+-------+------------+-----+-----+-------+"#;

    assert_eq!(
        expected,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn basic_test_2() {
    let path = basic_path("test.orc");
    let mut reader = new_arrow_reader(
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
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    let expected = r#"+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+
| int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease |
+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+
| 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         |
| 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          |
|                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           |
| 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            |
| 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             |
+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+"#;

    assert_eq!(
        expected,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn basic_test_3() {
    let path = basic_path("test.orc");
    let mut reader = new_arrow_reader(&path, &["timestamp_simple", "date_simple"]);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    let expected = r#"+----------------------------+-------------+
| timestamp_simple           | date_simple |
+----------------------------+-------------+
| 2023-04-01T20:15:30.002    | 2023-04-01  |
| 2021-08-22T07:26:44.525777 | 2023-03-01  |
| 2023-01-01T00:00:00        | 2023-01-01  |
| 2023-02-01T00:00:00        | 2023-02-01  |
| 2023-03-01T00:00:00        | 2023-03-01  |
+----------------------------+-------------+"#;
    assert_eq!(
        expected,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn basic_test_0() {
    let path = basic_path("test.orc");
    let mut reader = new_arrow_reader_root(&path);
    let batch = reader.try_collect::<Vec<_>>().unwrap();

    let expected = r#"+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+
| a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple |
+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+
| 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  |
| 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |
|     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  |
| 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  |
| 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  |
+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+"#;
    assert_eq!(
        expected,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}
