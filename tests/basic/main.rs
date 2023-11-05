use std::fs::File;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion_orc::arrow_reader::{ArrowReader, Cursor};
use datafusion_orc::async_arrow_reader::ArrowStreamReader;
use datafusion_orc::reader::Reader;
use futures_util::TryStreamExt;

use crate::misc::{LONG_BOOL_EXPECTED, LONG_STRING_DICT_EXPECTED, LONG_STRING_EXPECTED};

mod misc;

fn new_arrow_reader(path: &str, fields: &[&str]) -> ArrowReader<File> {
    let f = File::open(path).expect("no file found");

    let reader = Reader::new(f).unwrap();

    let cursor = Cursor::new(reader, fields).unwrap();

    ArrowReader::new(cursor, None)
}

async fn new_arrow_stream_reader_root(path: &str) -> ArrowStreamReader<tokio::fs::File> {
    let f = tokio::fs::File::open(path).await.unwrap();

    let reader = Reader::new_async(f).await.unwrap();

    let cursor = Cursor::root(reader).unwrap();

    ArrowStreamReader::new(cursor, None)
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
    let reader = new_arrow_reader(&path, &["timestamp_simple", "date_simple"]);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

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
    let reader = new_arrow_reader_root(&path);
    let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let expected = r#"+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+
| a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple | tinyint_simple |
+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+
| 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  | -1             |
| 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |                |
|     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  | 1              |
| 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  | 127            |
| 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  | -127           |
+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+"#;
    assert_eq!(
        expected,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[tokio::test]
pub async fn async_basic_test_0() {
    let path = basic_path("test.orc");
    let reader = new_arrow_stream_reader_root(&path).await;
    let batch = reader.try_collect::<Vec<_>>().await.unwrap();

    let expected = r#"+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+
| a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple | tinyint_simple |
+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+
| 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  | -1             |
| 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |                |
|     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  | 1              |
| 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  | 127            |
| 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  | -127           |
+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+----------------+"#;
    assert_eq!(
        expected,
        pretty::pretty_format_batches(&batch).unwrap().to_string()
    )
}

#[test]
pub fn alltypes_test() {
    let compressions = ["none", "snappy", "zlib", "lzo", "zstd", "lz4"];
    for compression in compressions {
        let path = basic_path(&format!("alltypes.{compression}.orc"));
        let reader = new_arrow_reader_root(&path);
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

        let expected = [
        "+---------+--------+-------------+----------------------+------------+----------------+--------------------------+----------+------------+",
        "| boolean | int16  | int32       | int64                | float32    | float64        | binary                   | utf8     | date32     |",
        "+---------+--------+-------------+----------------------+------------+----------------+--------------------------+----------+------------+",
        "|         |        |             |                      |            |                |                          |          |            |",
        "| true    | 0      | 0           | 0                    | 0.0        | 0.0            |                          |          | 1970-01-01 |",
        "| false   | 1      | 1           | 1                    | 1.0        | 1.0            | 61                       | a        | 1970-01-02 |",
        "| false   | -1     | -1          | -1                   | -1.0       | -1.0           | 20                       |          | 1969-12-31 |",
        "| true    | 32767  | 2147483647  | 9223372036854775807  | inf        | inf            | 656e636f6465             | encode   | 9999-12-31 |",
        "| true    | -32768 | -2147483648 | -9223372036854775808 | -inf       | -inf           | 6465636f6465             | decode   | 1582-10-15 |",
        "| true    | 50     | 50          | 50                   | 3.1415927  | 3.14159265359  | e5a4a7e7868ae5928ce5a58f | 大熊和奏 | 1582-10-16 |",
        "| true    | 51     | 51          | 51                   | -3.1415927 | -3.14159265359 | e69689e897a4e69cb1e5a48f | 斉藤朱夏 | 2000-01-01 |",
        "| true    | 52     | 52          | 52                   | 1.1        | 1.1            | e988b4e58e9fe5b88ce5ae9f | 鈴原希実 | 3000-12-31 |",
        "| false   | 53     | 53          | 53                   | -1.1       | -1.1           | f09fa494                 | 🤔       | 1900-01-01 |",
        "|         |        |             |                      |            |                |                          |          |            |",
        "+---------+--------+-------------+----------------------+------------+----------------+--------------------------+----------+------------+",
    ];
        assert_batches_eq(&batches, &expected);
    }
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
