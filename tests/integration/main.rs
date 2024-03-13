#![allow(non_snake_case)]

/// Tests against `.orc` and `.jsn.gz` in the official test suite (`orc/examples/`)
use std::fs::File;
use std::io::{Cursor, Read};
use std::sync::Arc;

use pretty_assertions::assert_eq;

use arrow::array::{AsArray, ListArray, PrimitiveArray};
use arrow::datatypes::{DataType, Field, Schema, UInt8Type};
use arrow::record_batch::RecordBatch;
use arrow_json::{writer::LineDelimited, WriterBuilder};
use datafusion_orc::arrow_reader::ArrowReaderBuilder;

fn normalize_json(json: &[u8]) -> String {
    json.split(|c| *c == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_slice::<serde_json::Value>(line).expect("Could not parse line")
        })
        .map(|v| serde_json::to_string_pretty(&v).expect("Could not re-serialize line"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Checks parsing a `.orc` file produces the expected result in the `.jsn.gz` path
fn test_expected_file(name: &str) {
    let dir = env!("CARGO_MANIFEST_DIR");
    let orc_path = format!("{}/tests/integration/data/{}.orc", dir, name);
    let jsn_gz_path = format!("{}/tests/integration/data/expected/{}.jsn.gz", dir, name);
    let f = File::open(orc_path).expect("Could not open .orc");
    let builder = ArrowReaderBuilder::try_new(f).unwrap();
    let orc_reader = builder.build();

    // Read .orc into JSON objects
    let mut json_writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(Cursor::new(Vec::new()));
    let batches: Vec<RecordBatch> = orc_reader.collect::<Result<Vec<_>, _>>().unwrap();
    batches
        .into_iter()
        .map(binaries_to_uint8_list)
        .for_each(|batch| {
            json_writer
                .write(&batch)
                .expect("Could not serialize row from .orc to JSON string")
        });
    let lines = normalize_json(&json_writer.into_inner().into_inner());

    // Read expected JSON objects
    let mut expected_json = String::new();
    flate2::read::GzDecoder::new(&File::open(jsn_gz_path).expect("Could not open .jsn.gz"))
        .read_to_string(&mut expected_json)
        .expect("Could not read .jsn.gz");

    // Reencode the input
    let expected_lines = normalize_json(expected_json.as_bytes());

    if lines.len() < 1000 {
        assert_eq!(lines, expected_lines);
    } else {
        // pretty_assertions consumes too much RAM and CPU on large diffs,
        // and it's unreadable anyway
        assert_eq!(lines[0..1000], expected_lines[0..1000]);
        assert!(lines == expected_lines);
    }
}

/// Converts binary columns into lists of UInt8 so they can be JSON-encoded
fn binaries_to_uint8_list(batch: RecordBatch) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(
            batch
                .schema()
                .fields
                .into_iter()
                .map(|field| {
                    Field::new(
                        field.name(),
                        match field.data_type() {
                            DataType::Binary => DataType::List(Arc::new(Field::new(
                                "value",
                                DataType::UInt8,
                                false,
                            ))),
                            DataType::LargeBinary => DataType::LargeList(Arc::new(Field::new(
                                "value",
                                DataType::UInt8,
                                false,
                            ))),
                            data_type => data_type.clone(),
                        },
                        field.is_nullable(),
                    )
                })
                .collect::<Vec<_>>(),
        )),
        batch
            .columns()
            .iter()
            .map(|array| match array.as_binary_opt() {
                Some(array) => {
                    let (offsets, values, nulls) = array.clone().into_parts();
                    ListArray::try_new(
                        Arc::new(Field::new("value", DataType::UInt8, false)),
                        offsets,
                        Arc::new(PrimitiveArray::<UInt8Type>::new(values.into(), None)),
                        nulls,
                    )
                    .map(Arc::new)
                    .expect("Could not create ListArray")
                }
                None => array.clone(),
            })
            .collect(),
    )
    .expect("Could not rebuild RecordBatch")
}

#[test]
fn columnProjection() {
    test_expected_file("TestOrcFile.columnProjection");
}
#[test]
fn emptyFile() {
    test_expected_file("TestOrcFile.emptyFile");
}
#[test]
#[ignore] // TODO: Why?
fn metaData() {
    test_expected_file("TestOrcFile.metaData");
}
#[test]
#[ignore] // TODO: Why?
fn test1() {
    test_expected_file("TestOrcFile.test1");
}
#[test]
#[ignore] // TODO: Incorrect timezone + representation differs
fn testDate1900() {
    test_expected_file("TestOrcFile.testDate1900");
}
#[test]
#[ignore] // TODO: Incorrect timezone + representation differs
fn testDate2038() {
    test_expected_file("TestOrcFile.testDate2038");
}
#[test]
fn testMemoryManagementV11() {
    test_expected_file("TestOrcFile.testMemoryManagementV11");
}
#[test]
fn testMemoryManagementV12() {
    test_expected_file("TestOrcFile.testMemoryManagementV12");
}
#[test]
fn testPredicatePushdown() {
    test_expected_file("TestOrcFile.testPredicatePushdown");
}
#[test]
#[ignore] // TODO: Why?
fn testSeek() {
    test_expected_file("TestOrcFile.testSeek");
}
#[test]
fn testSnappy() {
    test_expected_file("TestOrcFile.testSnappy");
}
#[test]
fn testStringAndBinaryStatistics() {
    test_expected_file("TestOrcFile.testStringAndBinaryStatistics");
}
#[test]
fn testStripeLevelStats() {
    test_expected_file("TestOrcFile.testStripeLevelStats");
}
#[test]
#[ignore] // TODO: Non-struct root type are not supported yet
fn testTimestamp() {
    test_expected_file("TestOrcFile.testTimestamp");
}
#[test]
#[ignore] // TODO: Unions are not supported yet
fn testUnionAndTimestamp() {
    test_expected_file("TestOrcFile.testUnionAndTimestamp");
}
#[test]
fn testWithoutIndex() {
    test_expected_file("TestOrcFile.testWithoutIndex");
}
#[test]
fn testLz4() {
    test_expected_file("TestVectorOrcFile.testLz4");
}
#[test]
fn testLzo() {
    test_expected_file("TestVectorOrcFile.testLzo");
}
#[test]
#[ignore] // TODO: not yet implemented
fn decimal() {
    test_expected_file("decimal");
}
#[test]
#[ignore] // TODO: Too slow
fn zlib() {
    test_expected_file("demo-12-zlib");
}
#[test]
#[ignore] // TODO: Why?
fn nulls_at_end_snappy() {
    test_expected_file("nulls-at-end-snappy");
}
#[test]
#[ignore] // TODO: Why?
fn orc_11_format() {
    test_expected_file("orc-file-11-format");
}
#[test]
fn orc_index_int_string() {
    test_expected_file("orc_index_int_string");
}
#[test]
#[ignore] // TODO: not yet implemented
fn orc_split_elim() {
    test_expected_file("orc_split_elim");
}
#[test]
#[ignore] // TODO: not yet implemented
fn orc_split_elim_cpp() {
    test_expected_file("orc_split_elim_cpp");
}
#[test]
#[ignore] // TODO: not yet implemented
fn orc_split_elim_new() {
    test_expected_file("orc_split_elim_new");
}
#[test]
#[ignore] // TODO: not yet implemented
fn over1k_bloom() {
    test_expected_file("over1k_bloom");
}
