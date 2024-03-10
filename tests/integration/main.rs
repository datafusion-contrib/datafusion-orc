#![allow(non_snake_case)]

/// Tests against `.orc` and `.jsn.gz` in the official test suite (`orc/examples/`)
use std::fs::File;
use std::io::Read;

use pretty_assertions::assert_eq;

use arrow::array::StructArray;
use arrow::record_batch::RecordBatch;
use datafusion_orc::arrow_reader::ArrowReaderBuilder;

/// Checks parsing a `.orc` file produces the expected result in the `.jsn.gz` path
fn test_expected_file(name: &str) {
    let dir = env!("CARGO_MANIFEST_DIR");
    let orc_path = format!("{}/tests/integration/data/{}.orc", dir, name);
    let jsn_gz_path = format!("{}/tests/integration/data/expected/{}.jsn.gz", dir, name);
    let f = File::open(orc_path).expect("Could not open .orc");
    let builder = ArrowReaderBuilder::try_new(f).unwrap();
    let orc_reader = builder.build();
    let total_row_count = orc_reader.total_row_count();

    // Read .orc into JSON objects
    let batches: Vec<RecordBatch> = orc_reader.collect::<Result<Vec<_>, _>>().unwrap();
    let objects: Vec<serde_json::Value> = batches
        .into_iter()
        .map(|batch| -> StructArray { batch.into() })
        .flat_map(|array| {
            arrow_json::writer::array_to_json_array(&array)
                .expect("Could not serialize convert row from .orc to JSON value")
        })
        .collect();

    // Read expected JSON objects
    let mut expected_json = String::new();
    flate2::read::GzDecoder::new(&File::open(jsn_gz_path).expect("Could not open .jsn.gz"))
        .read_to_string(&mut expected_json)
        .expect("Could not read .jsn.gz");

    let objects_count = objects.len();

    // Reencode the input to normalize it
    let expected_lines = expected_json
        .split('\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("Could not parse line in .jsn.gz")
        })
        .map(|v| {
            serde_json::to_string_pretty(&v).expect("Could not re-serialize line from .jsn.gz")
        })
        .collect::<Vec<_>>()
        .join("\n");

    let lines = objects
        .into_iter()
        .map(|v| serde_json::to_string_pretty(&v).expect("Could not serialize row from .orc"))
        .collect::<Vec<_>>()
        .join("\n");

    if lines.len() < 1000 {
        assert_eq!(lines, expected_lines);
    } else {
        // pretty_assertions consumes too much RAM and CPU on large diffs,
        // and it's unreadable anyway
        assert_eq!(lines[0..1000], expected_lines[0..1000]);
        assert!(lines == expected_lines);
    }

    assert_eq!(total_row_count, objects_count as u64);
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
#[ignore] // Why?
fn metaData() {
    test_expected_file("TestOrcFile.metaData");
}
#[test]
#[ignore] // Why?
fn test1() {
    test_expected_file("TestOrcFile.test1");
}
#[test]
#[should_panic] // Incorrect timezone + representation differs
fn testDate1900() {
    test_expected_file("TestOrcFile.testDate1900");
}
#[test]
#[should_panic] // Incorrect timezone + representation differs
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
#[should_panic] // Why?
fn testSeek() {
    test_expected_file("TestOrcFile.testSeek");
}
#[test]
fn testSnappy() {
    test_expected_file("TestOrcFile.testSnappy");
}
#[test]
#[should_panic] // arrow_json does not support binaries
fn testStringAndBinaryStatistics() {
    test_expected_file("TestOrcFile.testStringAndBinaryStatistics");
}
#[test]
fn testStripeLevelStats() {
    test_expected_file("TestOrcFile.testStripeLevelStats");
}
#[test]
#[should_panic] // Non-struct root type are not supported yet
fn testTimestamp() {
    test_expected_file("TestOrcFile.testTimestamp");
}
#[test]
#[should_panic] // Unions are not supported yet
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
#[ignore] // Differs on representation of some Decimals
fn decimal() {
    test_expected_file("decimal");
}
#[test]
#[ignore] // Too slow
fn zlib() {
    test_expected_file("demo-12-zlib");
}
#[test]
#[ignore] // Why?
fn nulls_at_end_snappy() {
    test_expected_file("nulls-at-end-snappy");
}
#[test]
#[ignore] // Why?
fn orc_11_format() {
    test_expected_file("orc-file-11-format");
}
#[test]
fn orc_index_int_string() {
    test_expected_file("orc_index_int_string");
}
#[test]
#[should_panic] // not yet implemented
fn orc_split_elim() {
    test_expected_file("orc_split_elim");
}
#[test]
#[should_panic] // not yet implemented
fn orc_split_elim_cpp() {
    test_expected_file("orc_split_elim_cpp");
}
#[test]
#[should_panic] // not yet implemented
fn orc_split_elim_new() {
    test_expected_file("orc_split_elim_new");
}
#[test]
#[should_panic] // not yet implemented
fn over1k_bloom() {
    test_expected_file("over1k_bloom");
}
