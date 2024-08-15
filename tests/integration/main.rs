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

/// Tests ORC files from the official test suite (`orc/examples/`) against Arrow feather
/// expected data sourced by reading the ORC files with PyArrow and persisting as feather.
use std::fs::File;

use arrow::{
    array::{Array, AsArray},
    compute::concat_batches,
    datatypes::TimestampNanosecondType,
    ipc::reader::FileReader,
    record_batch::{RecordBatch, RecordBatchReader},
};
use pretty_assertions::assert_eq;

use orc_rust::arrow_reader::ArrowReaderBuilder;

fn read_orc_file(name: &str) -> RecordBatch {
    let path = format!(
        "{}/tests/integration/data/{}.orc",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let f = File::open(path).unwrap();
    let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Gather all record batches into single one for easier comparison
    concat_batches(&schema, batches.iter()).unwrap()
}

fn read_feather_file(name: &str) -> RecordBatch {
    let feather_path = format!(
        "{}/tests/integration/data/expected_arrow/{}.feather",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let f = File::open(feather_path).unwrap();
    let reader = FileReader::try_new(f, None).unwrap();
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Gather all record batches into single one for easier comparison
    concat_batches(&schema, batches.iter()).unwrap()
}

/// Checks specific `.orc` file against corresponding expected feather file
fn test_expected_file(name: &str) {
    let actual_batch = read_orc_file(name);
    let expected_batch = read_feather_file(name);
    assert_eq!(actual_batch, expected_batch);
}

/// Similar to test_expected_file but compares the pretty formatted RecordBatches.
/// Useful when checking equality of values but not exact equality (e.g. representation
/// for UnionArrays can differ internally but logically represent the same values)
fn test_expected_file_formatted(name: &str) {
    let actual_batch = read_orc_file(name);
    let expected_batch = read_feather_file(name);

    let actual = arrow::util::pretty::pretty_format_batches(&[actual_batch])
        .unwrap()
        .to_string();
    let expected = arrow::util::pretty::pretty_format_batches(&[expected_batch])
        .unwrap()
        .to_string();
    let actual_lines = actual.trim().lines().collect::<Vec<_>>();
    let expected_lines = expected.trim().lines().collect::<Vec<_>>();
    assert_eq!(&actual_lines, &expected_lines);
}

#[test]
fn column_projection() {
    test_expected_file("TestOrcFile.columnProjection");
}

#[test]
// TODO: arrow-ipc reader can't read invalid custom_metadata
//       from the expected feather file:
//       https://github.com/apache/arrow-rs/issues/5547
#[ignore]
fn meta_data() {
    test_expected_file("TestOrcFile.metaData");
}

#[test]
fn test1() {
    // Compare formatted because Map key/value field names differs from PyArrow
    test_expected_file_formatted("TestOrcFile.test1");
}

#[test]
fn empty_file() {
    // Compare formatted because Map key/value field names differs from PyArrow
    test_expected_file_formatted("TestOrcFile.emptyFile");
}

#[test]
fn test_date_1900() {
    test_expected_file("TestOrcFile.testDate1900");
}

#[test]
fn test_date_1900_not_null() {
    // Don't use read_orc_file() because it concatenate batches, which would detect
    // there are no nulls and remove the NullBuffer, making this test useless
    let path = format!(
        "{}/tests/integration/data/TestOrcFile.testDate1900.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();
    let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    for batch in batches {
        assert!(batch.columns()[0]
            .as_primitive_opt::<TimestampNanosecondType>()
            .unwrap()
            .nulls()
            .is_none());
    }
}

#[test]
#[ignore]
// TODO: pending https://github.com/chronotope/chrono-tz/issues/155
fn test_date_2038() {
    test_expected_file("TestOrcFile.testDate2038");
}

#[test]
fn test_memory_management_v11() {
    test_expected_file("TestOrcFile.testMemoryManagementV11");
}

#[test]
fn test_memory_management_v12() {
    test_expected_file("TestOrcFile.testMemoryManagementV12");
}

#[test]
fn test_predicate_pushdown() {
    test_expected_file("TestOrcFile.testPredicatePushdown");
}

#[test]
fn test_seek() {
    // Compare formatted because Map key/value field names differs from PyArrow
    test_expected_file_formatted("TestOrcFile.testSeek");
}

#[test]
fn test_snappy() {
    test_expected_file("TestOrcFile.testSnappy");
}

#[test]
fn test_string_and_binary_statistics() {
    test_expected_file("TestOrcFile.testStringAndBinaryStatistics");
}

#[test]
fn test_stripe_level_stats() {
    test_expected_file("TestOrcFile.testStripeLevelStats");
}

#[test]
#[ignore] // TODO: Non-struct root type are not supported yet
fn test_timestamp() {
    test_expected_file("TestOrcFile.testTimestamp");
}

#[test]
fn test_union_and_timestamp() {
    // Compare formatted because internal Union representation can differ
    // even if it represents the same logical values
    test_expected_file_formatted("TestOrcFile.testUnionAndTimestamp");
}

#[test]
fn test_without_index() {
    test_expected_file("TestOrcFile.testWithoutIndex");
}

#[test]
fn test_lz4() {
    test_expected_file("TestVectorOrcFile.testLz4");
}

#[test]
fn test_lzo() {
    test_expected_file("TestVectorOrcFile.testLzo");
}

#[test]
fn decimal() {
    test_expected_file("decimal");
}

#[test]
fn zlib() {
    test_expected_file("demo-12-zlib");
}

#[test]
fn nulls_at_end_snappy() {
    test_expected_file("nulls-at-end-snappy");
}

#[test]
#[ignore]
// TODO: investigate why this fails (zero metadata length how?)
//       because of difference with ORC 0.11 vs 0.12?
fn orc_11_format() {
    test_expected_file("orc-file-11-format");
}

#[test]
fn orc_index_int_string() {
    test_expected_file("orc_index_int_string");
}

#[test]
#[ignore]
// TODO: how to handle DECIMAL(0, 0) type? Arrow expects non-zero precision
fn orc_split_elim() {
    test_expected_file("orc_split_elim");
}

#[test]
fn orc_split_elim_cpp() {
    test_expected_file("orc_split_elim_cpp");
}

#[test]
fn orc_split_elim_new() {
    test_expected_file("orc_split_elim_new");
}

#[test]
fn over1k_bloom() {
    test_expected_file("over1k_bloom");
}
