/// Tests ORC files from the official test suite (`orc/examples/`) against Arrow feather
/// expected data sourced by reading the ORC files with PyArrow and persisting as feather.
use std::{fs::File, sync::Arc};

use arrow::{
    array::{AsArray, MapArray, StructArray},
    compute::concat_batches,
    datatypes::{DataType, Field, Fields, Schema},
    ipc::reader::FileReader,
    record_batch::{RecordBatch, RecordBatchReader},
};
use pretty_assertions::assert_eq;

use datafusion_orc::arrow_reader::ArrowReaderBuilder;

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

#[test]
fn column_projection() {
    test_expected_file("TestOrcFile.columnProjection");
}

#[test]
#[ignore] // TODO: nullable difference
fn empty_file() {
    test_expected_file("TestOrcFile.emptyFile");
}

#[test]
#[ignore] // TODO: Why?
fn meta_data() {
    test_expected_file("TestOrcFile.metaData");
}

#[test]
fn test1() {
    let actual_batch = read_orc_file("TestOrcFile.test1");
    let expected_batch = read_feather_file("TestOrcFile.test1");

    // Super ugly code to rename the "key" and "value" in PyArrow MapArray to
    // "keys" and "values" which arrow-rs does
    // TODO: surely there is some better way to handle this?
    let mut fields = expected_batch.schema().fields[..11].to_vec();
    let entries_fields: Fields = vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new(
            "values",
            DataType::Struct(
                vec![
                    Field::new("int1", DataType::Int32, true),
                    Field::new("string1", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
    ]
    .into();
    let entries_field = Arc::new(Field::new_struct("entries", entries_fields.clone(), false));
    let map_field = Field::new("map", DataType::Map(entries_field.clone(), false), true);
    fields.push(Arc::new(map_field));
    let schema = Arc::new(Schema::new(fields));
    let mut columns = expected_batch.columns()[..11].to_vec();
    // Have to destruct the MapArray inorder to reconstruct with correct names for
    // MapArray struct children
    let map_array = expected_batch.column(11).as_map().clone();
    let (_, offsets, entries, nulls, ordered) = map_array.into_parts();
    let entries = {
        let (_, arrays, nulls) = entries.into_parts();
        StructArray::new(entries_fields, arrays, nulls)
    };
    let map_array = MapArray::new(entries_field, offsets, entries, nulls, ordered);
    columns.push(Arc::new(map_array));
    let expected_batch = RecordBatch::try_new(schema, columns).unwrap();

    assert_eq!(actual_batch, expected_batch);
}

#[test]
#[ignore] // TODO: Incorrect timezone + representation differs
fn test_date_1900() {
    test_expected_file("TestOrcFile.testDate1900");
}

#[test]
#[ignore] // TODO: Incorrect timezone + representation differs
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
#[ignore] // TODO: Why?
fn test_seek() {
    test_expected_file("TestOrcFile.testSeek");
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
#[ignore] // TODO: Unions are not supported yet
fn test_union_and_timestamp() {
    test_expected_file("TestOrcFile.testUnionAndTimestamp");
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
fn orc_split_elim_cpp() {
    test_expected_file("orc_split_elim_cpp");
}

#[test]
fn orc_split_elim_new() {
    test_expected_file("orc_split_elim_new");
}

#[test]
#[ignore] // TODO: not yet implemented
fn over1k_bloom() {
    test_expected_file("over1k_bloom");
}
