// Copied from https://github.com/DataEngineeringLabs/orc-format/blob/416490db0214fc51d53289253c0ee91f7fc9bc17/src/proto.rs
// TODO(weny): Considers using the official proto file?
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntegerStatistics {
    #[prost(sint64, optional, tag = "1")]
    pub minimum: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "2")]
    pub maximum: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "3")]
    pub sum: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoubleStatistics {
    #[prost(double, optional, tag = "1")]
    pub minimum: ::core::option::Option<f64>,
    #[prost(double, optional, tag = "2")]
    pub maximum: ::core::option::Option<f64>,
    #[prost(double, optional, tag = "3")]
    pub sum: ::core::option::Option<f64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringStatistics {
    #[prost(string, optional, tag = "1")]
    pub minimum: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "2")]
    pub maximum: ::core::option::Option<::prost::alloc::string::String>,
    /// sum will store the total length of all strings in a stripe
    #[prost(sint64, optional, tag = "3")]
    pub sum: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BucketStatistics {
    #[prost(uint64, repeated, tag = "1")]
    pub count: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecimalStatistics {
    #[prost(string, optional, tag = "1")]
    pub minimum: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "2")]
    pub maximum: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "3")]
    pub sum: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DateStatistics {
    /// min,max values saved as days since epoch
    #[prost(sint32, optional, tag = "1")]
    pub minimum: ::core::option::Option<i32>,
    #[prost(sint32, optional, tag = "2")]
    pub maximum: ::core::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimestampStatistics {
    /// min,max values saved as milliseconds since epoch
    #[prost(sint64, optional, tag = "1")]
    pub minimum: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "2")]
    pub maximum: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryStatistics {
    /// sum will store the total binary blob length in a stripe
    #[prost(sint64, optional, tag = "1")]
    pub sum: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnStatistics {
    #[prost(uint64, optional, tag = "1")]
    pub number_of_values: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "2")]
    pub int_statistics: ::core::option::Option<IntegerStatistics>,
    #[prost(message, optional, tag = "3")]
    pub double_statistics: ::core::option::Option<DoubleStatistics>,
    #[prost(message, optional, tag = "4")]
    pub string_statistics: ::core::option::Option<StringStatistics>,
    #[prost(message, optional, tag = "5")]
    pub bucket_statistics: ::core::option::Option<BucketStatistics>,
    #[prost(message, optional, tag = "6")]
    pub decimal_statistics: ::core::option::Option<DecimalStatistics>,
    #[prost(message, optional, tag = "7")]
    pub date_statistics: ::core::option::Option<DateStatistics>,
    #[prost(message, optional, tag = "8")]
    pub binary_statistics: ::core::option::Option<BinaryStatistics>,
    #[prost(message, optional, tag = "9")]
    pub timestamp_statistics: ::core::option::Option<TimestampStatistics>,
    #[prost(bool, optional, tag = "10")]
    pub has_null: ::core::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowIndexEntry {
    #[prost(uint64, repeated, tag = "1")]
    pub positions: ::prost::alloc::vec::Vec<u64>,
    #[prost(message, optional, tag = "2")]
    pub statistics: ::core::option::Option<ColumnStatistics>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowIndex {
    #[prost(message, repeated, tag = "1")]
    pub entry: ::prost::alloc::vec::Vec<RowIndexEntry>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BloomFilter {
    #[prost(uint32, optional, tag = "1")]
    pub num_hash_functions: ::core::option::Option<u32>,
    #[prost(fixed64, repeated, packed = "false", tag = "2")]
    pub bitset: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BloomFilterIndex {
    #[prost(message, repeated, tag = "1")]
    pub bloom_filter: ::prost::alloc::vec::Vec<BloomFilter>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stream {
    #[prost(enumeration = "stream::Kind", optional, tag = "1")]
    pub kind: ::core::option::Option<i32>,
    #[prost(uint32, optional, tag = "2")]
    pub column: ::core::option::Option<u32>,
    #[prost(uint64, optional, tag = "3")]
    pub length: ::core::option::Option<u64>,
}
/// Nested message and enum types in `Stream`.
pub mod stream {
    /// if you add new index stream kinds, you need to make sure to update
    /// StreamName to ensure it is added to the stripe in the right area
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        Present = 0,
        Data = 1,
        Length = 2,
        DictionaryData = 3,
        DictionaryCount = 4,
        Secondary = 5,
        RowIndex = 6,
        BloomFilter = 7,
    }
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ColumnEncoding {
    #[prost(enumeration = "column_encoding::Kind", optional, tag = "1")]
    pub kind: ::core::option::Option<i32>,
    #[prost(uint32, optional, tag = "2")]
    pub dictionary_size: ::core::option::Option<u32>,
}
/// Nested message and enum types in `ColumnEncoding`.
pub mod column_encoding {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        Direct = 0,
        Dictionary = 1,
        DirectV2 = 2,
        DictionaryV2 = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StripeFooter {
    #[prost(message, repeated, tag = "1")]
    pub streams: ::prost::alloc::vec::Vec<Stream>,
    #[prost(message, repeated, tag = "2")]
    pub columns: ::prost::alloc::vec::Vec<ColumnEncoding>,
    #[prost(string, optional, tag = "3")]
    pub writer_timezone: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Type {
    #[prost(enumeration = "r#type::Kind", optional, tag = "1")]
    pub kind: ::core::option::Option<i32>,
    #[prost(uint32, repeated, tag = "2")]
    pub subtypes: ::prost::alloc::vec::Vec<u32>,
    #[prost(string, repeated, tag = "3")]
    pub field_names: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag = "4")]
    pub maximum_length: ::core::option::Option<u32>,
    #[prost(uint32, optional, tag = "5")]
    pub precision: ::core::option::Option<u32>,
    #[prost(uint32, optional, tag = "6")]
    pub scale: ::core::option::Option<u32>,
}
/// Nested message and enum types in `Type`.
pub mod r#type {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        Boolean = 0,
        Byte = 1,
        Short = 2,
        Int = 3,
        Long = 4,
        Float = 5,
        Double = 6,
        String = 7,
        Binary = 8,
        Timestamp = 9,
        List = 10,
        Map = 11,
        Struct = 12,
        Union = 13,
        Decimal = 14,
        Date = 15,
        Varchar = 16,
        Char = 17,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StripeInformation {
    #[prost(uint64, optional, tag = "1")]
    pub offset: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub index_length: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub data_length: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "4")]
    pub footer_length: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "5")]
    pub number_of_rows: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserMetadataItem {
    #[prost(string, optional, tag = "1")]
    pub name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(bytes = "vec", optional, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StripeStatistics {
    #[prost(message, repeated, tag = "1")]
    pub col_stats: ::prost::alloc::vec::Vec<ColumnStatistics>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metadata {
    #[prost(message, repeated, tag = "1")]
    pub stripe_stats: ::prost::alloc::vec::Vec<StripeStatistics>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Footer {
    #[prost(uint64, optional, tag = "1")]
    pub header_length: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub content_length: ::core::option::Option<u64>,
    #[prost(message, repeated, tag = "3")]
    pub stripes: ::prost::alloc::vec::Vec<StripeInformation>,
    #[prost(message, repeated, tag = "4")]
    pub types: ::prost::alloc::vec::Vec<Type>,
    #[prost(message, repeated, tag = "5")]
    pub metadata: ::prost::alloc::vec::Vec<UserMetadataItem>,
    #[prost(uint64, optional, tag = "6")]
    pub number_of_rows: ::core::option::Option<u64>,
    #[prost(message, repeated, tag = "7")]
    pub statistics: ::prost::alloc::vec::Vec<ColumnStatistics>,
    #[prost(uint32, optional, tag = "8")]
    pub row_index_stride: ::core::option::Option<u32>,
}
/// Serialized length must be less that 255 bytes
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostScript {
    #[prost(uint64, optional, tag = "1")]
    pub footer_length: ::core::option::Option<u64>,
    #[prost(enumeration = "CompressionKind", optional, tag = "2")]
    pub compression: ::core::option::Option<i32>,
    #[prost(uint64, optional, tag = "3")]
    pub compression_block_size: ::core::option::Option<u64>,
    /// the version of the file format
    ///   [0, 11] = Hive 0.11
    ///   [0, 12] = Hive 0.12
    #[prost(uint32, repeated, tag = "4")]
    pub version: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint64, optional, tag = "5")]
    pub metadata_length: ::core::option::Option<u64>,
    /// Version of the writer:
    ///   0 (or missing) = original
    ///   1 = HIVE-8732 fixed
    #[prost(uint32, optional, tag = "6")]
    pub writer_version: ::core::option::Option<u32>,
    /// Leave this last in the record
    #[prost(string, optional, tag = "8000")]
    pub magic: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompressionKind {
    None = 0,
    Zlib = 1,
    Snappy = 2,
    Lzo = 3,
    Lz4 = 4,
    Zstd = 5,
}
