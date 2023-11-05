use std::io::Read;

use crate::arrow_reader::column::Column;
use crate::error::{InvalidColumnEncodingSnafu, Result};
use crate::proto;

use self::rle_v1::{SignedRleReaderV1, UnsignedRleReaderV1};
use self::rle_v2::{RleReaderV2, UnsignedRleReaderV2};

pub mod boolean_rle;
pub mod byte_rle;
pub mod float;
pub mod rle_v1;
pub mod rle_v2;
mod util;
pub mod variable_length;

#[derive(Clone, Copy, Debug)]
pub enum RleVersion {
    V1,
    V2,
}

impl RleVersion {
    pub fn get_unsigned_rle_reader<R: Read + Send + 'static>(
        &self,
        reader: R,
    ) -> Box<dyn Iterator<Item = Result<u64>> + Send> {
        match self {
            RleVersion::V1 => Box::new(UnsignedRleReaderV1::new(reader)),
            RleVersion::V2 => Box::new(UnsignedRleReaderV2::try_new(reader, true)),
        }
    }
}

impl From<proto::column_encoding::Kind> for RleVersion {
    fn from(value: proto::column_encoding::Kind) -> Self {
        match value {
            proto::column_encoding::Kind::Direct => Self::V1,
            proto::column_encoding::Kind::Dictionary => Self::V1,
            proto::column_encoding::Kind::DirectV2 => Self::V2,
            proto::column_encoding::Kind::DictionaryV2 => Self::V2,
        }
    }
}

pub fn get_direct_signed_rle_reader<R: Read + Send + 'static>(
    column: &Column,
    reader: R,
) -> Result<Box<dyn Iterator<Item = Result<i64>> + Send>> {
    match column.encoding().kind() {
        crate::proto::column_encoding::Kind::Direct => Ok(Box::new(SignedRleReaderV1::new(reader))),
        crate::proto::column_encoding::Kind::DirectV2 => {
            Ok(Box::new(RleReaderV2::try_new(reader, true, true)))
        }
        k => InvalidColumnEncodingSnafu {
            name: column.name(),
            encoding: k,
        }
        .fail(),
    }
}

pub fn get_direct_unsigned_rle_reader<R: Read + Send + 'static>(
    column: &Column,
    reader: R,
) -> Result<Box<dyn Iterator<Item = Result<u64>> + Send>> {
    match column.encoding().kind() {
        crate::proto::column_encoding::Kind::Direct => {
            Ok(Box::new(UnsignedRleReaderV1::new(reader)))
        }
        crate::proto::column_encoding::Kind::DirectV2 => {
            Ok(Box::new(UnsignedRleReaderV2::try_new(reader, true)))
        }
        k => InvalidColumnEncodingSnafu {
            name: column.name(),
            encoding: k,
        }
        .fail(),
    }
}
