use std::fmt;
use std::io::Read;
use std::ops::{BitOrAssign, ShlAssign};

use num::traits::CheckedShl;
use num::PrimInt;

use crate::arrow_reader::column::Column;
use crate::error::{InvalidColumnEncodingSnafu, Result};
use crate::proto::column_encoding::Kind as ProtoColumnKind;

use self::rle_v1::RleReaderV1;
use self::rle_v2::RleReaderV2;
use self::util::{signed_msb_decode, signed_zigzag_decode};

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
            RleVersion::V1 => Box::new(RleReaderV1::new(reader)),
            RleVersion::V2 => Box::new(RleReaderV2::new(reader)),
        }
    }
}

impl From<ProtoColumnKind> for RleVersion {
    fn from(value: ProtoColumnKind) -> Self {
        match value {
            ProtoColumnKind::Direct | ProtoColumnKind::Dictionary => Self::V1,
            ProtoColumnKind::DirectV2 | ProtoColumnKind::DictionaryV2 => Self::V2,
        }
    }
}

pub fn get_rle_reader<N: NInt + 'static, R: Read + Send + 'static>(
    column: &Column,
    reader: R,
) -> Result<Box<dyn Iterator<Item = Result<N>> + Send>> {
    match column.encoding().kind() {
        ProtoColumnKind::Direct => Ok(Box::new(RleReaderV1::<N, _>::new(reader))),
        ProtoColumnKind::DirectV2 => Ok(Box::new(RleReaderV2::<N, _>::new(reader))),
        k => InvalidColumnEncodingSnafu {
            name: column.name(),
            encoding: k,
        }
        .fail(),
    }
}

/// Helps generalise the decoder efforts to be specific to supported integers.
/// (Instead of decoding to u64/i64 for all then downcasting).
pub trait NInt:
    PrimInt
    + CheckedShl
    + BitOrAssign
    + ShlAssign<usize>
    + fmt::Debug
    + fmt::Display
    + fmt::Binary
    + Send
    + Sync
{
    const BYTE_SIZE: usize;

    /// Should truncate any extra bits.
    fn from_u64(u: u64) -> Self;

    fn from_u8(u: u8) -> Self;

    /// Assumes size of `b` matches `BYTE_SIZE`, can panic if it doesn't.
    fn from_be_bytes(b: &[u8]) -> Self;

    #[inline]
    fn zigzag_decode(self) -> Self {
        // Default noop for unsigned (signed should override this)
        self
    }

    #[inline]
    fn decode_signed_from_msb(self, _encoded_byte_size: usize) -> Self {
        // Default noop for unsigned (signed should override this)
        // Used when decoding Patched Base in RLEv2
        // TODO: is this correct for unsigned? Spec doesn't state, but seems logical.
        //       Add a test for this to check.
        self
    }
}

// We only implement for i16, i32, i64 and u64.
// ORC supports only signed Short, Integer and Long types for its integer types,
// and i8 is encoded as bytes. u64 is used for other encodings such as Strings
// (to encode length, etc.).

impl NInt for i16 {
    const BYTE_SIZE: usize = 2;

    fn from_u64(u: u64) -> Self {
        u as Self
    }

    fn from_u8(u: u8) -> Self {
        u as Self
    }

    fn from_be_bytes(b: &[u8]) -> Self {
        Self::from_be_bytes(b.try_into().unwrap())
    }

    fn zigzag_decode(self) -> Self {
        signed_zigzag_decode(self)
    }

    fn decode_signed_from_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_decode(self, encoded_byte_size)
    }
}

impl NInt for i32 {
    const BYTE_SIZE: usize = 4;

    fn from_u64(u: u64) -> Self {
        u as Self
    }

    fn from_u8(u: u8) -> Self {
        u as Self
    }

    fn from_be_bytes(b: &[u8]) -> Self {
        Self::from_be_bytes(b.try_into().unwrap())
    }

    fn zigzag_decode(self) -> Self {
        signed_zigzag_decode(self)
    }

    fn decode_signed_from_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_decode(self, encoded_byte_size)
    }
}

impl NInt for i64 {
    const BYTE_SIZE: usize = 8;

    fn from_u64(u: u64) -> Self {
        u as Self
    }

    fn from_u8(u: u8) -> Self {
        u as Self
    }

    fn from_be_bytes(b: &[u8]) -> Self {
        Self::from_be_bytes(b.try_into().unwrap())
    }

    fn zigzag_decode(self) -> Self {
        signed_zigzag_decode(self)
    }

    fn decode_signed_from_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_decode(self, encoded_byte_size)
    }
}

impl NInt for u64 {
    const BYTE_SIZE: usize = 8;

    fn from_u64(u: u64) -> Self {
        u as Self
    }

    fn from_u8(u: u8) -> Self {
        u as Self
    }

    fn from_be_bytes(b: &[u8]) -> Self {
        Self::from_be_bytes(b.try_into().unwrap())
    }
}
