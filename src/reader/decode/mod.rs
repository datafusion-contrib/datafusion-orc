use std::fmt;
use std::io::Read;
use std::ops::{BitOrAssign, ShlAssign};

use num::traits::CheckedShl;
use num::PrimInt;

use crate::column::Column;
use crate::error::{InvalidColumnEncodingSnafu, Result};
use crate::proto::column_encoding::Kind as ProtoColumnKind;

use self::rle_v1::RleReaderV1;
use self::rle_v2::RleReaderV2;
use self::util::{
    signed_msb_decode, signed_msb_encode, signed_zigzag_decode, signed_zigzag_encode,
};

// TODO: rename mod to encoding

pub mod boolean_rle;
pub mod byte_rle;
pub mod decimal;
pub mod float;
pub mod rle_v1;
pub mod rle_v2;
pub mod timestamp;
mod util;

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

pub fn get_rle_reader<N: NInt, R: Read + Send + 'static>(
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

/// Used for varint encoding & decoding, with methods for zigzag encoding
/// and decoding.
pub trait VarintSerde: PrimInt + CheckedShl + BitOrAssign {
    const BYTE_SIZE: usize;

    /// Calculate the minimum bit size required to represent this value, by truncating
    /// the leading zeros.
    #[inline]
    fn bits_used(self) -> usize {
        Self::BYTE_SIZE * 8 - self.leading_zeros() as usize
    }

    fn from_u8(b: u8) -> Self;

    // TODO: have separate type/trait to represent Zigzag encoded NInt?
    #[inline]
    fn zigzag_decode(self) -> Self {
        // Default noop for unsigned (signed should override this)
        self
    }

    #[inline]
    fn zigzag_encode(self) -> Self {
        // Default noop for unsigned (signed should override this)
        self
    }
}

/// Helps generalise the decoder efforts to be specific to supported integers.
/// (Instead of decoding to u64/i64 for all then downcasting).
pub trait NInt:
    VarintSerde + ShlAssign<usize> + fmt::Debug + fmt::Display + fmt::Binary + Send + Sync + 'static
{
    type Bytes: AsRef<[u8]> + AsMut<[u8]> + Default + Clone + Copy + fmt::Debug;

    #[inline]
    fn empty_byte_array() -> Self::Bytes {
        Self::Bytes::default()
    }

    /// Should truncate any extra bits.
    fn from_u64(u: u64) -> Self;

    fn from_be_bytes(b: Self::Bytes) -> Self;

    // TODO: use num_traits::ToBytes instead
    fn to_be_bytes(self) -> Self::Bytes;

    #[inline]
    fn decode_signed_msb(self, _encoded_byte_size: usize) -> Self {
        // Default noop for unsigned (signed should override this)
        // Used when decoding Patched Base in RLEv2
        self
    }

    #[inline]
    fn encode_signed_msb(self, _encoded_byte_size: usize) -> Self {
        // Default noop for unsigned (signed should override this)
        // Used when encoding Patched Base in RLEv2
        self
    }

    fn add_u64(self, u: u64) -> Option<Self>;

    fn sub_u64(self, u: u64) -> Option<Self>;
}

impl VarintSerde for i16 {
    const BYTE_SIZE: usize = 2;

    #[inline]
    fn from_u8(b: u8) -> Self {
        b as Self
    }

    #[inline]
    fn zigzag_decode(self) -> Self {
        signed_zigzag_decode(self)
    }

    #[inline]
    fn zigzag_encode(self) -> Self {
        signed_zigzag_encode(self)
    }
}

impl VarintSerde for i32 {
    const BYTE_SIZE: usize = 4;

    #[inline]
    fn from_u8(b: u8) -> Self {
        b as Self
    }

    #[inline]
    fn zigzag_decode(self) -> Self {
        signed_zigzag_decode(self)
    }

    #[inline]
    fn zigzag_encode(self) -> Self {
        signed_zigzag_encode(self)
    }
}

impl VarintSerde for i64 {
    const BYTE_SIZE: usize = 8;

    #[inline]
    fn from_u8(b: u8) -> Self {
        b as Self
    }

    #[inline]
    fn zigzag_decode(self) -> Self {
        signed_zigzag_decode(self)
    }

    #[inline]
    fn zigzag_encode(self) -> Self {
        signed_zigzag_encode(self)
    }
}

impl VarintSerde for u64 {
    const BYTE_SIZE: usize = 8;

    #[inline]
    fn from_u8(b: u8) -> Self {
        b as Self
    }
}

impl VarintSerde for i128 {
    const BYTE_SIZE: usize = 16;

    #[inline]
    fn from_u8(b: u8) -> Self {
        b as Self
    }

    #[inline]
    fn zigzag_decode(self) -> Self {
        signed_zigzag_decode(self)
    }

    #[inline]
    fn zigzag_encode(self) -> Self {
        signed_zigzag_encode(self)
    }
}

// We only implement for i16, i32, i64 and u64.
// ORC supports only signed Short, Integer and Long types for its integer types,
// and i8 is encoded as bytes. u64 is used for other encodings such as Strings
// (to encode length, etc.).

impl NInt for i16 {
    type Bytes = [u8; 2];

    #[inline]
    fn from_u64(u: u64) -> Self {
        u as Self
    }

    #[inline]
    fn from_be_bytes(b: Self::Bytes) -> Self {
        Self::from_be_bytes(b)
    }

    #[inline]
    fn to_be_bytes(self) -> Self::Bytes {
        self.to_be_bytes()
    }

    #[inline]
    fn decode_signed_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_decode(self, encoded_byte_size)
    }

    #[inline]
    fn encode_signed_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_encode(self, encoded_byte_size)
    }

    #[inline]
    fn add_u64(self, u: u64) -> Option<Self> {
        u.try_into().ok().and_then(|u| self.checked_add_unsigned(u))
    }

    #[inline]
    fn sub_u64(self, u: u64) -> Option<Self> {
        u.try_into().ok().and_then(|u| self.checked_sub_unsigned(u))
    }
}

impl NInt for i32 {
    type Bytes = [u8; 4];

    #[inline]
    fn from_u64(u: u64) -> Self {
        u as Self
    }

    #[inline]
    fn from_be_bytes(b: Self::Bytes) -> Self {
        Self::from_be_bytes(b)
    }

    #[inline]
    fn to_be_bytes(self) -> Self::Bytes {
        self.to_be_bytes()
    }

    #[inline]
    fn decode_signed_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_decode(self, encoded_byte_size)
    }

    #[inline]
    fn encode_signed_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_encode(self, encoded_byte_size)
    }

    #[inline]
    fn add_u64(self, u: u64) -> Option<Self> {
        u.try_into().ok().and_then(|u| self.checked_add_unsigned(u))
    }

    #[inline]
    fn sub_u64(self, u: u64) -> Option<Self> {
        u.try_into().ok().and_then(|u| self.checked_sub_unsigned(u))
    }
}

impl NInt for i64 {
    type Bytes = [u8; 8];

    #[inline]
    fn from_u64(u: u64) -> Self {
        u as Self
    }

    #[inline]
    fn from_be_bytes(b: Self::Bytes) -> Self {
        Self::from_be_bytes(b)
    }

    #[inline]
    fn to_be_bytes(self) -> Self::Bytes {
        self.to_be_bytes()
    }

    #[inline]
    fn decode_signed_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_decode(self, encoded_byte_size)
    }

    #[inline]
    fn encode_signed_msb(self, encoded_byte_size: usize) -> Self {
        signed_msb_encode(self, encoded_byte_size)
    }

    #[inline]
    fn add_u64(self, u: u64) -> Option<Self> {
        self.checked_add_unsigned(u)
    }

    #[inline]
    fn sub_u64(self, u: u64) -> Option<Self> {
        self.checked_sub_unsigned(u)
    }
}

impl NInt for u64 {
    type Bytes = [u8; 8];

    #[inline]
    fn from_u64(u: u64) -> Self {
        u as Self
    }

    #[inline]
    fn from_be_bytes(b: Self::Bytes) -> Self {
        Self::from_be_bytes(b)
    }

    #[inline]
    fn to_be_bytes(self) -> Self::Bytes {
        self.to_be_bytes()
    }

    #[inline]
    fn add_u64(self, u: u64) -> Option<Self> {
        self.checked_add(u)
    }

    #[inline]
    fn sub_u64(self, u: u64) -> Option<Self> {
        self.checked_sub(u)
    }
}
