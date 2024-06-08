use std::fmt;
use std::io::Read;
use std::ops::{BitOrAssign, ShlAssign};

use num::traits::CheckedShl;
use num::{PrimInt, Signed};

use crate::column::Column;
use crate::error::{InvalidColumnEncodingSnafu, Result};
use crate::proto::column_encoding::Kind as ProtoColumnKind;

use self::rle_v1::RleReaderV1;
use self::rle_v2::RleReaderV2;
use self::util::{
    get_closest_aligned_bit_width, signed_msb_decode, signed_msb_encode, signed_zigzag_decode,
    signed_zigzag_encode,
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
    ) -> Box<dyn Iterator<Item = Result<i64>> + Send> {
        match self {
            RleVersion::V1 => Box::new(RleReaderV1::<_, _, UnsignedEncoding>::new(reader)),
            RleVersion::V2 => Box::new(RleReaderV2::<_, _, UnsignedEncoding>::new(reader)),
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
        ProtoColumnKind::Direct => Ok(Box::new(RleReaderV1::<N, _, SignedEncoding>::new(reader))),
        ProtoColumnKind::DirectV2 => Ok(Box::new(RleReaderV2::<N, _, SignedEncoding>::new(reader))),
        k => InvalidColumnEncodingSnafu {
            name: column.name(),
            encoding: k,
        }
        .fail(),
    }
}

pub trait EncodingSign: Send + 'static {
    // TODO: have separate type/trait to represent Zigzag encoded NInt?
    fn zigzag_decode<N: VarintSerde>(v: N) -> N;
    fn zigzag_encode<N: VarintSerde>(v: N) -> N;

    fn decode_signed_msb<N: NInt>(v: N, encoded_byte_size: usize) -> N;
    fn encode_signed_msb<N: NInt>(v: N, encoded_byte_size: usize) -> N;
}

pub struct SignedEncoding;

impl EncodingSign for SignedEncoding {
    #[inline]
    fn zigzag_decode<N: VarintSerde>(v: N) -> N {
        signed_zigzag_decode(v)
    }

    #[inline]
    fn zigzag_encode<N: VarintSerde>(v: N) -> N {
        signed_zigzag_encode(v)
    }

    #[inline]
    fn decode_signed_msb<N: NInt>(v: N, encoded_byte_size: usize) -> N {
        signed_msb_decode(v, encoded_byte_size)
    }

    #[inline]
    fn encode_signed_msb<N: NInt>(v: N, encoded_byte_size: usize) -> N {
        signed_msb_encode(v, encoded_byte_size)
    }
}

pub struct UnsignedEncoding;

impl EncodingSign for UnsignedEncoding {
    #[inline]
    fn zigzag_decode<N: VarintSerde>(v: N) -> N {
        v
    }

    #[inline]
    fn zigzag_encode<N: VarintSerde>(v: N) -> N {
        v
    }

    #[inline]
    fn decode_signed_msb<N: NInt>(v: N, _encoded_byte_size: usize) -> N {
        v
    }

    #[inline]
    fn encode_signed_msb<N: NInt>(v: N, _encoded_byte_size: usize) -> N {
        v
    }
}

pub trait VarintSerde: PrimInt + CheckedShl + BitOrAssign + Signed {
    const BYTE_SIZE: usize;

    /// Calculate the minimum bit size required to represent this value, by truncating
    /// the leading zeros.
    #[inline]
    fn bits_used(self) -> usize {
        Self::BYTE_SIZE * 8 - self.leading_zeros() as usize
    }

    /// Feeds [`Self::bits_used`] into a mapping to get an aligned bit width.
    fn closest_aligned_bit_width(self) -> usize {
        get_closest_aligned_bit_width(self.bits_used())
    }

    fn from_u8(b: u8) -> Self;
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
    fn from_i64(u: i64) -> Self;

    fn from_be_bytes(b: Self::Bytes) -> Self;

    // TODO: use num_traits::ToBytes instead
    fn to_be_bytes(self) -> Self::Bytes;

    fn add_i64(self, i: i64) -> Option<Self>;

    fn sub_i64(self, i: i64) -> Option<Self>;

    // TODO: use Into<i64> instead?
    fn as_i64(self) -> i64;
}

impl VarintSerde for i16 {
    const BYTE_SIZE: usize = 2;

    #[inline]
    fn from_u8(b: u8) -> Self {
        b as Self
    }
}

impl VarintSerde for i32 {
    const BYTE_SIZE: usize = 4;

    #[inline]
    fn from_u8(b: u8) -> Self {
        b as Self
    }
}

impl VarintSerde for i64 {
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
}

// We only implement for i16, i32, i64 and u64.
// ORC supports only signed Short, Integer and Long types for its integer types,
// and i8 is encoded as bytes. u64 is used for other encodings such as Strings
// (to encode length, etc.).

impl NInt for i16 {
    type Bytes = [u8; 2];

    #[inline]
    fn from_i64(i: i64) -> Self {
        i as Self
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
    fn add_i64(self, i: i64) -> Option<Self> {
        i.try_into().ok().and_then(|i| self.checked_add(i))
    }

    #[inline]
    fn sub_i64(self, i: i64) -> Option<Self> {
        i.try_into().ok().and_then(|i| self.checked_sub(i))
    }

    #[inline]
    fn as_i64(self) -> i64 {
        self as i64
    }
}

impl NInt for i32 {
    type Bytes = [u8; 4];

    #[inline]
    fn from_i64(i: i64) -> Self {
        i as Self
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
    fn add_i64(self, i: i64) -> Option<Self> {
        i.try_into().ok().and_then(|i| self.checked_add(i))
    }

    #[inline]
    fn sub_i64(self, i: i64) -> Option<Self> {
        i.try_into().ok().and_then(|i| self.checked_sub(i))
    }

    #[inline]
    fn as_i64(self) -> i64 {
        self as i64
    }
}

impl NInt for i64 {
    type Bytes = [u8; 8];

    #[inline]
    fn from_i64(i: i64) -> Self {
        i as Self
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
    fn add_i64(self, i: i64) -> Option<Self> {
        self.checked_add(i)
    }

    #[inline]
    fn sub_i64(self, i: i64) -> Option<Self> {
        self.checked_sub(i)
    }

    #[inline]
    fn as_i64(self) -> i64 {
        self
    }
}
