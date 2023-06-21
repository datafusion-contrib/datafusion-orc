use std::io::Read;

use snafu::ResultExt;

use crate::error::{self, Result};

pub fn read_u8(reader: &mut dyn Read) -> Result<u8> {
    let mut byte = [0u8];
    reader.read_exact(&mut byte).context(error::IoSnafu)?;

    Ok(byte[0])
}

pub fn bytes_to_long_be<R: Read>(r: &mut R, mut n: usize) -> Result<i64> {
    let mut out: i64 = 0;

    while n > 0 {
        n -= 1;
        let val = read_u8(r)? as i64;
        out |= val << (n * 8) as u64;
    }

    Ok(out)
}

pub fn get_closest_fixed_bits(width: usize) -> usize {
    match width {
        0 => 1,
        1..=24 => width,
        25..=26 => 26,
        27..=28 => 28,
        29..=30 => 30,
        31..=32 => 32,
        33..=40 => 40,
        41..=48 => 48,
        49..=56 => 56,
        _ => 64,
    }
}

pub fn read_ints(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    bit_size: usize,
    r: &mut dyn Read,
) -> Result<()> {
    let mut bits_left = 0;
    let mut current = 0;

    match bit_size {
        1 => unrolled_unpack_1(buffer, offset, len, r),
        2 => unrolled_unpack_2(buffer, offset, len, r),
        4 => unrolled_unpack_4(buffer, offset, len, r),
        8 => unrolled_unpack_8(buffer, offset, len, r),
        16 => unrolled_unpack_16(buffer, offset, len, r),
        24 => unrolled_unpack_24(buffer, offset, len, r),
        32 => unrolled_unpack_32(buffer, offset, len, r),
        40 => unrolled_unpack_40(buffer, offset, len, r),
        48 => unrolled_unpack_48(buffer, offset, len, r),
        56 => unrolled_unpack_56(buffer, offset, len, r),
        64 => unrolled_unpack_64(buffer, offset, len, r),
        _ => {
            for item in buffer.iter_mut().skip(offset).take(len) {
                let mut result: i64 = 0;
                let mut bits_left_to_read = bit_size;

                while bits_left_to_read > bits_left {
                    result <<= bits_left;
                    result |= (current & ((1 << bits_left) - 1)) as i64;
                    bits_left_to_read -= bits_left;

                    current = read_u8(r)? as i32;

                    bits_left = 8;
                }

                if bits_left_to_read > 0 {
                    result <<= bits_left_to_read;
                    bits_left -= bits_left_to_read;
                    result |= ((current >> bits_left) & ((1 << bits_left_to_read) - 1)) as i64;
                }

                *item = result;
            }

            Ok(())
        }
    }
}

fn unrolled_unpack_1(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    reader: &mut dyn Read,
) -> Result<()> {
    let num_hops = 8;
    let remainder = len % num_hops;
    let end_offset = offset + len;
    let end_unroll = end_offset - remainder;

    for i in (offset..end_unroll).step_by(num_hops) {
        let byte = read_u8(reader)?;
        let val = byte as u64;
        buffer[i] = ((val >> 7) & 1) as i64;
        buffer[i + 1] = ((val >> 6) & 1) as i64;
        buffer[i + 2] = ((val >> 5) & 1) as i64;
        buffer[i + 3] = ((val >> 4) & 1) as i64;
        buffer[i + 4] = ((val >> 3) & 1) as i64;
        buffer[i + 5] = ((val >> 2) & 1) as i64;
        buffer[i + 6] = ((val >> 1) & 1) as i64;
        buffer[i + 7] = (val & 1) as i64;
    }

    if remainder > 0 {
        let mut start_shift = 7;
        let val = read_u8(reader)? as u64;
        for item in buffer.iter_mut().take(end_offset).skip(end_unroll) {
            *item = ((val >> start_shift) & 1) as i64;
            start_shift -= 1;
        }
    }

    Ok(())
}

fn unrolled_unpack_2(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    reader: &mut dyn Read,
) -> Result<()> {
    let num_hops = 4;
    let remainder = len % num_hops;
    let end_offset = offset + len;
    let end_unroll = end_offset - remainder;

    for i in (offset..end_unroll).step_by(num_hops) {
        let val = read_u8(reader)? as u64;
        buffer[i] = ((val >> 6) & 3) as i64;
        buffer[i + 1] = ((val >> 4) & 3) as i64;
        buffer[i + 2] = (val >> 2 & 3) as i64;
        buffer[i + 3] = ((val) & 3) as i64;
    }

    if remainder > 0 {
        let mut start_shift = 6;
        let val = read_u8(reader)? as u64;
        for item in buffer.iter_mut().take(end_offset).skip(end_unroll) {
            *item = ((val >> start_shift) & 3) as i64;
            start_shift -= 2;
        }
    }
    Ok(())
}

fn unrolled_unpack_4(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    reader: &mut dyn Read,
) -> Result<()> {
    let num_hops = 2;
    let remainder = len % num_hops;
    let end_offset = offset + len;
    let end_unroll = end_offset - remainder;

    for i in (offset..end_unroll).step_by(num_hops) {
        let val = read_u8(reader)? as u64;
        buffer[i] = ((val >> 4) & 15) as i64;
        buffer[i + 1] = (val & 15) as i64;
    }

    if remainder > 0 {
        let mut start_shift = 6;
        let val = read_u8(reader)? as u64;
        for item in buffer.iter_mut().take(end_offset).skip(end_unroll) {
            *item = ((val >> start_shift) & 15) as i64;
            start_shift -= 4;
        }
    }
    Ok(())
}

fn unrolled_unpack_8(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 1)
}

fn unrolled_unpack_16(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 2)
}

fn unrolled_unpack_24(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 3)
}

fn unrolled_unpack_32(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 4)
}

fn unrolled_unpack_40(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 5)
}

fn unrolled_unpack_48(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 6)
}

fn unrolled_unpack_56(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 7)
}

fn unrolled_unpack_64(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
) -> Result<()> {
    unrolled_unpack_bytes(buffer, offset, len, r, 8)
}

fn unrolled_unpack_bytes(
    buffer: &mut [i64],
    offset: usize,
    len: usize,
    r: &mut dyn Read,
    num_bytes: usize,
) -> Result<()> {
    let num_hops = 8;
    let remainder = len % num_hops;
    let end_offset = offset + len;
    let end_unroll = end_offset - remainder;
    let mut i = offset;
    while i < end_unroll {
        read_long_be(r, buffer, i, num_hops, num_bytes)?;
        i += num_hops;
    }
    if remainder > 0 {
        read_remaining_longs(buffer, i, r, remainder, num_bytes)?;
    }
    Ok(())
}

fn read_remaining_longs(
    buffer: &mut [i64],
    mut offset: usize,
    r: &mut dyn Read,
    mut remainder: usize,
    num_bytes: usize,
) -> Result<()> {
    let to_read = remainder * num_bytes;
    let mut read_buffer = vec![0u8; to_read];
    r.read_exact(&mut read_buffer).context(error::IoSnafu)?;

    let mut idx = 0;
    match num_bytes {
        1 => {
            while remainder > 0 {
                buffer[offset] = i64::from(read_buffer[idx]);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        2 => {
            while remainder > 0 {
                buffer[offset] = read_long_be2(&read_buffer, idx * 2);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        3 => {
            while remainder > 0 {
                buffer[offset] = read_long_be3(&read_buffer, idx * 3);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        4 => {
            while remainder > 0 {
                buffer[offset] = read_long_be4(&read_buffer, idx * 4);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        5 => {
            while remainder > 0 {
                buffer[offset] = read_long_be5(&read_buffer, idx * 5);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        6 => {
            while remainder > 0 {
                buffer[offset] = read_long_be6(&read_buffer, idx * 6);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        7 => {
            while remainder > 0 {
                buffer[offset] = read_long_be7(&read_buffer, idx * 7);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        8 => {
            while remainder > 0 {
                buffer[offset] = read_long_be8(&read_buffer, idx * 8);
                offset += 1;
                remainder -= 1;
                idx += 1;
            }
        }
        _ => {
            return error::InvalidInputSnafu {
                msg: "Invalid number of bytes",
            }
            .fail();
        }
    }

    Ok(())
}

fn read_long_be(
    r: &mut dyn Read,
    buffer: &mut [i64],
    start: usize,
    num_hops: usize,
    num_bytes: usize,
) -> Result<()> {
    let to_read = num_hops * num_bytes;
    let mut read_buffer = vec![0u8; to_read];
    r.read_exact(&mut read_buffer).context(error::IoSnafu)?;

    match num_bytes {
        1 => {
            for i in 0..8 {
                buffer[start + i] = i64::from(read_buffer[i]);
            }
        }
        2 => {
            for i in 0..8 {
                buffer[start + i] = read_long_be2(&read_buffer, i * 2);
            }
        }
        3 => {
            for i in 0..8 {
                buffer[start + i] = read_long_be3(&read_buffer, i * 3);
            }
        }
        4 => {
            for i in 0..8 {
                buffer[start + i] = read_long_be4(&read_buffer, i * 4);
            }
        }
        5 => {
            for i in 0..8 {
                buffer[start + i] = read_long_be5(&read_buffer, i * 5);
            }
        }
        6 => {
            for i in 0..8 {
                buffer[start + i] = read_long_be6(&read_buffer, i * 6);
            }
        }
        7 => {
            for i in 0..8 {
                buffer[start + i] = read_long_be7(&read_buffer, i * 7);
            }
        }
        8 => {
            for i in 0..8 {
                buffer[start + i] = read_long_be8(&read_buffer, i * 8);
            }
        }
        _ => {
            return error::InvalidInputSnafu {
                msg: "Invalid number of bytes",
            }
            .fail();
        }
    }

    Ok(())
}

fn read_long_be2(read_buffer: &[u8], rb_offset: usize) -> i64 {
    (i64::from(read_buffer[rb_offset]) << 8) + i64::from(read_buffer[rb_offset + 1])
}

fn read_long_be3(read_buffer: &[u8], rb_offset: usize) -> i64 {
    (i64::from(read_buffer[rb_offset]) << 16)
        + (i64::from(read_buffer[rb_offset + 1]) << 8)
        + i64::from(read_buffer[rb_offset + 2])
}

fn read_long_be4(read_buffer: &[u8], rb_offset: usize) -> i64 {
    (i64::from(read_buffer[rb_offset]) << 24)
        + (i64::from(read_buffer[rb_offset + 1]) << 16)
        + (i64::from(read_buffer[rb_offset + 2]) << 8)
        + i64::from(read_buffer[rb_offset + 3])
}

fn read_long_be5(read_buffer: &[u8], rb_offset: usize) -> i64 {
    (i64::from(read_buffer[rb_offset]) << 32)
        + (i64::from(read_buffer[rb_offset + 1]) << 24)
        + (i64::from(read_buffer[rb_offset + 2]) << 16)
        + (i64::from(read_buffer[rb_offset + 3]) << 8)
        + i64::from(read_buffer[rb_offset + 4])
}

fn read_long_be6(read_buffer: &[u8], rb_offset: usize) -> i64 {
    (i64::from(read_buffer[rb_offset]) << 40)
        + (i64::from(read_buffer[rb_offset + 1]) << 32)
        + (i64::from(read_buffer[rb_offset + 2]) << 24)
        + (i64::from(read_buffer[rb_offset + 3]) << 16)
        + (i64::from(read_buffer[rb_offset + 4]) << 8)
        + i64::from(read_buffer[rb_offset + 5])
}

fn read_long_be7(read_buffer: &[u8], rb_offset: usize) -> i64 {
    (i64::from(read_buffer[rb_offset]) << 48)
        + (i64::from(read_buffer[rb_offset + 1]) << 40)
        + (i64::from(read_buffer[rb_offset + 2]) << 32)
        + (i64::from(read_buffer[rb_offset + 3]) << 24)
        + (i64::from(read_buffer[rb_offset + 4]) << 16)
        + (i64::from(read_buffer[rb_offset + 5]) << 8)
        + i64::from(read_buffer[rb_offset + 6])
}

fn read_long_be8(read_buffer: &[u8], rb_offset: usize) -> i64 {
    (i64::from(read_buffer[rb_offset]) << 56)
        + (i64::from(read_buffer[rb_offset + 1]) << 48)
        + (i64::from(read_buffer[rb_offset + 2]) << 40)
        + (i64::from(read_buffer[rb_offset + 3]) << 32)
        + (i64::from(read_buffer[rb_offset + 4]) << 24)
        + (i64::from(read_buffer[rb_offset + 5]) << 16)
        + (i64::from(read_buffer[rb_offset + 6]) << 8)
        + i64::from(read_buffer[rb_offset + 7])
}
