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

use std::marker::PhantomData;

use arrow::datatypes::{ArrowTimestampType, TimeUnit};
use snafu::ensure;

use crate::{
    encoding::PrimitiveValueDecoder,
    error::{DecodeTimestampSnafu, Result},
};

const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;

pub struct TimestampDecoder<T: ArrowTimestampType> {
    base_from_epoch: i64,
    data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    _marker: PhantomData<T>,
}

impl<T: ArrowTimestampType> TimestampDecoder<T> {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
            _marker: PhantomData,
        }
    }
}

impl<T: ArrowTimestampType> PrimitiveValueDecoder<T::Native> for TimestampDecoder<T> {
    fn decode(&mut self, out: &mut [T::Native]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut data = vec![0; out.len()];
        let mut secondary = vec![0; out.len()];
        self.data.decode(&mut data)?;
        self.secondary.decode(&mut secondary)?;
        for (index, (&seconds_since_orc_base, &nanoseconds)) in
            data.iter().zip(secondary.iter()).enumerate()
        {
            out[index] =
                decode_timestamp::<T>(self.base_from_epoch, seconds_since_orc_base, nanoseconds)?;
        }
        Ok(())
    }
}

/// Arrow TimestampNanosecond type cannot represent the full datetime range of
/// the ORC Timestamp type, so this iterator provides the ability to decode the
/// raw nanoseconds without restricting it to the Arrow TimestampNanosecond range.
pub struct TimestampNanosecondAsDecimalDecoder {
    base_from_epoch: i64,
    data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
}

impl TimestampNanosecondAsDecimalDecoder {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
        }
    }
}

impl PrimitiveValueDecoder<i128> for TimestampNanosecondAsDecimalDecoder {
    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut data = vec![0; out.len()];
        let mut secondary = vec![0; out.len()];
        self.data.decode(&mut data)?;
        self.secondary.decode(&mut secondary)?;
        for (index, (&seconds_since_orc_base, &nanoseconds)) in
            data.iter().zip(secondary.iter()).enumerate()
        {
            out[index] =
                decode_timestamp_as_i128(self.base_from_epoch, seconds_since_orc_base, nanoseconds);
        }
        Ok(())
    }
}

fn decode(base: i64, seconds_since_orc_base: i64, nanoseconds: i64) -> (i128, i64, u64) {
    let data = seconds_since_orc_base;
    // TODO: is this a safe cast?
    let mut nanoseconds = nanoseconds as u64;
    // Last 3 bits indicate how many trailing zeros were truncated
    let zeros = nanoseconds & 0x7;
    nanoseconds >>= 3;
    // Multiply by powers of 10 to get back the trailing zeros
    // TODO: would it be more efficient to unroll this? (if LLVM doesn't already do so)
    if zeros != 0 {
        nanoseconds *= 10_u64.pow(zeros as u32 + 1);
    }
    let seconds_since_epoch = data + base;
    // Timestamps below the UNIX epoch with nanoseconds > 999_999 need to be
    // adjusted to have 1 second subtracted due to ORC-763:
    // https://issues.apache.org/jira/browse/ORC-763
    let seconds = if seconds_since_epoch < 0 && nanoseconds > 999_999 {
        seconds_since_epoch - 1
    } else {
        seconds_since_epoch
    };
    // Convert into nanoseconds since epoch, which Arrow uses as native representation
    // of timestamps
    // The timestamp may overflow i64 as ORC encodes them as a pair of (seconds, nanoseconds)
    // while we encode them as a single i64 of nanoseconds in Arrow.
    let nanoseconds_since_epoch =
        (seconds as i128 * NANOSECONDS_IN_SECOND as i128) + (nanoseconds as i128);
    (nanoseconds_since_epoch, seconds, nanoseconds)
}

fn decode_timestamp<T: ArrowTimestampType>(
    base: i64,
    seconds_since_orc_base: i64,
    nanoseconds: i64,
) -> Result<i64> {
    let (nanoseconds_since_epoch, seconds, nanoseconds) =
        decode(base, seconds_since_orc_base, nanoseconds);

    let nanoseconds_in_timeunit = match T::UNIT {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    // Error if loss of precision
    // TODO: make this configurable (e.g. can succeed but truncate)
    ensure!(
        nanoseconds_since_epoch % nanoseconds_in_timeunit == 0,
        DecodeTimestampSnafu {
            seconds,
            nanoseconds,
            to_time_unit: T::UNIT,
        }
    );

    // Convert to i64 and error if overflow
    let num_since_epoch = (nanoseconds_since_epoch / nanoseconds_in_timeunit)
        .try_into()
        .or_else(|_| {
            DecodeTimestampSnafu {
                seconds,
                nanoseconds,
                to_time_unit: T::UNIT,
            }
            .fail()
        })?;

    Ok(num_since_epoch)
}

fn decode_timestamp_as_i128(base: i64, seconds_since_orc_base: i64, nanoseconds: i64) -> i128 {
    let (nanoseconds_since_epoch, _, _) = decode(base, seconds_since_orc_base, nanoseconds);
    nanoseconds_since_epoch
}
