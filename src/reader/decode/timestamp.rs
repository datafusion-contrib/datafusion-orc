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

use crate::error::{DecodeTimestampSnafu, Result};

const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;

pub struct TimestampIterator<T: ArrowTimestampType, Item: TryFrom<i128>> {
    base_from_epoch: i64,
    data: Box<dyn Iterator<Item = Result<i64>> + Send>,
    secondary: Box<dyn Iterator<Item = Result<u64>> + Send>,
    _marker: PhantomData<(T, Item)>,
}

impl<T: ArrowTimestampType, Item: TryFrom<i128>> TimestampIterator<T, Item> {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn Iterator<Item = Result<i64>> + Send>,
        secondary: Box<dyn Iterator<Item = Result<u64>> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
            _marker: PhantomData,
        }
    }
}

impl<T: ArrowTimestampType, Item: TryFrom<i128>> Iterator for TimestampIterator<T, Item> {
    type Item = Result<Item>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: throw error for mismatched stream lengths?
        let (seconds_since_orc_base, nanoseconds) =
            self.data.by_ref().zip(self.secondary.by_ref()).next()?;
        decode_timestamp::<T, _>(self.base_from_epoch, seconds_since_orc_base, nanoseconds)
            .transpose()
    }
}

fn decode_timestamp<T: ArrowTimestampType, Ret: TryFrom<i128>>(
    base: i64,
    seconds_since_orc_base: Result<i64>,
    nanoseconds: Result<u64>,
) -> Result<Option<Ret>> {
    let data = seconds_since_orc_base?;
    let mut nanoseconds = nanoseconds?;
    // Last 3 bits indicate how many trailing zeros were truncated
    let zeros = nanoseconds & 0x7;
    nanoseconds >>= 3;
    // Multiply by powers of 10 to get back the trailing zeros
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

    let nanoseconds_in_timeunit = match T::UNIT {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    // Error if loss of precision
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

    Ok(Some(num_since_epoch))
}
