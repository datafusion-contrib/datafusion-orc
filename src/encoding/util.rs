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

use std::io::Read;

use snafu::ResultExt;

use crate::error::{self, Result};

/// Read single byte.
#[inline]
pub fn read_u8(reader: &mut impl Read) -> Result<u8> {
    let mut byte = [0];
    reader.read_exact(&mut byte).context(error::IoSnafu)?;
    Ok(byte[0])
}

/// Like [`read_u8()`] but returns `Ok(None)` if reader has reached EOF.
#[inline]
pub fn try_read_u8(reader: &mut impl Read) -> Result<Option<u8>> {
    let mut byte = [0];
    let length = reader.read(&mut byte).context(error::IoSnafu)?;
    Ok((length > 0).then_some(byte[0]))
}
