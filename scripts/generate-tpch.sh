#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
BASE_DIR=$SCRIPT_DIR/..
DATA_DIR=$BASE_DIR/benchmark_data
VENV_BIN=$BASE_DIR/venv/bin

SCALE_FACTOR=${1:-1}

# Generate TBL data
mkdir -p $DATA_DIR
docker run --rm \
  -v $DATA_DIR:/data \
  ghcr.io/scalytics/tpch-docker:main -vf -s $SCALE_FACTOR
# Removing trailing |
sed -i 's/.$//' benchmark_data/*.tbl
$VENV_BIN/python $SCRIPT_DIR/convert_tpch.py
echo "Done"
