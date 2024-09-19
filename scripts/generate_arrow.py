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

# Requires pyarrow to be installed
import glob
from pyarrow import orc, feather

dir = "tests/integration/data"

files = glob.glob(f"{dir}/expected/*")
files = [file.removeprefix(f"{dir}/expected/").removesuffix(".jsn.gz") for file in files]

ignore_files = [
    "TestOrcFile.testTimestamp" # Root data type isn't struct
]

files = [file for file in files if file not in ignore_files]

for file in files:
    print(f"Converting {file} from ORC to feather")
    table = orc.read_table(f"{dir}/{file}.orc")
    feather.write_feather(table, f"{dir}/expected_arrow/{file}.feather")
