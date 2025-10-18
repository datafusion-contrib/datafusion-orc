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

.PHONY: fmt
fmt: ## Format all the Rust code.
	cargo fmt --all


.PHONY: clippy
clippy: ## Check clippy rules.
	cargo clippy --workspace --all-targets -- -D warnings


.PHONY: fmt-toml
fmt-toml: ## Format all TOML files.
	taplo format --option "indent_string=    "

.PHONY: check-toml
check-toml: ## Check all TOML files.
	taplo format --check --option "indent_string=    "