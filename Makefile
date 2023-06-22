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