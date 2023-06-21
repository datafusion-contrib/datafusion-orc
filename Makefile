.PHONY: fmt
fmt: ## Format all the Rust code.
	cargo fmt --all


.PHONY: clippy
clippy: ## Check clippy rules.
	cargo clippy --workspace --all-targets -- -D warnings
