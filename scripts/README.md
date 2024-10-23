## Generate data

Setup the virtual environment with dependencies on PyArrow, PySpark and PyOrc
to generate the reference data:

```bash
# Run once
./scripts/setup-venv.sh
./scripts/prepare-test-data.sh
```

Then execute the tests:

```bash
cargo test
```

