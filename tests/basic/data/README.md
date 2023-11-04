## Generate data

```bash
python3 -m venv venv
venv/bin/pip install -U pip
# for write.py
venv/bin/pip install -U pyorc
# for generate_orc.py
venv/bin/pip install -U pyspark

./venv/bin/python write.py
./venv/bin/python generate_orc.py

cargo test
```
