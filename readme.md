# lmdbm

This is a Python DBM interface style wrapper around [LMDB](http://www.lmdb.tech/doc/) (Lightning Memory-Mapped Database).
It uses the existing lower level Python bindings [py-lmdb](https://lmdb.readthedocs.io).
This is especially useful on Windows, where otherwise `dbm.dumb` is the default `dbm` database.

## Install
- `pip install lmdbm`

## Example
```python
from lmdbm import Lmdb
with Lmdb.open("test.db", "c") as db:
  db[b"key"] = b"value"
  db.update({b"key1": b"value1", b"key2": b"value2"})  # batch insert, uses a single transaction
```
