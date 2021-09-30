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

### Use inheritance to store Python objects using json serialization

```python
import json
from lmdbm import Lmdb

class JsonLmdb(Lmdb):
  def _pre_key(self, value):
    return value.encode("utf-8")
  def _post_key(self, value):
    return value.decode("utf-8")
  def _pre_value(self, value):
    return json.dumps(value).encode("utf-8")
  def _post_value(self, value):
    return json.loads(value.decode("utf-8"))

with JsonLmdb.open("test.db", "c") as db:
  db["key"] = {"some": "object"}
  obj = db["key"]
  print(obj["some"])  # prints "object"
```

## Warning

As of `lmdb==1.2.1` the docs say that calling `lmdb.Environment.set_mapsize` from multiple processes "may cause catastrophic loss of data". If `lmdbm` is used in write mode from multiple processes, set `autogrow=False` and map_size to a large enough value: `Lmdb.open(..., map_size=2**30, autogrow=False)`.

## Benchmarks

See `benchmark.py` and `requirements-bench.txt`. Other storage engines which could be tested: `wiredtiger`, `berkeleydb`.
Storage engines not benchmarked:
	- `tinydb` (because it doesn't have built-in str/bytes keys)

### continuous writes in seconds (best of 3)
| items | lmdbm  |lmdbm-batch|pysos |sqlitedict|sqlitedict-batch|dbm.dumb|semidbm|vedis |vedis-batch|unqlite|unqlite-batch|
|------:|-------:|----------:|-----:|---------:|---------------:|-------:|------:|-----:|----------:|------:|------------:|
|     10|   0.000|      0.015| 0.000|     0.031|           0.000|   0.016|  0.000| 0.000|      0.000|  0.000|        0.000|
|    100|   0.094|      0.000| 0.000|     0.265|           0.016|   0.188|  0.000| 0.000|      0.000|  0.000|        0.000|
|   1000|   1.684|      0.016| 0.015|     3.885|           0.124|   2.387|  0.016| 0.015|      0.015|  0.016|        0.000|
|  10000|  16.895|      0.093| 0.265|    45.334|           1.326|  25.350|  0.156| 0.093|      0.094|  0.094|        0.093|
| 100000| 227.106|      1.030| 2.698|   461.638|          12.964| 238.400|  1.623| 1.388|      1.467|  1.466|        1.357|
|1000000|3482.520|     13.104|27.815|  5851.239|         133.396|2432.945| 16.411|15.693|     15.709| 14.508|       14.103|

### random reads in seconds (best of 3)
| items |lmdbm |lmdbm-batch|pysos |sqlitedict|sqlitedict-batch|dbm.dumb|semidbm| vedis |vedis-batch|unqlite|unqlite-batch|
|------:|-----:|-----------|-----:|---------:|----------------|-------:|------:|------:|-----------|------:|-------------|
|     10| 0.000|           | 0.000|     0.000|                |   0.000|  0.000|  0.000|           |  0.000|             |
|    100| 0.000|           | 0.000|     0.031|                |   0.000|  0.000|  0.000|           |  0.000|             |
|   1000| 0.016|           | 0.015|     0.250|                |   0.109|  0.016|  0.015|           |  0.000|             |
|  10000| 0.109|           | 0.156|     2.558|                |   1.123|  0.171|  0.109|           |  0.109|             |
| 100000| 1.014|           | 2.137|    27.769|                |  11.419|  2.090|  1.170|           |  1.170|             |
|1000000|10.390|           |24.258|   447.613|                | 870.580| 22.838|214.486|           |211.319|             |
