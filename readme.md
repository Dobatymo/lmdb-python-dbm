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

## Benchmarks

See `benchmark.py`.

### 1. run

#### continuous writes in seconds
| items | lmdbm | pysos |sqlitedict|  dbm   |
|------:|------:|------:|---------:|-------:|
|     10| 0.0160| 0.0000|   0.01500|  0.0160|
|    100| 0.0000| 0.0000|   0.03100|  0.0940|
|   1000| 0.1090| 0.0630|   0.35900|  1.7310|
|  10000| 0.1560| 0.5770|   3.80700| 13.2130|
| 100000| 1.4820| 5.0700|  23.04100| 96.0030|
|1000000|19.2820|51.5120| 212.75400|908.4870|

#### random reads in seconds
| items | lmdbm | pysos |sqlitedict|  dbm   |
|------:|------:|------:|---------:|-------:|
|     10| 0.0000| 0.0000|   0.01500|  0.0000|
|    100| 0.0000| 0.0000|   0.07800|  0.1250|
|   1000| 0.0320| 0.0310|   1.06100|  0.3740|
|  10000| 0.2340| 0.3280|   7.28500|  2.1220|
| 100000| 1.9650| 3.7910|  59.67100| 20.1080|
|1000000|17.9710|40.0460| 478.33000|201.7250|

### 2. run

#### continuous writes in seconds
| items | lmdbm | pysos |sqlitedict|   dbm   |
|------:|------:|------:|---------:|--------:|
|     10| 0.0000| 0.0000|   0.01600|  0.01600|
|    100| 0.0000| 0.0000|   0.01600|  0.09300|
|   1000| 0.0320| 0.0460|   0.21900|  0.84200|
|  10000| 0.1560| 2.6210|   2.09100|  8.42400|
| 100000| 1.5130| 4.9140|  20.71700| 86.86200|
|1000000|18.1430|48.0950| 208.88600|878.16000|

#### random reads in seconds
| items | lmdbm | pysos |sqlitedict|  dbm   |
|------:|------:|------:|---------:|-------:|
|     10| 0.0000|  0.000|    0.0000|  0.0000|
|    100| 0.0000|  0.000|    0.0630|  0.0150|
|   1000| 0.0150|  0.016|    0.4990|  0.1720|
|  10000| 0.1720|  0.250|    4.2430|  1.7470|
| 100000| 1.7470|  3.588|   49.3120| 18.4240|
|1000000|17.8150| 38.454|  516.3170|196.8730|
