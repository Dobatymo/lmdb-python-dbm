import dbm.dumb

try:
    import dbm.gnu

    gdbm = True
except ModuleNotFoundError:
    gdbm = False

import json
import os
import os.path
from collections import defaultdict
from contextlib import redirect_stdout, suppress
from random import randrange
from typing import Callable, DefaultDict, Dict, Iterable, List, Sequence, TextIO

import pysos
import semidbm
from genutility.iter import batch
from genutility.time import MeasureTime
from pytablewriter import MarkdownTableWriter
from sqlitedict import SqliteDict
from unqlite import UnQLite
from vedis import Vedis
from wtdbm import WiredTigerDBM
from wtdbm.wtdbm import remove_wtdbm

from lmdbm import Lmdb, __version__
from lmdbm.lmdbm import remove_lmdbm

ResultsDict = Dict[int, Dict[str, Dict[str, float]]]


class JsonLmdb(Lmdb):
    def _pre_value(self, value):
        return json.dumps(value).encode("utf-8")

    def _post_value(self, value):
        return json.loads(value.decode("utf-8"))


class JsonWtdbm(WiredTigerDBM):
    def _pre_value(self, value):
        return json.dumps(value).encode("utf-8")

    def _post_value(self, value):
        return json.loads(value.decode("utf-8"))


def data(size):
    for i in range(size):
        yield "key_" + str(i), {"some": "object_" + str(i)}


def randkeys(num, size):
    for i in range(num):
        yield "key_" + str(randrange(0, size))  # nosec


def allkeys(num):
    for i in range(num):
        yield "key_" + str(i)


def remove_dbm(path):
    with suppress(FileNotFoundError):
        os.unlink(path + ".dat")
    with suppress(FileNotFoundError):
        os.unlink(path + ".bak")
    with suppress(FileNotFoundError):
        os.unlink(path + ".dir")


def remove_semidbm(path):
    with suppress(FileNotFoundError):
        os.unlink(path + "/data")
    with suppress(FileNotFoundError):
        os.rmdir(path)


def run_bench(N, db_tpl) -> Dict[str, Dict[str, float]]:

    batchsize = 1000

    LMDBM_FILE = db_tpl.format("lmdbm")
    LMDBM_BATCH_FILE = db_tpl.format("lmdbm-batch")
    PYSOS_FILE = db_tpl.format("pysos")
    SQLITEDICT_FILE = db_tpl.format("sqlitedict")
    SQLITEDICT_BATCH_FILE = db_tpl.format("sqlitedict-batch")
    DBM_DUMB_FILE = db_tpl.format("dbm.dumb")
    DBM_GNU_FILE = db_tpl.format("dbm.gnu")
    SEMIDBM_FILE = db_tpl.format("semidbm")
    VEDIS_FILE = db_tpl.format("vedis")
    VEDIS_BATCH_FILE = db_tpl.format("vedis-batch")
    UNQLITE_FILE = db_tpl.format("unqlite")
    UNQLITE_BATCH_FILE = db_tpl.format("unqlite-batch")
    WTDBM_FILE = db_tpl.format("wtdbm")
    WTDBM_BATCH_FILE = db_tpl.format("wtdbm-batch")

    remove_lmdbm(LMDBM_FILE)
    remove_lmdbm(LMDBM_BATCH_FILE)
    remove_wtdbm(WTDBM_FILE)
    remove_wtdbm(WTDBM_BATCH_FILE)
    with suppress(FileNotFoundError):
        os.unlink(PYSOS_FILE)
    with suppress(FileNotFoundError):
        os.unlink(SQLITEDICT_FILE)
    with suppress(FileNotFoundError):
        os.unlink(SQLITEDICT_BATCH_FILE)
    remove_dbm(DBM_DUMB_FILE)
    remove_semidbm(SEMIDBM_FILE)
    with suppress(FileNotFoundError):
        os.unlink(VEDIS_FILE)
    with suppress(FileNotFoundError):
        os.unlink(VEDIS_BATCH_FILE)
    with suppress(FileNotFoundError):
        os.unlink(UNQLITE_FILE)
    with suppress(FileNotFoundError):
        os.unlink(UNQLITE_BATCH_FILE)

    ret: DefaultDict[str, Dict[str, float]] = defaultdict(dict)

    # writes

    with MeasureTime() as t:
        with JsonLmdb.open(LMDBM_FILE, "c") as db:
            for k, v in data(N):
                db[k] = v
    ret["lmdbm"]["write"] = t.get()
    print("lmdbm write", N, t.get())

    with MeasureTime() as t:
        with JsonLmdb.open(LMDBM_BATCH_FILE, "c") as db:
            for pairs in batch(data(N), batchsize):
                db.update(pairs)
    ret["lmdbm-batch"]["write"] = t.get()
    print("lmdbm-batch write", N, t.get())

    with open(os.devnull, "w") as devnull:  # mute annoying "free lines" output
        with redirect_stdout(devnull):
            with MeasureTime() as t:
                db = pysos.Dict(PYSOS_FILE)
                for k, v in data(N):
                    db[k] = v
                db.close()
    ret["pysos"]["write"] = t.get()
    print("pysos write", N, t.get())

    with MeasureTime() as t:
        with SqliteDict(SQLITEDICT_FILE, autocommit=True) as db:
            for k, v in data(N):
                db[k] = v
    ret["sqlitedict"]["write"] = t.get()
    print("sqlitedict write", N, t.get())

    with MeasureTime() as t:
        with SqliteDict(SQLITEDICT_BATCH_FILE, autocommit=False) as db:
            for pairs in batch(data(N), batchsize):
                db.update(pairs)
                db.commit()
    ret["sqlitedict-batch"]["write"] = t.get()
    print("sqlitedict-batch write", N, t.get())

    with MeasureTime() as t:
        with JsonWtdbm.open(WTDBM_FILE, "c") as db:
            for k, v in data(N):
                db[k] = v
    ret["wiredtiger-dbm"]["write"] = t.get()
    print("wiredtiger-dbm write", N, t.get())

    with MeasureTime() as t:
        with JsonWtdbm.open(WTDBM_BATCH_FILE, "c") as db:
            for pairs in batch(data(N), batchsize):
                db.update(pairs)
    ret["wiredtiger-dbm-batch"]["write"] = t.get()
    print("wiredtiger-dbm-batch write", N, t.get())

    with MeasureTime() as t:
        with dbm.dumb.open(DBM_DUMB_FILE, "c") as db:
            for k, v in data(N):
                db[k] = json.dumps(v)
    ret["dbm.dumb"]["write"] = t.get()
    print("dbm.dumb write", N, t.get())

    if gdbm:
        with MeasureTime() as t:
            with dbm.gnu.open(DBM_GNU_FILE, "c") as db:
                for k, v in data(N):
                    db[k] = json.dumps(v)
        ret["dbm.gnu"]["write"] = t.get()
        print("dbm.gnu write", N, t.get())

    with MeasureTime() as t:
        db = semidbm.open(SEMIDBM_FILE, "c")
        for k, v in data(N):
            db[k] = json.dumps(v)
        db.close()
    ret["semidbm"]["write"] = t.get()
    print("semidbm write", N, t.get())

    with MeasureTime() as t:
        with Vedis(VEDIS_FILE) as db:
            for k, v in data(N):
                db[k] = json.dumps(v)
    ret["vedis"]["write"] = t.get()
    print("vedis write", N, t.get())

    with MeasureTime() as t:
        with Vedis(VEDIS_BATCH_FILE) as db:
            for pairs in batch(data(N), batchsize):
                db.update({k: json.dumps(v) for k, v in pairs})
    ret["vedis-batch"]["write"] = t.get()
    print("vedis-batch write", N, t.get())

    with MeasureTime() as t:
        with UnQLite(UNQLITE_FILE) as db:
            for k, v in data(N):
                db[k] = json.dumps(v)
    ret["unqlite"]["write"] = t.get()
    print("unqlite write", N, t.get())

    with MeasureTime() as t:
        with UnQLite(UNQLITE_BATCH_FILE) as db:
            for pairs in batch(data(N), batchsize):
                db.update({k: json.dumps(v) for k, v in pairs})
    ret["unqlite-batch"]["write"] = t.get()
    print("unqlite-batch write", N, t.get())

    # reads

    with MeasureTime() as t:
        with JsonLmdb.open(LMDBM_FILE, "r") as db:
            for k in allkeys(N):
                db[k]
    # ret["lmdbm"]["read"] = t.get()
    print("lmdbm cont read", N, t.get())

    with MeasureTime() as t:
        with JsonLmdb.open(LMDBM_FILE, "r") as db:
            for k in randkeys(N, N):
                db[k]
    ret["lmdbm"]["read"] = t.get()
    print("lmdbm rand read", N, t.get())

    with open(os.devnull, "w") as devnull:  # mute annoying "free lines" output
        with redirect_stdout(devnull):
            with MeasureTime() as t:
                db = pysos.Dict(PYSOS_FILE)
                for k in randkeys(N, N):
                    db[k]
                db.close()
    ret["pysos"]["read"] = t.get()
    print("pysos read", N, t.get())

    with MeasureTime() as t:
        with SqliteDict(SQLITEDICT_FILE) as db:
            for k in randkeys(N, N):
                db[k]
    ret["sqlitedict"]["read"] = t.get()
    print("sqlitedict read", N, t.get())

    with MeasureTime() as t:
        with JsonWtdbm.open(WTDBM_FILE, "r") as db:
            for k in randkeys(N, N):
                db[k]
    ret["wiredtiger-dbm"]["read"] = t.get()
    print("wiredtiger-dbm read", N, t.get())

    with MeasureTime() as t:
        with dbm.dumb.open(DBM_DUMB_FILE, "r") as db:
            for k in randkeys(N, N):
                json.loads(db[k])
    ret["dbm.dumb"]["read"] = t.get()
    print("dbm.dumb read", N, t.get())

    if gdbm:
        with MeasureTime() as t:
            with dbm.gnu.open(DBM_GNU_FILE, "r") as db:
                for k in randkeys(N, N):
                    json.loads(db[k])
        ret["dbm.gnu"]["read"] = t.get()
        print("dbm.gnu read", N, t.get())

    with MeasureTime() as t:
        db = semidbm.open(SEMIDBM_FILE, "r")
        for k in randkeys(N, N):
            json.loads(db[k])
        db.close()
    ret["semidbm"]["read"] = t.get()
    print("semidbm read", N, t.get())

    with MeasureTime() as t:
        with Vedis(VEDIS_FILE) as db:
            for k in randkeys(N, N):
                json.loads(db[k])
    ret["vedis"]["read"] = t.get()
    print("vedis read", N, t.get())

    with MeasureTime() as t:
        with UnQLite(UNQLITE_FILE) as db:
            for k in randkeys(N, N):
                json.loads(db[k])
    ret["unqlite"]["read"] = t.get()
    print("unqlite read", N, t.get())

    return ret


def bench(base: str, nums: Iterable[int]) -> ResultsDict:

    with suppress(FileExistsError):
        os.mkdir(base)

    ret = {}
    db_tpl = os.path.join(base, "test_{}.db")

    for num in nums:
        ret[num] = run_bench(num, db_tpl)

    return ret


def write_markdown_table(stream: TextIO, results: ResultsDict, method: str):

    for k, v in results.items():
        headers = list(v.keys())
        break

    value_matrix = []
    for k, v in results.items():
        row = [str(k)]
        for h in headers:
            value = v[h].get(method)
            if value is None:
                new_value = ""
            else:
                new_value = format(value, ".04f")
            row.append(new_value)
        value_matrix.append(row)

    headers = ["items"] + headers

    writer = MarkdownTableWriter(table_name=method, headers=headers, value_matrix=value_matrix)
    writer.dump(stream, close_after_write=False)


def _check_same_keys(dicts: Sequence[dict]):
    assert len(dicts) >= 2

    for d in dicts[1:]:
        assert dicts[0].keys() == d.keys()


def merge_results(results: Sequence[ResultsDict], func: Callable = min) -> ResultsDict:

    out: ResultsDict = {}

    _check_same_keys(results)
    for key1 in results[0].keys():
        _check_same_keys([d[key1] for d in results])
        out.setdefault(key1, {})
        for key2 in results[0][key1].keys():
            _check_same_keys([d[key1][key2] for d in results])
            out[key1].setdefault(key2, {})
            for key3 in results[0][key1][key2].keys():
                out[key1][key2][key3] = func(d[key1][key2][key3] for d in results)

    return out


if __name__ == "__main__":

    from argparse import ArgumentParser

    from genutility.iter import progress

    parser = ArgumentParser()
    parser.add_argument("--outpath", default="bench-dbs", help="Directory to store temporary benchmarking databases")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        metavar="N",
        default=[10, 100, 10**3, 10**4, 10**5, 10**6],
        help="Number of records to read/write",
    )
    parser.add_argument("--bestof", type=int, metavar="N", default=3, help="Run N benchmarks")
    parser.add_argument("--outfile", default="benchmarks.md", help="Benchmark results")
    args = parser.parse_args()

    with open(args.outfile, "wt", encoding="utf-8") as fw:
        results: List[ResultsDict] = []

        for _ in progress(range(args.bestof)):
            results.append(bench(args.outpath, args.sizes))

        best_results = merge_results(results)

        write_markdown_table(fw, best_results, "write")
        write_markdown_table(fw, best_results, "read")
