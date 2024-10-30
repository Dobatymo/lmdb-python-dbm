import dbm.dumb
import json
import os
import os.path
import pathlib
import pickle  # nosec
import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import closing, suppress
from random import randrange
from typing import Any, Callable, ContextManager, DefaultDict, Dict, Iterable, List, Sequence, TextIO

import pysos
import rocksdict
import semidbm
import sqlitedict
import unqlite
import vedis
import shelve
from genutility.iter import batch
from genutility.time import MeasureTime
from pytablewriter import MarkdownTableWriter

import lmdbm
import lmdbm.lmdbm

ResultsDict = Dict[int, Dict[str, Dict[str, float]]]

# Do not continue benchmark if the current
# step requires more seconds than MAX_TIME
MAX_TIME = 10
BATCH_SIZE = 10000


class BaseBenchmark(ABC):
    def __init__(self, db_tpl, db_type):
        self.available = True
        self.batch_available = True
        self.path = db_tpl.format(db_type)
        self.name = db_type
        self.write = -1
        self.batch = -1
        self.read = -1
        self.combined = -1

    @abstractmethod
    def open(self) -> ContextManager:
        """Open the database"""

        pass

    def commit(self) -> None:  # noqa: B027
        """Commit the changes, if it is not done automatically"""

        pass

    def purge(self) -> None:
        """Remove the database file(s)"""

        with suppress(FileNotFoundError):
            os.unlink(self.path)

    def encode(self, value: Any) -> Any:
        """Convert Python objects to database-capable ones"""

        return value

    def decode(self, value: Any) -> Any:
        """Convert database values to Python objects"""

        return value

    def measure_writes(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for key, value in self.generate_data(N):
                if t.get() > MAX_TIME:
                    break
                db[key] = self.encode(value)
                self.commit()
        if t.get() < MAX_TIME:
            self.write = t.get()
        self.print_time("write", N, t)

    def measure_batch(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for pairs in batch(self.generate_data(N), BATCH_SIZE):
                if t.get() > MAX_TIME:
                    break
                db.update({key: self.encode(value) for key, value in pairs})
                self.commit()
        if t.get() < MAX_TIME:
            self.batch = t.get()
        self.print_time("batch write", N, t)

    def measure_reads(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for key in self.random_keys(N, N):
                if t.get() > MAX_TIME:
                    break
                self.decode(db[key])
        if t.get() < MAX_TIME:
            self.read = t.get()
        self.print_time("read", N, t)

    def measure_combined(self, read=1, write=10, repeat=100) -> None:
        with MeasureTime() as t, self.open() as db:
            for _ in range(repeat):
                if t.get() > MAX_TIME:
                    break
                for key, value in self.generate_data(read):
                    db[key] = self.encode(value)
                    self.commit()
                for key in self.random_keys(10, write):
                    self.decode(db[key])
        if t.get() < MAX_TIME:
            self.combined = t.get()
        self.print_time("combined", (read + write) * repeat, t)

    def database_is_built(self):
        return self.batch >= 0 or self.write >= 0

    def print_time(self, measure_type, numbers, t):
        print(f"{self.name:<20s} {measure_type:<15s} {str(numbers):<10s} {t.get():10.5f}")

    @staticmethod
    def generate_data(size):
        for i in range(size):
            yield "key_" + str(i), {"some": "object_" + str(i)}

    @staticmethod
    def random_keys(num, size):
        for _ in range(num):
            yield "key_" + str(randrange(0, size))  # nosec


class JsonEncodedBenchmark(BaseBenchmark):
    def encode(self, value):
        return json.dumps(value)

    def decode(self, value):
        return json.loads(value)


class DummyPickleBenchmark(BaseBenchmark):
    class MyDict(dict):
        def close(self):
            pass

    def __init__(self, db_tpl):
        super().__init__(db_tpl, "dummypickle")
        self.native_dict = None

    def open(self):
        if pathlib.Path(self.path).exists():
            with open(self.path, "rb") as f:
                self.native_dict = self.MyDict(pickle.load(f))  # nosec
        else:
            self.native_dict = self.MyDict()
        return closing(self.native_dict)

    def commit(self):
        tmp_file = self.path + ".tmp"
        with open(tmp_file, "wb") as f:
            pickle.dump(self.native_dict, f)
        shutil.move(tmp_file, self.path)


class DummyJsonBenchmark(BaseBenchmark):
    class MyDict(dict):
        def close(self):
            pass

    def __init__(self, db_tpl):
        super().__init__(db_tpl, "dummyjson")
        self.native_dict = None

    def open(self):
        if pathlib.Path(self.path).exists():
            with open(self.path) as f:
                self.native_dict = self.MyDict(json.load(f))
        else:
            self.native_dict = self.MyDict()
        return closing(self.native_dict)

    def commit(self):
        tmp_file = self.path + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(self.native_dict, f, ensure_ascii=False, check_circular=False, sort_keys=False)
        shutil.move(tmp_file, self.path)


class DumbDbmBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "dbm.dumb")

    def open(self):
        return dbm.dumb.open(self.path, "c")

    def purge(self):
        with suppress(FileNotFoundError):
            os.unlink(self.path + ".dat")
        with suppress(FileNotFoundError):
            os.unlink(self.path + ".bak")
        with suppress(FileNotFoundError):
            os.unlink(self.path + ".dir")


class SemiDbmBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "semidbm")
        self.batch_available = False

    def open(self):
        return closing(semidbm.open(self.path, "c"))

    def purge(self):
        with suppress(FileNotFoundError):
            os.unlink(self.path + "/data")
        with suppress(FileNotFoundError):
            os.rmdir(self.path)


class LdbmBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "lmdbm")

    def open(self):
        return lmdbm.Lmdb.open(self.path, "c")

    def purge(self):
        lmdbm.lmdbm.remove_lmdbm(self.path)


class PysosBenchmark(BaseBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "pysos")
        self.batch_available = False

    def open(self):
        return closing(pysos.Dict(self.path))


class SqliteAutocommitBenchmark(BaseBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "sqlite-autocommit")

    def open(self):
        return sqlitedict.SqliteDict(self.path, autocommit=True)


class SqliteWalBenchmark(BaseBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "sqlite-wal")

    def open(self):
        return sqlitedict.SqliteDict(self.path, autocommit=True, journal_mode="WAL")


class SqliteBatchBenchmark(BaseBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "sqlite-batch")
        self.db = None

    def open(self):
        self.db = sqlitedict.SqliteDict(self.path, autocommit=False)
        return self.db

    def commit(self):
        self.db.commit()


class GnuDbmBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "dbm.gnu")
        try:
            import dbm.gnu

            self.gnu_dbm = dbm.gnu
        except ImportError:
            self.available = False
        self.batch_available = False

    def open(self):
        return self.gnu_dbm.open(self.path, "c")


class ShelveBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "shelve")

    def open(self):
        return shelve.open(self.path)


class VedisBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "vedis")

    def open(self):
        return vedis.Vedis(self.path)


class UnqliteBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "unqlite")

    def open(self):
        return unqlite.UnQLite(self.path)


class RocksdictBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "rocksdict")
        self.batch_available = False

    def open(self):
        return closing(rocksdict.Rdict(self.path))

    def purge(self):
        rocksdict.Rdict.destroy(self.path)


BENCHMARK_CLASSES = [
    LdbmBenchmark,
    VedisBenchmark,
    UnqliteBenchmark,
    RocksdictBenchmark,
    GnuDbmBenchmark,
    ShelveBenchmark,
    SemiDbmBenchmark,
    PysosBenchmark,
    DumbDbmBenchmark,
    SqliteWalBenchmark,
    SqliteAutocommitBenchmark,
    SqliteBatchBenchmark,
    DummyPickleBenchmark,
    DummyJsonBenchmark,
]


def run_bench(N, db_tpl) -> Dict[str, Dict[str, float]]:
    benchmarks = [C(db_tpl) for C in BENCHMARK_CLASSES]

    for benchmark in benchmarks:
        if not benchmark.available:
            continue
        benchmark.purge()
        benchmark.measure_writes(N)
        if benchmark.batch_available:
            benchmark.purge()
            benchmark.measure_batch(N)
        if benchmark.database_is_built():
            benchmark.measure_reads(N)
            benchmark.measure_combined(read=1, write=10, repeat=100)

    ret: DefaultDict[str, Dict[str, float]] = defaultdict(dict)
    for benchmark in benchmarks:
        ret[benchmark.name]["read"] = benchmark.read
        ret[benchmark.name]["write"] = benchmark.write
        ret[benchmark.name]["batch"] = benchmark.batch
        ret[benchmark.name]["combined"] = benchmark.combined

    return ret


def bench(base: str, nums: Iterable[int]) -> ResultsDict:
    with suppress(FileExistsError):
        os.mkdir(base)

    ret = {}
    db_tpl = os.path.join(base, "test_{}.db")

    for num in nums:
        print("")
        ret[num] = run_bench(num, db_tpl)

    return ret


def write_markdown_table(stream: TextIO, results: ResultsDict, method: str):
    for v in results.values():
        headers = list(v.keys())
        break

    value_matrix = []
    for k, v in results.items():
        row = [str(k)]
        for h in headers:
            value = v[h].get(method)
            if value is None or value < 0:
                new_value = "-"
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

    from genutility.rich import Progress
    from rich.progress import Progress as RichProgress

    parser = ArgumentParser()
    parser.add_argument("--outpath", default="bench-dbs", help="Directory to store temporary benchmarking databases")
    parser.add_argument("--version", action="version", version=lmdbm.__version__)
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

    results: List[ResultsDict] = []

    with RichProgress() as progress:
        p = Progress(progress)
        for _ in p.track(range(args.bestof)):
            results.append(bench(args.outpath, args.sizes))

    if args.bestof == 1:
        best_results = results[0]
    else:
        best_results = merge_results(results)

    with open(args.outfile, "w", encoding="utf-8") as fw:
        write_markdown_table(fw, best_results, "write")
        write_markdown_table(fw, best_results, "batch")
        write_markdown_table(fw, best_results, "read")
        write_markdown_table(fw, best_results, "combined")
