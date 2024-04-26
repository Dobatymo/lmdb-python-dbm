import logging
from collections.abc import Mapping, MutableMapping
from gzip import compress, decompress
from pathlib import Path
from sys import exit
from typing import Any, Generic, Iterator, List, Optional, Tuple, TypeVar, Union

import lmdb
from typing_extensions import Self

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")

logger = logging.getLogger(__name__)

_DEFAULT = object()


class error(Exception):
    pass


class MissingOk:
    # for python < 3.8 compatibility

    def __init__(self, ok: bool) -> None:
        self.ok = ok

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if isinstance(exc_value, FileNotFoundError) and self.ok:
            return True


def remove_lmdbm(file: str, missing_ok: bool = True) -> None:
    base = Path(file)
    with MissingOk(missing_ok):
        (base / "data.mdb").unlink()
    with MissingOk(missing_ok):
        (base / "lock.mdb").unlink()
    with MissingOk(missing_ok):
        base.rmdir()


class Lmdb(MutableMapping, Generic[KT, VT]):
    autogrow_error = "Failed to grow LMDB ({}). Is there enough disk space available?"
    autogrow_msg = "Grew database (%s) map size to %s"

    def __init__(self, env: lmdb.Environment, autogrow: bool) -> None:
        self.env = env
        self.autogrow = autogrow

    @classmethod
    def open(
        cls, file: str, flag: str = "r", mode: int = 0o755, map_size: int = 2**20, autogrow: bool = True, **kwargs
    ) -> "Lmdb":
        """
        Opens the database `file`.
        `flag`: r (read only, existing), w (read and write, existing),
                c (read, write, create if not exists), n (read, write, overwrite existing)
        `map_size`: Initial database size. Defaults to 2**20 (1MB).
        `autogrow`: Automatically grow the database size when `map_size` is exceeded.
                WARNING: Set this to `False` for multi-process write access.
        `**kwargs`: All other keyword arguments are passed through to `lmdb.open`.
        """

        if flag == "r":  # Open existing database for reading only (default)
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=True, create=False, mode=mode, **kwargs)
        elif flag == "w":  # Open existing database for reading and writing
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=False, mode=mode, **kwargs)
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=True, mode=mode, **kwargs)
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            remove_lmdbm(file)
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=True, mode=mode, **kwargs)
        else:
            raise ValueError("Invalid flag")

        return cls(env, autogrow)

    @property
    def map_size(self) -> int:
        return self.env.info()["map_size"]

    @map_size.setter
    def map_size(self, value: int) -> None:
        self.env.set_mapsize(value)

    def _pre_key(self, key: KT) -> bytes:
        if isinstance(key, bytes):
            return key
        elif isinstance(key, str):
            return key.encode("Latin-1")

        raise TypeError(key)

    def _post_key(self, key: bytes) -> KT:
        return key

    def _pre_value(self, value: VT) -> bytes:
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("Latin-1")

        raise TypeError(value)

    def _post_value(self, value: bytes) -> VT:
        return value

    def __getitem__(self, key: KT) -> VT:
        with self.env.begin() as txn:
            value = txn.get(self._pre_key(key))
        if value is None:
            raise KeyError(key)
        return self._post_value(value)

    def __setitem__(self, key: KT, value: VT) -> None:
        k = self._pre_key(key)
        v = self._pre_value(value)
        for _i in range(12):
            try:
                with self.env.begin(write=True) as txn:
                    txn.put(k, v)
                    return
            except lmdb.MapFullError:
                if not self.autogrow:
                    raise
                new_map_size = self.map_size * 2
                self.map_size = new_map_size
                logger.info(self.autogrow_msg, self.env.path(), new_map_size)

        exit(self.autogrow_error.format(self.env.path()))

    def __delitem__(self, key: KT) -> None:
        with self.env.begin(write=True) as txn:
            txn.delete(self._pre_key(key))

    def keys(self) -> Iterator[KT]:
        with self.env.begin() as txn:
            for key in txn.cursor().iternext(keys=True, values=False):
                yield self._post_key(key)

    def items(self) -> Iterator[Tuple[KT, VT]]:
        with self.env.begin() as txn:
            for key, value in txn.cursor().iternext(keys=True, values=True):
                yield (self._post_key(key), self._post_value(value))

    def values(self) -> Iterator[VT]:
        with self.env.begin() as txn:
            for value in txn.cursor().iternext(keys=False, values=True):
                yield self._post_value(value)

    def __contains__(self, key: KT) -> bool:
        with self.env.begin() as txn:
            value = txn.get(self._pre_key(key))
        return value is not None

    def __iter__(self) -> Iterator[KT]:
        return self.keys()

    def __len__(self) -> int:
        with self.env.begin() as txn:
            return txn.stat()["entries"]

    def pop(self, key: KT, default: Union[VT, T] = _DEFAULT) -> Union[VT, T]:
        with self.env.begin(write=True) as txn:
            value = txn.pop(self._pre_key(key))
        if value is None:
            return default
        return self._post_value(value)

    def update(self, __other: Any = (), **kwds: VT) -> None:  # python3.8 only: update(self, other=(), /, **kwds)
        # fixme: `kwds`

        # note: benchmarking showed that there is no real difference between using lists or iterables
        # as input to `putmulti`.
        # lists: Finished 14412594 in 253496 seconds.
        # iter:  Finished 14412594 in 256315 seconds.

        # save generated lists in case the insert fails and needs to be retried
        # for performance reasons, but mostly because `__other` could be an iterable
        # which would already be exhausted on the second try
        pairs_other: Optional[List[Tuple[bytes, bytes]]] = None
        pairs_kwds: Optional[List[Tuple[bytes, bytes]]] = None

        for _i in range(12):
            try:
                with self.env.begin(write=True) as txn:
                    with txn.cursor() as curs:
                        if isinstance(__other, Mapping):
                            pairs_other = pairs_other or [
                                (self._pre_key(key), self._pre_value(__other[key])) for key in __other
                            ]
                            curs.putmulti(pairs_other)
                        elif hasattr(__other, "keys"):
                            pairs_other = pairs_other or [
                                (self._pre_key(key), self._pre_value(__other[key])) for key in __other.keys()
                            ]
                            curs.putmulti(pairs_other)
                        else:
                            pairs_other = pairs_other or [
                                (self._pre_key(key), self._pre_value(value)) for key, value in __other
                            ]
                            curs.putmulti(pairs_other)

                        pairs_kwds = pairs_kwds or [
                            (self._pre_key(key), self._pre_value(value)) for key, value in kwds.items()
                        ]
                        curs.putmulti(pairs_kwds)

                        return
            except lmdb.MapFullError:
                if not self.autogrow:
                    raise
                new_map_size = self.map_size * 2
                self.map_size = new_map_size
                logger.info(self.autogrow_msg, self.env.path(), new_map_size)

        exit(self.autogrow_error.format(self.env.path()))

    def sync(self) -> None:
        self.env.sync()

    def close(self) -> None:
        self.env.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args):
        self.close()


class LmdbGzip(Lmdb):
    def __init__(self, env, autogrow: bool, compresslevel: int = 9):
        Lmdb.__init__(self, env, autogrow)
        self.compresslevel = compresslevel

    def _pre_value(self, value: VT) -> bytes:
        value = Lmdb._pre_value(self, value)
        return compress(value, self.compresslevel)

    def _post_value(self, value: bytes) -> VT:
        return decompress(value)


def open(file, flag="r", mode=0o755, **kwargs):
    return Lmdb.open(file, flag, mode, **kwargs)
