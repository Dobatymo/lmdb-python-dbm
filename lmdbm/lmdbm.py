import logging
from collections.abc import Mapping, MutableMapping
from gzip import compress, decompress
from pathlib import Path
from sys import exit
from typing import TYPE_CHECKING

import lmdb

if TYPE_CHECKING:
	from typing import Any, Iterator, List, Optional, Tuple, TypeVar, Union
	T = TypeVar("T")
	KT = Union[bytes, str] # TypeVar("KT")
	VT = Union[bytes, str] # TypeVar("VT")

class error(Exception):
	pass

class MissingOk(object):

	# for python < 3.8 compatibility

	def __init__(self, ok):
		# type: (bool, ) -> None

		self.ok = ok

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if isinstance(exc_value, FileNotFoundError) and self.ok:
			return True

def remove_lmdbm(file, missing_ok=True):
	# type: (str, bool) -> None

	base = Path(file)
	with MissingOk(missing_ok):
		(base / "data.mdb").unlink()
	with MissingOk(missing_ok):
		(base / "lock.mdb").unlink()
	with MissingOk(missing_ok):
		base.rmdir()

class Lmdb(MutableMapping):

	def __init__(self, env):
		# type: (lmdb.Environment, ) -> None

		self.env = env

	@classmethod
	def open(cls, file, flag="r", mode=0o755, map_size=2**20):
		# type: (str, str, int, int) -> Lmdb

		"""
			Opens the database `file`.
			`flag`: r (read only, existing), w (read and write, existing),
				c (read, write, create if not exists), n (read, write, overwrite existing)
			`map_size`. Initial database size. Defaults to 2**20 (1MB).
		"""

		if flag == "r":  # Open existing database for reading only (default)
			env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=True, create=False, mode=mode)
		elif flag == "w":  # Open existing database for reading and writing
			env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=False, mode=mode)
		elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
			env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=True, mode=mode)
		elif flag == "n":  # Always create a new, empty database, open for reading and writing
			remove_lmdbm(file)
			env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=True, mode=mode)
		else:
			raise ValueError("Invalid flag")

		return cls(env)

	@property
	def map_size(self):
		# type: () -> int

		return self.env.info()["map_size"]

	def _pre_key(self, key):
		# type: (object, ) -> bytes

		if isinstance(key, bytes):
			return key
		elif isinstance(key, str):
			return key.encode("Latin-1")

		raise TypeError

	def _post_key(self, key):
		# type: (bytes, ) -> KT

		return key

	def _pre_value(self, value):
		# type: (VT, ) -> bytes

		if isinstance(value, bytes):
			return value
		elif isinstance(value, str):
			return value.encode("Latin-1")

		raise TypeError

	def _post_value(self, value):
		# type: (bytes, ) -> VT

		return value

	def __getitem__(self, key):
		# type: (KT, ) -> VT

		with self.env.begin() as txn:
			value = txn.get(self._pre_key(key))
		if value is None:
			raise KeyError(key)
		return self._post_value(value)

	def __setitem__(self, key, value):
		# type: (KT, VT) -> None

		k = self._pre_key(key)
		v = self._pre_value(value)
		for i in range(12):
			try:
				with self.env.begin(write=True) as txn:
					txn.put(k, v)
					return
			except lmdb.MapFullError:
				new_map_size = self.map_size * 2
				self.env.set_mapsize(new_map_size)
				logging.info("Grew database map size to %s", new_map_size)
		exit("Failed to grow lmdb")

	def __delitem__(self, key):
		# type: (KT, ) -> None

		with self.env.begin(write=True) as txn:
			txn.delete(self._pre_key(key))

	def keys(self):
		# type: () -> Iterator[KT]

		with self.env.begin() as txn:
			for key in txn.cursor().iternext(keys=True, values=False):
				yield self._post_key(key)

	def items(self):
		# type: () -> Iterator[Tuple[KT, VT]]

		with self.env.begin() as txn:
			for key, value in txn.cursor().iternext(keys=True, values=True):
				yield (self._post_key(key), self._post_value(value))

	def values(self):
		# type: () -> Iterator[VT]

		with self.env.begin() as txn:
			for value in txn.cursor().iternext(keys=False, values=True):
				yield self._post_value(value)

	def __contains__(self, key):
		# type: (object, ) -> bool

		with self.env.begin() as txn:
			value = txn.get(self._pre_key(key))
		return value is not None

	def __iter__(self):
		# type: () -> Iterator[KT]

		return self.keys()

	def __len__(self):
		# type: () -> int

		with self.env.begin() as txn:
			return txn.stat()["entries"]

	def pop(self, key, default=object()):
		# type: (KT, Union[VT, T]) -> Union[VT, T]

		with self.env.begin(write=True) as txn:
			value = txn.pop(self._pre_key(key))
		if value is None:
			return default
		return self._post_value(value)

	def update(self, __other=(), **kwds):  # python3.8 only: update(self, other=(), /, **kwds)
		# type: (Any, **VT) -> None

		# fixme: `kwds`

		# note: benchmarking showed that there is no real difference between using lists or iterables
		# as input to `putmulti`.
		# lists: Finished 14412594 in 253496 seconds.
		# iter:  Finished 14412594 in 256315 seconds.

		# save generated lists in case the insert fails and needs to be retried
		# for performance reasons, but mostly because `__other` could be an iterable
		# which would already be exhausted on the second try
		pairs_other = None  # type: Optional[List[Tuple[bytes, bytes]]]
		pairs_kwds = None  # type: Optional[List[Tuple[bytes, bytes]]]

		for i in range(12):
			try:
				with self.env.begin(write=True) as txn:
					with txn.cursor() as curs:
						if isinstance(__other, Mapping):
							pairs_other = pairs_other or [(self._pre_key(key), self._pre_value(__other[key])) for key in __other]
							curs.putmulti(pairs_other)
						elif hasattr(__other, "keys"):
							pairs_other = pairs_other or [(self._pre_key(key), self._pre_value(__other[key])) for key in __other.keys()]
							curs.putmulti(pairs_other)
						else:
							pairs_other = pairs_other or [(self._pre_key(key), self._pre_value(value)) for key, value in __other]
							curs.putmulti(pairs_other)

						pairs_kwds = pairs_kwds or [(self._pre_key(key), self._pre_value(value)) for key, value in kwds.items()]
						curs.putmulti(pairs_kwds)

						return
			except lmdb.MapFullError:
				new_map_size = self.map_size * 2
				self.env.set_mapsize(new_map_size)
				logging.info("Grew database map size to %s", new_map_size)

		exit("Failed to grow lmdb")

	def sync(self):
		# type: () -> None

		self.env.sync()

	def close(self):
		# type: () -> None

		self.env.close()

	def __enter__(self):
		return self

	def __exit__(self, *args):
		self.close()

class LmdbGzip(Lmdb):

	def __init__(self, env, compresslevel=9):
		Lmdb.__init__(self, env)
		self.compresslevel = compresslevel

	def _pre_value(self, value):
		# type: (VT, ) -> bytes

		value = Lmdb._pre_value(self, value)
		return compress(value, self.compresslevel)

	def _post_value(self, value):
		# type: (bytes, ) -> VT

		return decompress(value)

def open(file, flag="r", mode=0o755):
	return Lmdb.open(file, flag, mode)
