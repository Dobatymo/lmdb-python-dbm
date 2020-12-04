from pathlib import Path

from genutility.test import MyTestCase
from lmdb import Error

from lmdbm import Lmdb
from lmdbm.lmdbm import remove_lmdbm


class LmdbmTests(MyTestCase):

	_name = "./test.db"

	_dict = {
		b"a": b"Python:",
		b"b": b"Programming",
		b"c": b"the",
		b"d": b"way",
		b"f": b"Guido",
		b"g": b"intended",
	}

	def _init_db(self):
		with Lmdb.open(self._name, "n") as db:
			for k, v in self._dict.items():
				db[k] = v

	def _delete_db(self):
		remove_lmdbm(self._name, False)

	def test_mem_grow(self):
		with Lmdb.open(self._name, "n", map_size=1024) as db:

			key = b"asd"
			value = b"asd"*1000

			db[key] = value
			assert db.setdefault(key, b"asd") == value
			assert db[key] == value
			assert db.get(key) == value

		self._delete_db()

	def test_mem_grow_batch(self):

		value = b"asd"*1000

		def data():
			yield "key_1", value
			yield "key_2", value

		with Lmdb.open(self._name, "n", map_size=1024) as db:

			db.update(data())
			assert db["key_1"] == value
			assert db["key_2"] == value

		self._delete_db()

	def test_missing_read_only(self):

		with self.assertRaises(Error):
			with Lmdb.open(self._name, "r", map_size=1024) as db:
				db["key"] = "value"

		assert not Path(self._name).exists()

	def test_modify(self):

		self._init_db()
		with Lmdb.open(self._name, "c") as f:
			self._dict[b"g"] = f[b"g"] = b"indented"
			self.assertUnorderedMappingEqual(f, self._dict)

			self.assertEqual(f.setdefault(b"xxx", b"foo"), b"foo")
			self.assertEqual(f[b"xxx"], b"foo")

		self._delete_db()

if __name__ == "__main__":
	import unittest
	unittest.main()
