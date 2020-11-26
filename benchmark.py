import dbm
import json
import logging
import os
import os.path
import time
from collections import defaultdict
from contextlib import redirect_stdout, suppress
from random import randrange

import pysos
from genutility.iter import batch
from genutility.time import MeasureTime
from pytablewriter import MarkdownTableWriter
from sqlitedict import SqliteDict

from lmdbm import Lmdb, __version__
from lmdbm.lmdbm import remove_lmdbm


class JsonLmdb(Lmdb):
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

def run_bench(N, db_tpl):

	batchsize = 1000
	LMDB_FILE = db_tpl.format("lmdb")
	PYSOS_FILE = db_tpl.format("pysos")
	SQLITEDICT_FILE = db_tpl.format("sqlitedict")
	DBM_FILE = db_tpl.format("dbm")

	remove_lmdbm(LMDB_FILE)
	with suppress(FileNotFoundError):
		os.unlink(PYSOS_FILE)
	with suppress(FileNotFoundError):
		os.unlink(SQLITEDICT_FILE)
	remove_dbm(DBM_FILE)
	ret = defaultdict(dict)

	# writes

	""" # without batch
	with PrintStatementTime("lmdb (no batch) {} writes: {{delta:.02f}}".format(N)):
		db = JsonLmdb.open(LMDB_FILE, "c")
		for k, v in data(N):
			db[k] = v
		db.close()

	remove_lmdbm(LMDB_FILE)
	"""

	with MeasureTime() as t:
		with JsonLmdb.open(LMDB_FILE, "c") as db:
			for pairs in batch(data(N), batchsize):
				db.update(pairs)
	ret["lmdb"]["write"] = t.get()
	print("lmdb batch write", N, t.get())

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
		with SqliteDict(SQLITEDICT_FILE) as db:
			for pairs in batch(data(N), batchsize):
				db.update(pairs)
				db.commit()
	ret["sqlitedict"]["write"] = t.get()
	print("sqlitedict batch write", N, t.get())

	with MeasureTime() as t:
		with dbm.open(DBM_FILE, "c") as db:
			for k, v in data(N):
				db[k] = json.dumps(v)
	ret["dbm"]["write"] = t.get()
	print("dbm write", N, t.get())

	# reads

	with MeasureTime() as t:
		with JsonLmdb.open(LMDB_FILE, "r") as db:
			for k in allkeys(N):
				a = db[k]
	#ret["lmdb"]["read"] = t.get()
	print("lmdb cont read", N, t.get())

	with MeasureTime() as t:
		with JsonLmdb.open(LMDB_FILE, "r") as db:
			for k in randkeys(N, N):
				a = db[k]
	ret["lmdb"]["read"] = t.get()
	print("lmdb rand read", N, t.get())

	with open(os.devnull, "w") as devnull:  # mute annoying "free lines" output
		with redirect_stdout(devnull):
			with MeasureTime() as t:
				db = pysos.Dict(PYSOS_FILE)
				for k in randkeys(N, N):
					a = db[k]
				db.close()
	ret["pysos"]["read"] = t.get()
	print("pysos read", N, t.get())

	with MeasureTime() as t:
		with SqliteDict(SQLITEDICT_FILE) as db:
			for k in randkeys(N, N):
				a = db[k]
	ret["sqlitedict"]["read"] = t.get()
	print("sqlitedict read", N, t.get())

	with MeasureTime() as t:
		with dbm.open(DBM_FILE, "r") as db:
			for k in randkeys(N, N):
				a = json.loads(db[k])
	ret["dbm"]["read"] = t.get()
	print("dbm read", N, t.get())

	return ret

def bench(base):

	with suppress(FileExistsError):
		os.mkdir(base)

	ret = {}
	db_tpl = os.path.join(base, "test_{}.db")

	for num in [10, 100, 10**3, 10**4, 10**5, 10**6]:
		ret[num] = run_bench(num, db_tpl)

	return ret

def print_markdown_table(results, method):

	for k, v in results.items():
		headers = list(v.keys())
		break

	value_matrix = []
	for k, v in results.items():
		row = [k]
		for h in headers:
			row.append(v[h][method])
		value_matrix.append(row)

	headers = ["items"] + headers

	writer = MarkdownTableWriter(table_name=method, headers=headers, value_matrix=value_matrix)
	writer.write_table()

if __name__ == "__main__":

	from argparse import ArgumentParser
	parser = ArgumentParser()
	parser.add_argument("outpath", default="bench-dbs", help="Directory to store temporary benchmarking databases")
	parser.add_argument("--version", action="version", version=__version__)
	args = parser.parse_args()

	a = bench(args.outpath)
	print_markdown_table(a, "write")
	print_markdown_table(a, "read")
	b = bench(args.outpath)
	print_markdown_table(b, "write")
	print_markdown_table(b, "read")
