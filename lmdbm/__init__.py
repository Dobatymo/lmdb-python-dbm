"""Python DBM style wrapper around LMDB (Lightning Memory-Mapped Database)"""

from .lmdbm import Lmdb, LmdbGzip, error, open

__version__ = "0.0.6"

__all__ = ["Lmdb", "LmdbGzip", "error", "open", "__version__"]
