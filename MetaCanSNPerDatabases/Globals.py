

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging
from typing import Generator, Callable, Iterable, Self, overload, final, Literal, Any

LOGGER = logging.Logger()
LOGGER.addHandler(logging.FileHandler(os.devnull))

DATABASE_VERSIONS : dict[str,int] = {

}
STRICT : bool = False

SOURCES = ["https://github.com/FOI-Bioinformatics/CanSNPer2-data/raw/master/database/{databaseName}"]