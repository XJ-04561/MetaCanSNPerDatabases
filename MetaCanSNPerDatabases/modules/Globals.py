

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging
from typing import Generator, Callable, Iterable, Self, overload, final, Literal, Any

LOGGER = logging.Logger("MetaCanSNPerDatabases")
LOGGER.addHandler(logging.FileHandler("MetaCanSNPerDatabases.log"))

DATABASE_VERSIONS : dict[str,int] = {}
CURRENT_VERSION : str = ""
STRICT : bool = False

SOURCES = [
    "https://github.com/XJ-04561/MetaCanSNPer-data/raw/master/database/{databaseName}",
    "https://github.com/FOI-Bioinformatics/CanSNPer2-data/raw/master/database/{databaseName}"
]