

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging
from typing import Generator, Callable, Iterable, Self, overload, final, Literal, Any

LOGGER = logging.Logger("MetaCanSNPerDatabases")
LOGGER.addHandler(logging.FileHandler("MetaCanSNPerDatabases.log"))

DATABASE_VERSIONS : dict[str,int] = {
    "7630f33662e27489b7bb7b3b121ca4ff" : 1, # Legacy CanSNPer
    "e585ee1f5ed2fc1d00efeea7a146e1b1" : 2  # MetaCanSNPer Alpha version
}
CURRENT_VERSION = 1
STRICT : bool = False

SOURCES = [
    "https://github.com/XJ-04561/MetaCanSNPer-data/raw/master/database/{databaseName}",
    "https://github.com/FOI-Bioinformatics/CanSNPer2-data/raw/master/database/{databaseName}" # Legacy CanSNPer
]