

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging, shutil, sys
from typing import Generator, Callable, Iterable, Self, overload, final, Literal, LiteralString, Any, TextIO, BinaryIO, Never

LOGGER = logging.Logger("MetaCanSNPerDatabases", level=logging.WARNING)

type Mode = Literal["r", "w"]
type ReadMode = Literal["r"]
type WriteMode = Literal["w"]
type Direction = Literal["DESC","ASC"]

DATABASE_VERSIONS : dict[str,int] = {
    "7630f33662e27489b7bb7b3b121ca4ff" : 1, # Legacy CanSNPer
    "78b97cab2afb2f5b9081598374439ac6" : 2  # MetaCanSNPer Alpha version
}
LEGACY_VERSION = 0
CURRENT_VERSION = 2
CURRENT_HASH = "78b97cab2afb2f5b9081598374439ac6"
STRICT : bool = False

SOURCES = [
    "https://github.com/XJ-04561/MetaCanSNPer-data/raw/master/database/{databaseName}", # MetaCanSNPer
    "https://github.com/FOI-Bioinformatics/CanSNPer2-data/raw/master/database/{databaseName}" # Legacy CanSNPer
]