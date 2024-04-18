

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging, shutil, sys, itertools

from typing import (
    Generator, Callable, Iterable, Self, Literal, LiteralString, Any, TextIO,
	BinaryIO, Never, Iterator, TypeVar, Type, get_args, get_origin, ChainMap,
	Union, Type, overload, final, Generic, Dict)

from PseudoPathy import Path, DirectoryPath, FilePath, PathGroup, PathLibrary, PathList
from PseudoPathy.Library import CommonGroups
from PseudoPathy.PathShortHands import *

from MetaCanSNPerDatabases._core.Exceptions import *
class Rest(Iterator): pass
class All(Iterator): pass

ASSERTIONS = [
	SchemaNotEmpty,
	ValidTablesSchema,
	ValidIndexesSchema
]

SQL_TYPES = {
	"INTEGER" : int,
	"TEXT" : str,
	"VARCHAR" : str,
	"CHAR" : str,
	"DATE" : str,
	"DATETIME" : str,
	"DECIMAL" : float,
	"NULL" : None
}

whitespacePattern = re.compile(r"\s+")
sqlite3TypePattern = re.compile(r"^(?P<integer>INTEGER)|(?P<decimal>DECIMAL)|(?P<char>(VAR)?CHAR[(](?P<number>[0-9]*)[)])|(?P<date>DATE)|(?P<datetime>DATETIME)|(?P<text>TEXT)$", re.IGNORECASE)
namePattern = re.compile(r"^[a-zA-Z0-9_\-*]*$")
formatPattern = re.compile(r"[{](.*?)[}]")

LOGGER = logging.Logger("SQLOOP", level=100)

DATABASE_VERSIONS : dict[str,int] = {}

CURRENT_VERSION = 2
CURRENT_TABLES_HASH = ""
CURRENT_INDEXES_HASH = ""

SOURCES = []

class Query: pass
class Comparison: pass
class Column: pass
class Table: pass
class Index: pass
class PrimaryKey: pass
class ForeignKey: pass
class Unique: pass
class Database: pass
class Word: pass
class Aggregate: pass