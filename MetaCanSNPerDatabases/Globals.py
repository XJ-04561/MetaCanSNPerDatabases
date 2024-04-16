

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging, shutil, sys

from typing import (
    Generator, Callable, Iterable, Self, Literal, LiteralString, Any, TextIO,
	BinaryIO, Never, Iterator, TypeVar, Type, get_args, get_origin, ChainMap,
	Union, Type, overload, final)

from PseudoPathy import Path, DirectoryPath, FilePath, PathGroup, PathLibrary, PathList
from PseudoPathy.Library import CommonGroups
from PseudoPathy.PathShortHands import *

from MetaCanSNPerDatabases.core.Exceptions import *

ASSERTIONS = [
	SchemaNotEmpty,
	ValidTablesSchema,
	ValidIndexesSchema
]

pluralPattern = re.compile(r"s$|x$|z$|sh$|ch$")
hiddenPattern = re.compile(r"^_[^_].*")

def pluralize(string : str) -> str:
	match pluralPattern.search(string):
		case None:
			return f"{string}s"
		case _:
			return f"{string}es"

class AutoObject:
	"""Automatically assigns values passed to the constructor to the annotations
	of the object class. Skips 'hidden' attributes starting with only one
	underscore. Meaning, _name would not be assigned a value from the
	construction call, but __name__ would, as it starts with one, and not two
	underscores."""
	def __init__(self, *args, **kwargs):
		i = 0
		for name, typeHint in self.__annotations__.items():
			if hiddenPattern.fullmatch(name) is None:
				continue
			elif name in kwargs:
				self.__setattr__(name, kwargs[name])
			elif len(args) > i:
				self.__setattr__(name, args[i])
				i += 1
			else:
				if not hasattr(self, name):
					raise MissingArgument(f"Missing required argument {name} for {self.__class__.__name__}.__init__")


def isType(value, typed):
	try:
		return isinstance(value, typed)
	except TypeError:
		try:
			assert isinstance(value, get_origin(typed))
			subTypes = get_args(typed)
			if len(subTypes) == 0:
				return True
			elif len(subTypes) == 1:
				return all(isType(v, subTypes[0]) for v in value)
			elif isinstance(subTypes[0], int) and len(value) == subTypes[0]:
				return all(isType(v, t) for v,t in zip(value, subTypes[1:]))
			elif len(subTypes) == len(value):
				return all(isType(v, subTyped) for v, subTyped in zip(value, subTypes))
		except:
			pass
	finally:
		return False

class Overload:

	_funcs : list[tuple[dict[int|str,Type],Callable]]
	__name__ : str

	def __init__(self, func : Callable):
		self._funcs = []
		self.__name__ = func.__code__.co_name
		self.add(func)

	def __call__(self, *args : tuple[Any], **kwargs : dict[str,Any]):
		
		for annotation, func in self._funcs:
			if func.__code__.co_posonlyargcount != len(args):
				continue
			elif any(not isType(arg, annotation[name]) for name, arg in zip(func.__code__.co_varnames, args)):
				continue
			elif any(not isType(kwargs[name], annotation[name]) for name in kwargs):
				continue
			else:
				return func(*args, **kwargs)
		raise NotImplemented(f"No definition satisfies {self.__name__}({', '.join([', '.join(map(str,args)), ', '.join(map('='.join, args.items()))])})")
	
	def __repr__(self):
		return f"<Overloaded function '{self.__name__}'>"
	
	def add(self, func : Callable):
		try:
			assert isinstance(func, Callable)
			self._funcs.append((func.__annotations__, func))
		except AssertionError:
			raise TypeError(f"Only `Callable` objects are overloadable. {func!r} is not an instance of `Callable`")
		return self

class Mode: pass
class ReadMode: pass
class WriteMode: pass
class Direction: pass
class Nucleotides: pass

Mode        = Literal["r", "w"]
ReadMode    = Literal["r"]
WriteMode   = Literal["w"]

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