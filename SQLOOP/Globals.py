

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging, shutil, sys, itertools, random
from time import sleep


from typing import (
	Generator, Callable, Iterable, Self, Literal, LiteralString, Any, TextIO,
	BinaryIO, Never, Iterator, TypeVar, Type, get_args, get_origin,
	Union, Type, overload, final, Generic, Dict, GenericAlias)
from types import FunctionType, MethodType

from PseudoPathy import Path, DirectoryPath, FilePath, PathGroup, PathLibrary, PathList
from PseudoPathy.PathShortHands import *

from SQLOOP._core.Exceptions import *
from This import this

class Nothing:
	def __eq__(self, other):
		return False

class Mode: pass
class ReadMode: pass
class WriteMode: pass
Mode        = Literal["r", "w"]
ReadMode    = Literal["r"]
WriteMode   = Literal["w"]
class Rest(Iterator): pass
class All(Iterator): pass

class sql(str):
	def __new__(cls, obj):
		return obj.__sql__()
	
class NoHash:
	def __eq__(self, other): return True

class classProperty(property):

	def __get__(self, instance, owner=None):
		if instance is None and owner is not None:
			return super().__get__(self, owner)
		else:
			return super().__get__(self, instance)

allCapsSnakecase = re.compile("^[A-Z_0-9]+$")
snakeCase = re.compile("^[a-zA-Z_0-9]+$")
camelKiller = re.compile(r"(?<=[^A-Z])(?=[A-Z])")

camelCase = re.compile("^[^_]+$")
snakeKiller = re.compile(r"[_](\w)")

def camel2snake(string):
	if allCapsSnakecase.fullmatch(string) or snakeCase.fullmatch(string):
		return string
	else:
		return camelKiller.sub("_", string).lower()

def snake2camel(string):
	if camelCase.fullmatch(string):
		return string
	else:
		snakeKiller.sub(lambda m:m.group(1).upper(), string)

class SQLDict(dict):

	def __init__(self, iterable : Iterable=None, *args, **kwargs):
		"""If given a sequence of length 2 items, then the behavior is that of a dict initialization. Otherwise will
		create a dict where the value of each pair is an item in the sequence, with the return value of a 'str' call on
		the item as the key.
		
		"""
		if iterable is None:
			super().__init__((), *args, **kwargs)
		elif hasattr(iterable, "__iter__") and all(map(lambda x:hasattr(x, "__len__"), iterable)) and all(map(lambda x:len(x) == 2, iterable)):
			super().__init__(iterable, *args, **kwargs)
		else:
			super().__init__(map(lambda x:(str(x),x), iterable), *args, **kwargs)
	
	def __iter__(self):
		return iter(self.values())
	
	def __contains__(self, item):
		if isinstance(item, str):
			return super().__contains__(item)
		else:
			return item in self.values()
	
	def __eq__(self, other):
		if (not hasattr(other, "__len__") or len(other) != len(self)) \
			and not hasattr(other, "__next__"):
			return False
		if hasattr(other, "__len__") and len(other) == len(self):
			return all(x == y for x,y in zip(self, other))
		if hasattr(other, "__next__"):
			# The following will return False if one of the iterables/iterators outpaces the other.
			return all(x == y for x,y in zip(itertools.chain(self, itertools.repeat(Nothing())), itertools.chain(other, itertools.repeat(Nothing()))))
		return False
	
	def __getitem__(self, key):
		if isinstance(key, str):
			return super().__getitem__(key)
		elif isinstance(key, int) and key < len(self):
			for i,v in zip(range(key+1), self.values()): pass
			return v
		elif isinstance(key, int):
			raise IndexError(f"Position {key} is out of range in {self!r} that has a length of {len(self)}")
		else:
			return super().__getitem__(key)

@cache
def alphabetize(n : int):
	m = n
	out = []
	while m > 0:
		out.append("ABCDEFGHIJKLMNOPQRSTUVWXYZ"[n%26])
		m //= 26
	return "".join(out)


def isRelated(cls1 : type|object, cls2 : type) -> bool:
	"""Convenience function which returns True if cls1 is both a type and a subclass of cls2. This is useful because
	attempting issubclass() on an object as first argument raises an exception,  so this can be used instead of
	explicitly typing isinstance(cls1, type) and issubclass(cl1, cls2)"""
	return isinstance(cls1, type) and issubclass(cls1, cls2)


ASSERTIONS = (
	SchemaNotEmpty,
	ValidTablesSchema,
	ValidIndexesSchema
)

SQL_TYPES = {
	"INTEGER" : int,
	"TEXT" : str,
	"VARCHAR" : str,
	"CHAR" : str,
	"DATE" : str,
	"DATETIME" : str,
	"DECIMAL" : float,
	"NULL" : (lambda x : None)
}

SQL_TYPE_NAMES = {
	int : "INTEGER",
	str : "TEXT",
	float : "DECIMAL",
	None : "NULL"
}

OPERATOR_DUNDERS = {
	"==" : "__eq__",
	"!=" : "__ne__",
	"<" : "__lt__",
	"<=" : "__le__",
	">" : "__gt__",
	">=" : "__ge__"
}

whitespacePattern = re.compile(r"\s+")
sqlite3TypePattern = re.compile(r"^(?P<integer>INTEGER)|(?P<decimal>DECIMAL)|(?P<char>(VAR)?CHAR[(](?P<number>[0-9]*)[)])|(?P<date>DATE)|(?P<datetime>DATETIME)|(?P<text>TEXT)$", re.IGNORECASE)
namePattern = re.compile(r"^[a-zA-Z0-9_\-*]*$")
formatPattern = re.compile(r"[{](.*?)[}]")
fileNamePattern = re.compile(r"[a-zA-Z0-9_\-]*")
antiFileNamePattern = re.compile(r"[^a-zA-Z0-9_\-]")

SOFTWARE_NAME = "SQLOOP"

SOURCES = []

