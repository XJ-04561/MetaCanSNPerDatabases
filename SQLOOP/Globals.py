

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

tableCreationCommand = re.compile(r"^\W*CREATE\W+(TEMP|TEMPORARY\W)?\W*(TABLE|INDEX)(\W+IF\W+NOT\W+EXISTS)?\W+(\w[.])?", flags=re.ASCII|re.IGNORECASE)

class Error(Exception):
	
	msg : str
	def __init__(self, *args, **kwargs):
		super().__init__()
		self.args = self.msg.format(*args, **kwargs)

class SQLTuple(tuple):
	def __new__(cls, *args, **kwargs):
		if args:
			return super().__new__(cls, map(lambda x:x if not isinstance(x, tuple) else SQLTuple(x), args[0]), *(args[1:]), **kwargs)
		else:
			return super().__new__(cls, *args, **kwargs)
	
	def __str__(self):
		from SQLOOP._core.Structures import SQLObject, NewMeta, Hardcoded
		return f"({', '.join(map(lambda x:str(x) if isinstance(x, (Hardcoded, SQLObject, NewMeta, SQLTuple)) else "?", self))})"
	
	@property
	def params(self):
		from SQLOOP._core.Structures import Hardcoded, SQLObject, NewMeta
		out = []
		for item in self:
			if not isinstance(item, (Hardcoded, SQLObject, NewMeta)):
				out.append(item)
			elif hasattr(item, "params"):
				out.extend(item.params)
		return out

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
		if isinstance(obj, type):
			return type(obj).__sql__(obj)
		else:
			return obj.__sql__()
	
class NoHash:
	def __eq__(self, other): return True

class ClassProperty:

	owner : type
	name : str
	fget : Callable
	fset : Callable
	fdel : Callable

	def __init__(self, fget, fset=None, fdel=None, doc=None):
		self.fget = fget
		self.fset = fset
		self.fdel = fdel
		
		self.__doc__ = doc if doc else fget.__doc__
	
	def __get__(self, instance, owner=None):
		return self.fget(instance or owner)
	
	def __set__(self, instance, value):
		self.fset(instance, value)
	
	def __delete__(self, instance):
		self.fdel(instance)
	
	def __set_name__(self, owner, name):
		self.owner = owner
		self.name = name
	
	def __repr__(self):
		return f"{object.__repr__(self)[:-1]} name={self.name!r}>"


class CachedClassProperty:

	def __init__(self, func):
		self.func = func

	def __get__(self, instance, owner=None):
		if instance is None and owner is not None:
			if self.name in owner.__dict__:
				return owner.__dict__[self.name]
			ret = self.func(owner)
			setattr(owner, self.name, ret)
			return ret
		else:
			if self.name in instance.__dict__:
				return instance.__dict__[self.name]
			ret = self.func(instance)
			setattr(instance, self.name, ret)
			return ret
	
	def __set__(self, instance, value):
		instance.__dict__ = value
	
	def __delete__(self, instance):
		del instance.__dict__[self.name]

	def __set_name__(self, owner, name):
		self.name = name
		
	def __repr__(self):
		return f"{object.__repr__(self)[:-1]} name={self.name!r}>"

allCapsSnakecase = re.compile("^[A-Z_0-9]+$")
snakeCase = re.compile("^[a-z_0-9]+$")
camelKiller = re.compile(r"(?<=[^A-Z])(?=[A-Z])")

camelCase = re.compile("^[^_]+$")
snakeKiller = re.compile(r"[_](\w)")

def binner(key, iterable, outType=list):
	if issubclass(outType, dict):
		return outType((i,tuple(data)) for i, data in itertools.groupby(sorted(iterable, key=key), key=key))
	else:
		return outType(tuple(data) for i, data in itertools.groupby(sorted(iterable, key=key), key=key))
		

T = TypeVar("T")
def first(iterator : Iterable[T]|Iterator[T]) -> T|None:
	try:
		return next(iterator)
	except TypeError:
		try:
			return next(iter(iterator))
		except:
			pass
	finally:
		return None

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
		elif not hasattr(iterable, "__iter__") and not hasattr(iterable, "__next__"):
			super().__init__(iterable, *args, **kwargs)
		else:
			data = list(iterable)
			if all(map(lambda x:hasattr(x, "__len__"), data)) and all(map(lambda x:len(x) == 2, data)):
				super().__init__(data, *args, **kwargs)
			else:
				super().__init__(list(map(lambda x:(str(x),x), data)), *args, **kwargs)
		self.valueSet = set(self)
	
	def __iter__(self):
		return iter(self.values())
	
	def __contains__(self, item):
		from SQLOOP._core.Structures import ColumnAlias
		if isinstance(item, str):
			return super().__contains__(item)
		elif isRelated(item, ColumnAlias):
			return super().__contains__(item.fullName)
		elif hasattr(item, "__sql__"):
			return super().__contains__(str(item))
		else:
			return item in self.values()
	
	def __or__(self, other):
		return type(self)(itertools.chain(self, other))

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
	
	def __setitem__(self, key, value):
		self.valueSet.add(value)
		super().__setitem__(key, value)
	
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
	
	def __delitem__(self, key):
		self.valueSet.remove(self[key])
		super().__delitem__(key)

	def append(self, item):
		self.valueSet.add(item)
		super().__setitem__(str(item), item)
	
	def isdisjoint(self, *args, **kwargs): return self.valueSet.isdisjoint(*args, **kwargs)
	def intersection(self, *args, **kwargs): return SQLDict(self.valueSet.intersection(*args, **kwargs))
	def difference(self, *args, **kwargs): return SQLDict(self.valueSet.difference(*args, **kwargs))
	def union(self, *args, **kwargs): return SQLDict(self.valueSet.union(*args, **kwargs))
	def without(self, *iterables): return SQLDict(filter(lambda x:not any(x in it for it in iterables), self))

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

