

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging, shutil, sys, itertools, random, copy
from time import sleep
from PseudoPathy import Path, DirectoryPath, FilePath, PathGroup, PathLibrary, PathList
from PseudoPathy.PathShortHands import *
from GeekyGadgets import *
from GeekyGadgets.TypeHinting import *

from SQLOOP._core.Exceptions import *

_NOT_SET = object()
_T = TypeVar("_T")
LOGGER = logging.getLogger("SQLOOP")
MAX_DEBUG = False
SOFTWARE_NAME = "SQLOOP"
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
	">=" : "__ge__",
	"IN" : "__contains__"
}

whitespacePattern = re.compile(r"\s+")
sqlite3TypePattern = re.compile(r"^(?P<integer>INTEGER)|(?P<decimal>DECIMAL)|(?P<char>(VAR)?CHAR[(](?P<number>[0-9]*)[)])|(?P<date>DATE)|(?P<datetime>DATETIME)|(?P<text>TEXT)$", re.IGNORECASE)
namePattern = re.compile(r"^[a-zA-Z0-9_\-*]*$")
formatPattern = re.compile(r"[{](.*?)[}]")
fileNamePattern = re.compile(r"[a-zA-Z0-9_\-]*")
antiFileNamePattern = re.compile(r"[^a-zA-Z0-9_\-]")
tableCreationCommand = re.compile(r"^\W*CREATE\W+(TEMP|TEMPORARY\W)?\W*(TABLE|INDEX)(\W+IF\W+NOT\W+EXISTS)?\W+(\w[.])?", flags=re.ASCII|re.IGNORECASE)

class Connection(sqlite3.Connection):
	filename : str | bytes | os.PathLike[str] | os.PathLike[bytes] = None

	@overload
	def __init__(self, database: str | bytes | os.PathLike[str] | os.PathLike[bytes], timeout: float = ..., detect_types: int = ..., isolation_level: str | None = ..., check_same_thread: bool = ..., factory: sqlite3.Connection | None = ..., cached_statements: int = ..., uri: bool = ..., autocommit: bool = ...) -> None: ...
	def __init__(self, database: str | bytes | os.PathLike[str] | os.PathLike[bytes], *args, **kwargs) -> None:
		self.filename = database
		super().__init__(database, *args, **kwargs)
		
	def __repr__(self):
		return f"{object.__repr__(self)[:-1]} '{self.filename}'>"

class SQLOOPError(Exception):
	
	msg : str
	def __init__(self, *args, **kwargs):
		super().__init__()
		self.args = self.msg.format(*args, **kwargs)

class SQLOOP:

	__sql_name__ : str
	"""Can be set through the 'name' keyword argument in class creation:
	```python
	class MyObject(SQLObject, name="my_thing"):
		...
	```
	Defaults to a 'snake_case'-version of the class name (In the above example it would be "my_object")
	"""
	params : list

	def __init_subclass__(cls, *args, name : str|None=None, **kwargs) -> None:
		if name is not None:
			cls.__sql_name__ = name.lower()
		else:
			cls.__sql_name__ = Case.toSnake(cls.__name__) if "_" not in cls.__name__.strip("_") else cls.__name__.lower()
		super().__init_subclass__(*args, **kwargs)

	def __repr__(self):
		if isinstance(self, type):
			return f"<{(self.__bases__ or [self.__class__])[0].__name__} {self.__name__!r}/{self.__sql_name__!r} at 0x{id(self):0>16X}>"
		else:
			return f"<{self.__class__.__name__}/{self.__class__.__sql_name__!r} at 0x{id(self):0>16X}>"
		# if isinstance(self, type):
		# 	return f"<{(self.__bases__ or [self.__class__])[0].__name__} {self.__name__!r} at 0x{id(self):0>16X} {' '.join(map(lambda pair : '{}={}'.format(*pair), filter(lambda x:not x[0].startswith('_') or x[0] == '__sql_name__', vars(self).items())))}>"
		# else:
		# 	return f"<{self.__class__.__name__} at 0x{id(self):0>16X} {' '.join(map(lambda pair : '{}={}'.format(*pair), filter(lambda x:not x[0].startswith('_') or x[0] == '__sql_name__', vars(self).items())))}>"
		
	def __str__(self):
		return self.__sql_name__
	
	def __sql__(self):
		return self.__sql_name__
	
	def __sub__(self, right):
		from SQLOOP._core.Structures import Query
		from SQLOOP._core.Words import IN, NOT, IS
		if isinstance(right, (IN, NOT, IS)): return NotImplemented
		return Query(self, right)
	
	def __rsub__(self, left):
		from SQLOOP._core.Structures import Query
		return Query(left, self)
	
	def __matmul__(self, other : sqlite3.Connection) -> sqlite3.Cursor:
		from SQLOOP._core.Databases import Fetcher, Query
		if hasattr(other, "execute"):
			return other.execute(str(self), self.params)
		return NotImplemented

	@property
	def params(self):
		return []

class SQLTuple(SQLOOP, tuple):
	
	@overload
	def __new__(cls, iterable : Iterable): ...
	@overload
	def __new__(cls, *items : Any): ...

	def __new__(cls, *args):
		if len(args) == 1 and isinstance(args[0], Iterable):
			args = tuple(args[0])
		from SQLOOP._core.Structures import SanitizedValue
		return tuple.__new__(cls, map(lambda x:x if isinstance(x, SQLOOP) else SQLTuple(x) if isinstance(x, Iterable) else SanitizedValue(x), args))
	
	def __str__(self):
		return f"({', '.join(map(format, self))})"
	
	@property
	def params(self):
		out = []
		for item in self:
			out.extend(getReadyAttr(item, "params", []))
		return out

class sql(str, SQLOOP):
	def __new__(cls, obj):
		if isinstance(obj, type):
			return super().__new__(cls, type(obj).__sql__(obj))
		else:
			return super().__new__(cls, obj.__sql__())

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
				super().__init__(list(map(lambda x:(getattr(x, "__sql_name__", None) or str(x),x), data)), *args, **kwargs)
		self.valueSet = set(self)
	
	def __iter__(self):
		return iter(self.values())
	
	def __contains__(self, item):
		from SQLOOP._core.Structures import ColumnAlias, Column
		if isinstance(item, str):
			return super().__contains__(item)
		elif isRelated(item, ColumnAlias):
			return super().__contains__(item.fullName)
		elif isRelated(item, Column):
			return super().__contains__(item.__sql_name__)
		elif isinstance(item, SQLOOP):
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
			return all(x == y for x,y in ZipLongest(self, other))
		return False
	
	def __setitem__(self, key, value):
		self.valueSet.add(value)
		if hasattr(key, "__sql_name__"):
			super().__setitem__(key.__sql_name__, value)
		else:
			super().__setitem__(key, value)
	
	def __getitem__(self, key):
		if hasattr(key, "__sql_name__"):
			return super().__getitem__(key.__sql_name__)
		elif isinstance(key, str):
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
