"""

SQLObject:
	__name__ : str
		# The name of the object on the python-end.
	name : str
		# The name of the object on the database-end.
	_database : Database

Comparison:
	left : Any
	operator : str
	right : Any

Column(SQLObject):
	type : str
		# SQL-name of the datatype in which the column value is represented.
Table(SQLObject):
	columns : tuple[Column] = tuple()
		# Columns in the order they appear in the table.
	options : tuple[PrimaryKey|ForeignKey|Unique] = tuple()
		# Additional entries into the definition which are not themselves columns.
	_database : Database
		# Set by Databases when a table is accessed through a Database-object.
Index(SQLObject):
	table : Any
		# Table being indexed.
	columns : list[Column]
		# Columns being part of the index.

Query:
	words : tuple[Word] = tuple()
Word:
	name : str
		# SQL name a word used in statements.
	content : tuple
		# List of items to follow the word
	sep : str = ", "
		# Separator to separate items.
EnclosedWord
	# Same as a Word, but the items will be enclosed in parenthesis.
"""

from MetaCanSNPerDatabases.Globals import *

class AutoObject:
	"""Automatically assigns values passed to the constructor to the annotations
	of the object class. Skips 'hidden' attributes starting with only one
	underscore. Meaning, _name would not be assigned a value from the
	construction call, but __name__ would, as it starts with one, and not two
	underscores."""
	def __init__(self, *args, **kwargs):
		from MetaCanSNPerDatabases._core.Functions import hiddenPattern
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
					from MetaCanSNPerDatabases._core.Exceptions import MissingArgument
					raise MissingArgument(f"Missing required argument {name} for {self.__class__.__name__}.__init__")
"""

from typing import Callable, Any, Type, overload, get_args, get_origin, Self, final
from types import FunctionType, MethodType

class Overload:
	OVERLOADS_DICT : dict[str,Self] = {}
	from types import FunctionType, MethodType
	class TypingError(Exception): pass
	class UntypedArgument(TypingError):
		def __init__(self, funcName, *names, **kwargs):
			super().__init__(f"Function {funcName!r} parameter(s) {', '.join(map(repr, names))} is untyped (Lacks annotation through type-hinting). You can type a parameter by adding the type-object after a colon in the parameter list.\nExample:\ndef f(x : int) -> float:\n\treturn x / 2", **kwargs)

	class Signature:

		name : str
		names : tuple[str]
		requiredNames : set[str]
		requireds : tuple[type]
		optionalNames : set[str]
		optionals : dict[str,type]
		allNames : set[str]
		args : type|None = None
		kwargs : type|None = None

		def __init__(self, func : Callable):
			if not isinstance(func, Overload.FunctionType|Overload.MethodType):
				raise ValueError(f"Can only overload using functions or methods, not {type(func)}")
			self.name = func.__qualname__
			if "." in func.__qualname__: # isinstance(func, Overload.MethodType):
				self.names = func.__code__.co_varnames[1:]
			else:
				self.names = func.__code__.co_varnames
			if set(self.names).difference(func.__annotations__):
				raise Overload.UntypedArgument(func.__name__, *set(func.__code__.co_varnames).difference(func.__annotations__))
			self.requireds = {name : func.__annotations__.get(name, Any) for name in self.names[:func.__code__.co_argcount]}
			self.optionals = {} if func.__kwdefaults__ is None else {name : func.__annotations__.get(name, Any) for name in func.__kwdefaults__}
			self.requiredNames = set(self.requireds)
			self.optionalNames = set(self.optionals)
			self.allNames = self.requiredNames | self.optionalNames

			match func.__code__.co_flags & (8+4):
				case 8:
					self.kwargs = func.__annotations__[self.names[-1]]
				case 4:
					self.args = func.__annotations__[self.names[-1]]
				case 12:
					self.kwargs, self.args = func.__annotations__[self.names[-1]], func.__annotations__[self.names[-2]]

		def match(self, args : tuple[Any], kwargs : dict[str,Any]):
			"""NAIVE : Only accepts positional arguments that end up in 'args', do not positional arguments by name."""
			if self.requiredNames.difference(self.names[:len(args)], kwargs):
				return False
			if not self.allNames.issuperset(kwargs):
				return False
			for name, value in zip(self.name, args):
				if not Overload.isType(value, self.requireds[name]):
					return False
			if not name == self.names[-1]:
				for name in self.requiredNames.intersection(kwargs):
					if not Overload.isType(kwargs[name], self.requireds[name]):
						return False
			
			for name, sigType in self.optionals.items():
				if not Overload.isType(kwargs[name], sigType):
					return False

			return True

	_funcs : list[tuple[dict[int|str,Type],Callable,dict]]
	__name__ : str
	overloadDummy : Callable
	
	
	def __new__(cls, func : Callable):
		
		if func.__qualname__ not in Overload.OVERLOADS_DICT:
			Overload.OVERLOADS_DICT[func.__qualname__] = self = super().__new__(cls)
			self._funcs = []
		else:
			self = Overload.OVERLOADS_DICT[func.__qualname__]

		self._funcs.append((Overload.Signature(func), func))
		
		self.overloadDummy = overload(func)
		import copy
		fOut = copy.deepcopy(func)
		fOut.__code__ = self.__call__.__code__
		return fOut
	
	def __hash__(self):
		return self.overloadDummy.__hash__()

	def __call__(self, *args : tuple[Any], **kwargs : dict[str,Any]):
		for signature, func in self._funcs:
			if signature(args, kwargs):
				return func(*args, **kwargs)
		msg = f"No definition satisfies {self.__name__}("
		msg += ", ".join(map(repr,args))
		if not msg.endswith("("):
			msg += ", "
		msg += ", ".join(map("{0[0]}={0[1]!r}".format, kwargs.items()))
		raise NotImplementedError(msg + ")")
	
	def __repr__(self):
		return f"<Overloaded function '{self.__name__}'>"
	
	def add(self, func : Callable):
		self._funcs.append((Overload.Signature(func), func))
		self.dummy = overload(func)
		self.dummy.__code__ = self.dummy.__code__.__code__.replace(co_code=self.__call__.__code__.co_code)
	
	@staticmethod
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
					return all(Overload.isType(v, subTypes[0]) for v in value)
				elif isinstance(subTypes[0], int) and len(value) == subTypes[0]:
					return all(Overload.isType(v, t) for v,t in zip(value, subTypes[1:]))
				elif len(subTypes) == len(value):
					return all(Overload.isType(v, subTyped) for v, subTyped in zip(value, subTypes))
			except:
				pass
		finally:
			return False


class Test:
	
	@Overload
	def f(self, a : int): pass
	
	@Overload
	def f(self, a : float): pass

class Test2:

	@overload
	def f(self, a : int): pass
	
	@overload
	def f(self, a : float): pass

	def f(self, a): pass

t1 = Test()
t1.f(2)
t2 = Test2()
print(repr(t2.f))
"""
class Overload:
	from types import FunctionType, MethodType
	class TypingError(Exception): pass
	class UntypedArgument(TypingError):
		def __init__(self, funcName, *names, **kwargs):
			super().__init__(f"Function {funcName!r} parameter(s) {', '.join(names)} is untyped (Lacks annotation through type-hinting). You can type a parameter by adding the type-object after a colon in the parameter list.\nExample:\ndef f(x : int) -> float:\n\treturn x / 2", **kwargs)

	class Signature:

		name : str
		names : tuple[str]
		requiredNames : set[str]
		requireds : tuple[type]
		optionalNames : set[str]
		optionals : dict[str,type]
		allNames : set[str]
		args : type|None = None
		kwargs : type|None = None

		def __init__(self, func : Callable):
			if not isinstance(func, Overload.FunctionType|Overload.MethodType):
				raise ValueError(f"Can only overload using functions or methods, not {type(func)}")
			if set(func.__code__.co_varnames).difference(func.__annotations__):
				raise Overload.UntypedArgument(func.__name__, *set(func.__code__.co_varnames).difference(func.__annotations__))
			self.name = func.__qualname__
			if isinstance(func, Overload.MethodType):
				self.names = func.__func__.__code__.co_varnames[1:]
			else:
				self.names = func.__code__.co_varnames
			self.requireds = {name : func.__annotations__.get(name, Any) for name in self.names[:func.__code__.co_argcount]}
			self.optionals = {name : func.__annotations__.get(name, Any) for name in func.__code__.__kwdefaults__}
			self.requiredNames = set(self.requireds)
			self.optionalNames = set(self.optionals)
			self.allNames = self.requiredNames | self.optionalNames

			match func.__code__.co_flags & (8+4):
				case 8:
					self.kwargs = func.__annotations__[self.names[-1]]
				case 4:
					self.args = func.__annotations__[self.names[-1]]
				case 12:
					self.kwargs, self.args = func.__annotations__[self.names[-1]], func.__annotations__[self.names[-2]]

		def match(self, args : tuple[Any], kwargs : dict[str,Any]):
			"""NAIVE : Only accepts positional arguments that end up in 'args', do not positional arguments by name."""
			if self.requiredNames.difference(self.names[:len(args)], kwargs):
				return False
			if not self.allNames.issuperset(kwargs):
				return False
			for name, value in zip(self.name, args):
				if not Overload.isType(value, self.requireds[name]):
					return False
			if not name == self.names[-1]:
				for name in self.requiredNames.intersection(kwargs):
					if not Overload.isType(kwargs[name], self.requireds[name]):
						return False
			
			for name, sigType in self.optionals.items():
				if not Overload.isType(kwargs[name], sigType):
					return False

			return True

	_funcs : list[tuple[dict[int|str,Type],Callable,dict]]
	__name__ : str

	def __init__(self, func : Callable):
		self._funcs = []
		# self.__func__ = self.__call__
		self.add(func)
	
	def __new__(cls, func : Callable):
		obj = super().__init__(cls, func)
		type(Overload.__new__)(obj.__call__)
		return obj

	def __call__(self, *args : tuple[Any], **kwargs : dict[str,Any]):
		for signature, func in self._funcs:
			if signature(args, kwargs):
				return func(*args, **kwargs)
		msg = f"No definition satisfies {self.__name__}("
		msg += ", ".join(map(repr,args))
		if not msg.endswith("("):
			msg += ", "
		msg += ", ".join(map("{0[0]}={0[1]!r}".format, kwargs.items()))
		raise NotImplementedError(msg + ")")
	
	def __repr__(self):
		return f"<Overloaded function '{self.__name__}'>"
	
	def add(self, func : Callable):
		self._funcs.append((Overload.Signature(func), func))
		overload(func)
		self.__func__ = func
		return self
	
	@staticmethod
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
					return all(Overload.isType(v, subTypes[0]) for v in value)
				elif isinstance(subTypes[0], int) and len(value) == subTypes[0]:
					return all(Overload.isType(v, t) for v,t in zip(value, subTypes[1:]))
				elif len(subTypes) == len(value):
					return all(Overload.isType(v, subTyped) for v, subTyped in zip(value, subTypes))
			except:
				pass
		finally:
			return False

class Mode: pass
class ReadMode: pass
class WriteMode: pass
Mode        = Literal["r", "w"]
ReadMode    = Literal["r"]
WriteMode   = Literal["w"]

class sql(str):
	def __new__(cls, obj):
		return super().__new__(cls, obj.__sql__())

class this:
	"""Class that can be used when map() needs a function that just needs to grab an attribute."""
	@classmethod
	def __getattribute__(cls, attrName):
		return object.__getattribute__(cls, "this")(attrName)
	class this:
		def __init__(self, attrName):
			self.__name__ = attrName
		def __call__(self, obj):
			return getattr(obj, self.__name__)
		def __repr__(self):
			return f"<this.{self.__name__} object>"


class SQLObject(AutoObject):
	
	__name__ : str
	name : str
	
	def __repr__(self):
		from MetaCanSNPerDatabases._core.Functions import pluralize
		return f"<{pluralize(self.__class__.__name__)}.{self.__name__} {' '.join(map(lambda keyVal : '{}={:!r}'.format(*keyVal), vars(self).items()))} at {hex(id(self))}>"
		
	def __str__(self):
		return self.name
	
	def __sql__(self):
		return self.name
	
	def __hash__(self):
		return self.__sql__().__hash__()
	
	def __format__(self, format_spec : str):
		if format_spec.endswith("!sql"):
			return self.__sql__().__format__(format_spec.rstrip("!sql"))
		elif format_spec.endswith("!r"):
			return self.__repr__().__format__(format_spec.rstrip("!r"))
		elif format_spec.endswith("!s"):
			return self.__str__().__format__(format_spec.rstrip("!s"))
		else:
			return self.__str__().__format__(format_spec)

class SanitizedValue(SQLObject):
	value : Any
	def __str__(self):
		return "?" if self.value is not None else "null"
	def __sql__(self):
		return self.value
	@property
	def params(self):
		return [self.value]

class Comparison(AutoObject):
	
	left : Any
	operator : str
	right : Any

	def __init__(self, left, operator, right):
		self.left = left
		self.operator = operator
		self.right = SanitizedValue(right) if not isinstance(right, SQLObject) else right

	def __repr__(self):
		return f"<Comparison {str(self)!r}>"

	def __str__(self):
		f"{self.left} {self.operator} {self.right}"
	
	@property
	def params(self):
		out = []
		for item in self.content:
			if hasattr(item, "params"):
				out.extend(item.params)
		return out

class Assignment(AutoObject):
	
	variable : str|Column
	value : Any

	def __repr__(self):
		return f"<Assignment {str(self)!r}>"

	def __str__(self):
		f"{self.variable} = {self.value}"

class Column(SQLObject):
	
	type : str
	
	@Overload
	def __init__(self, name : str):
		self.__name__ = self.name = name
		self.type = None
	
	@__init__.add
	def _(self, name : str, type : str):
		self.__name__ = self.name = name
		self.type = type
	
	@__init__.add
	def _(self, __name__ : str, name : str, type : str):
		self.__name__ = __name__
		self.name = name
		self.type = type

	def __sql__(self):
		return f"{self.name} {self.type}"
	
	def __lt__(self, right):
		return Comparison(self, "<", right)
	def __gt__(self, right):
		return Comparison(self, ">", right)
	def __le__(self, right):
		return Comparison(self, "<=", right)
	def __ge__(self, right):
		return Comparison(self, ">=", right)
	def __eq__(self, right):
		return Comparison(self, "==", right)
	def __ne__(self, right):
		return Comparison(self, "!=", right)
	def __contains__(self, right):
		return Comparison(self, "IN", right)

class Table(SQLObject):

	columns : tuple[Column] = tuple()
	options : tuple[Query|Word] = tuple()
	
	def __sql__(self):
		sep = ",\n\t"
		return f"{self.name} ({sep.join(ChainMap(map(sql, self.options), map(sql, self.columns)))})"
	
	def __hash__(self):
		return self.__sql__().__hash__()
	
	def __repr__(self):
		return f"<{__name__}.{self.__class__.__name__} columns={[c.__name__ for c in self.columns]} at {hex(id(self))}>"
	
	def __iter__(self):
		for column in self.columns:
			yield column

class Index(SQLObject):

	table : Any
	columns : tuple[Column]

	def __sql__(self):
		return f"{self} ON {self.table}({', '.join(map(str, self.columns))})"

class Query:

	words : tuple[Word] = tuple()

	def __init__(self, *words):
		self.words = []
		for word in words:
			if not isinstance(word, Query):
				self.words.append(word)
			else:
				self.words.extend(word.words)
		self.words = tuple(self.words)

	def __iter__(self):
		yield f"{' '.join(map(str, self.words))};"
		yield self.params
	
	def __str__(self):
		return ' '.join(map(str, self.words))
	
	def __format__(self, format_spec):
		return f"({' '.join(map(str, self.words))})".__format__(format_spec)

	def __sub__(self, other : Query|Word):
		return Query(self, other)

	def __mult__(self, right):
		self.words[-1] = self.words[-1](ALL)
		return Query(self, right)
	
	def __getattr__(self, key):
		if key in map(lambda obj : obj.__name__, Word.__subclasses__()):
			return next(filter(lambda obj : obj.__name__ == key, Word.__subclasses__()))
		return self.__getattribute__(key)
	
	@property
	def params(self):
		return [word.params for word in self.words if hasattr(word, "params")]

class Word:
	
	content : tuple
	sep : str = ", "

	def __init__(self, *args : tuple[Table|Column|Index|Comparison], **kwargs : dict[str,Any]):
		self.content = args + tuple(map(lambda keyVal : Comparison(keyVal[0], "==", keyVal[1]), kwargs.items()))

	def __sub__(self, other):
		return Query(self, other)

	def __repr__(self):
		from MetaCanSNPerDatabases._core.Functions import pluralize
		return f"<{pluralize(self.__class__.__base__.__name__)}.{self.__class__.__name__} content={self.content}>"
		
	def __str__(self) -> str:
		return f"{self.__class__.__name__} {self.sep.join(map(str, self.content))}" if len(self.content) else ""
	
	def __sql__(self):
		return self.__str__()
	
	def __hash__(self):
		return self.__sql__().__hash__()
	
	def __format__(self, format_spec : str):
		if format_spec.endswith("!sql"):
			return self.__sql__().__format__(format_spec.rstrip("!sql"))
		elif format_spec.endswith("!r"):
			return self.__repr__().__format__(format_spec.rstrip("!r"))
		elif format_spec.endswith("!s"):
			return self.__str__().__format__(format_spec.rstrip("!s"))
		else:
			return self.__str__().__format__(format_spec)
	
	@property
	def params(self):
		out = []
		for item in self.content:
			if hasattr(item, "params"):
				out.extend(item.params)
		return out
		
class EnclosedWord(Word):
	def __str__(self):
		return f"{self.__class__.__name__} ({self.sep.join(map(str, self.content))})"
	
class Prefix(type):

	def __str__(self):
		return self.__name__
	def __sub__(self, other):
		return Query(self, other)


ALL				= Column("ALL",				"*",					"")
SQLITE_MASTER	= Table("SQLITE_MASTER", "sqlite_master", (Column("type"), Column("name"), Column("tbl_name"), Column("rootpage"), Column("sql")))