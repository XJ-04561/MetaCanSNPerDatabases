
from typing import Any
from SQLOOP.Globals import *

__all__ = (
	"sql", "NoMatchingDefinition", "AutoObject", "Mode", "ReadMode", "WriteMode", "NewMeta", "SQLObject", "HasColumns",
	"SanitizedValue", "Comparison", "Assignment", "Table", "Column", "Index", "Query", "Prefix", "Word",
	"EnclosedWord", "ALL", "SQLITE_MASTER", "SQL", "NAME", "TYPE", "ROW_ID", "TABLE_NAME", "ROOT_PAGE"
)

class sql(str):
	def __new__(cls, obj):
		return obj.__sql__()

class NoMatchingDefinition(TypeError):
	def __init__(self, name, args=(), kwargs={}):
		self.args = (f"No definition of {name!r} fits args={args}, kwargs={kwargs}")

class AutoObject:
	"""Automatically assigns values passed to the constructor to the annotations
	of the object class. Skips 'hidden' attributes starting with only one
	underscore. Meaning, _name would not be assigned a value from the
	construction call, but __name__ would, as it starts with one, and not two
	underscores."""
	def __init__(self, *args, **kwargs):
		from SQLOOP._core.Functions import hiddenPattern
		i = 0
		for name, typeHint in self.__annotations__.items():
			if hiddenPattern.match(name) is not None:
				continue
			elif hasattr(self, name) and not isinstance(getattr(self, name), (cached_property, property)):
				continue
			elif name in kwargs:
				self.__setattr__(name, kwargs[name])
			elif len(args) > i:
				self.__setattr__(name, args[i])
				i += 1
			else:
				if not hasattr(self, name):
					from SQLOOP._core.Exceptions import MissingArgument
					raise MissingArgument(f"Missing required argument {name} for {self.__class__.__name__}.__init__")

class Mode: pass
class ReadMode: pass
class WriteMode: pass
Mode        = Literal["r", "w"]
ReadMode    = Literal["r"]
WriteMode   = Literal["w"]

class NewMeta(type):

	__name__ : str
	__sql_name__ : str
	
	def __repr__(self):
		return f"<{self.__bases__[0].__name__} {self.__name__!r} at 0x{id(self):0>16X} {' '.join(map(lambda pair : '{}={!r}'.format(*pair), filter(lambda x:not x[0].startswith('_'), vars(self).items())))}>"
		
	def __str__(self):
		return self.__sql_name__
	
	def __sql__(self):
		return self.__sql_name__
	
	def __sub__(self, right):
		return Query(self, right)
	
	def __rsub__(self, left):
		return Query(self, left)
	
	def __eq__(self, right):
		return Comparison(self, "==", right)
	def __ne__(self, right):
		return Comparison(self, "!=", right)
	def __lt__(self, right):
		return Comparison(self, "<", right)
	def __le__(self, right):
		return Comparison(self, "<=", right)
	def __gt__(self, right):
		return Comparison(self, ">", right)
	def __ge__(self, right):
		return Comparison(self, ">=", right)
	
	# def __format__(self, format_spec : str):
	# 	if format_spec.endswith("!sql"):
	# 		return self.__sql__().__format__(format_spec.rstrip("!sql"))
	# 	elif format_spec.endswith("!r"):
	# 		return self.__repr__().__format__(format_spec.rstrip("!r"))
	# 	elif format_spec.endswith("!s"):
	# 		return self.__str__().__format__(format_spec.rstrip("!s"))
	# 	else:
	# 		return self.__str__().__format__(format_spec)

	def __hash__(self):
		return hash(tuple(sorted(filter(lambda x:x[0] == "__sql_name__" or (not x[0].startswith("_") and getattr(x[1], "__hash__", None) is not None), vars(self).items()), key=lambda x:x)))

class SQLObject(metaclass=NewMeta):

	def __init_subclass__(cls, *, name=None, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		from SQLOOP._core.Functions import pluralize
		cls.__sql_name__ = name or cls.__name__

		# types = set(map(lambda x :cls.__annotations__.get(x[0]) if type(x[1]) in [property, cached_property] else type(x[1]), filter(lambda x:isinstance(x[1], (SQLObject, property, cached_property)), vars(cls).items())))
		
		# newAttrs = {}
		# for tp in types:
		# 	name = tp.__name__[0].lower() + tp.__name__[1:]
		# 	tupe = tuple(filter(lambda x:isinstance(x, tp), vars(cls).values()))
		# 	d = dict(filter(lambda x:isinstance(x[1], tp), vars(cls).items()))
		# 	newAttrs[pluralize(name)] = tupe
		# 	newAttrs[name + "Lookup"] = d
		
		# for name, value in newAttrs:
		# 	setattr(cls, name, value)

class Column(SQLObject):

	type : str = None

	def __init_subclass__(cls, *, type : str|Type=None, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		if type in SQL_TYPE_NAMES:
			cls.type = SQL_TYPE_NAMES[type]
		elif isinstance(type, str) and any(type.upper().startswith(name) for name in SQL_TYPES):
			cls.type = type
		else:
			raise TypeError(f"Column type must either be a string or a python type mapped to sql type names in {SQL_TYPES=}." "\n" f"It was instead: {type!r}")

class HasColumns:
	
	columns : tuple[Column]
	columnLookup : dict[str,Column]

	def __init_subclass__(cls, **kwargs):
		cls.columns = tuple(filter(lambda x : isinstance(x, type) and issubclass(x, Column), vars(cls).values()))
		cls.columnLookup = dict(filter(lambda x : isinstance(x[1], type) and issubclass(x[1], Column), vars(cls).items()))


class SanitizedValue(SQLObject):
	
	value : Any

	def __init__(self, value : Any):
		self.value = value
	def __str__(self):
		return "?" if self.value is not None else "null"
	def __sql__(self):
		return self.value
	
	@property
	def params(self):
		return [self.value]

class Comparison:
	
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
		return f"{self.left} {self.operator} {self.right}"
	
	def __bool__(self):
		return hash(self.left) == hash(self.right)
	
	@property
	def params(self):
		out = []
		for item in self.content:
			if hasattr(item, "params"):
				out.extend(item.params)
		return out

class Assignment:
	
	variable : str|Column
	value : Any

	def __init__(self, variable : str|Column, value : Any):
		self.variable = variable
		self.value = value

	def __repr__(self):
		return f"<Assignment {str(self)!r}>"

	def __str__(self):
		return f"{self.variable} = {self.value}"
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
_NO_KEY_VALUE = object()
QUERY_CACHE = {}
class Prefix(type):

	def __str__(self):
		return self.__name__
	def __sub__(self, other):
		return Query(self, other)
class Table: pass
class Index: pass
class Word(metaclass=Prefix):
	
	content : tuple
	sep : str = ", "

	def __init__(self, *args : tuple[Table|Column|Index|Comparison], **kwargs : dict[str,Any]):
		self.content = args + tuple(map(lambda keyVal : Comparison(keyVal[0], "==", keyVal[1]), kwargs.items()))

	def __sub__(self, other):
		return Query(self, other)

	def __repr__(self):
		from SQLOOP._core.Functions import pluralize
		return f"<{pluralize(self.__class__.__base__.__name__)}.{self.__class__.__name__} content={self.content}>"
		
	def __str__(self) -> str:
		return f"{self.__class__.__name__} {self.sep.join(map(str, self.content))}" if len(self.content) else ""
	
	def __iter__(self):
		for item in self.content:
			yield item

	def __sql__(self):
		return self.__str__()
	
	def __hash__(self):
		return sql(self).__hash__()
	
	# def __format__(self, format_spec : str):
	# 	if format_spec.endswith("!sql"):
	# 		return self.__sql__().__format__(format_spec.rstrip("!sql"))
	# 	elif format_spec.endswith("!r"):
	# 		return self.__repr__().__format__(format_spec.rstrip("!r"))
	# 	elif format_spec.endswith("!s"):
	# 		return self.__str__().__format__(format_spec.rstrip("!s"))
	# 	else:
	# 		return self.__str__().__format__(format_spec)
	
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
class Query:

	words : tuple[Word] = tuple()

	def __init__(self, *words : tuple[Word]):
		self.words = []
		for word in words:
			if not isinstance(word, Query):
				self.words.append(word)
			else:
				self.words.extend(word.words)
		self.words = tuple(self.words)
	
	@cache
	def __iter__(self):
		yield sql(self)
		yield self.params
	
	def __hash__(self):
		return hash(self.words)

	def __sql__(self):
		return str(self)+";"

	def __str__(self):
		if (ret := QUERY_CACHE.get(hash(self), _NO_KEY_VALUE)) is _NO_KEY_VALUE:
			ret = QUERY_CACHE[hash(self)] = " ".join(map(format, self.words))
		return ret
	
	def __format__(self, format_spec):
		return f"({format(str(self), format_spec)})"

	def __sub__(self, right : Self|Word):
		from SQLOOP._core.Words import FROM
		return Query(self, right)
	
	def __rsub__(self, left : Self|Word):
		from SQLOOP._core.Words import FROM
		return Query(self, left)

	def __mult__(self, right):
		self.words[-1] = self.words[-1](ALL)
		return Query(self, right)
	
	def __getattr__(self, key):
		if key in map(*this.__name__, Word.__subclasses__()):
			return next(filter(*this.__name__ == key, Word.__subclasses__()))
		return self.__getattribute__(key)
	
	@property
	def params(self):
		return list(map(*this.params, filter(*this.__hasattr__("params"), self.words)))

class Table(SQLObject, HasColumns):
	
	columns : tuple[Column]
	columnLookup : dict[str,Column]
	options : tuple[Query] = ()

class Index(SQLObject, HasColumns):

	table : Any

	def __sql__(self):
		return f"{self} ON {self.table}({', '.join(map(str, self.columns))})"

# class SQLObject(AutoObject):
	
# 	__name__ : str
# 	name : str = cached_property(lambda self:self.__name__)
	
# 	def __repr__(self):
# 		from SQLOOP._core.Functions import pluralize
# 		return f"<{pluralize(self.__class__.__name__)}.{self.__name__} {' '.join(map(lambda pair : '{}={!r}'.format(*pair), vars(self).items()))} at {hex(id(self))}>"
		
# 	def __str__(self):
# 		return self.name
	
# 	def __sql__(self):
# 		return self.name
	
# 	def __hash__(self):
# 		return hash(self.__class__) + hash(self.__name__)
	
# 	def __format__(self, format_spec : str):
# 		if format_spec.endswith("!sql"):
# 			return self.__sql__().__format__(format_spec.rstrip("!sql"))
# 		elif format_spec.endswith("!r"):
# 			return self.__repr__().__format__(format_spec.rstrip("!r"))
# 		elif format_spec.endswith("!s"):
# 			return self.__str__().__format__(format_spec.rstrip("!s"))
# 		else:
# 			return self.__str__().__format__(format_spec)

# class SanitizedValue(SQLObject):
# 	value : Any
# 	def __str__(self):
# 		return "?" if self.value is not None else "null"
# 	def __sql__(self):
# 		return self.value
# 	@property
# 	def params(self):
# 		return [self.value]

# class Comparison(AutoObject):
	
# 	left : Any
# 	operator : str
# 	right : Any

# 	def __init__(self, left, operator, right):
# 		self.left = left
# 		self.operator = operator
# 		self.right = SanitizedValue(right) if not isinstance(right, SQLObject) else right

# 	def __repr__(self):
# 		return f"<Comparison {str(self)!r}>"

# 	def __str__(self):
# 		f"{self.left} {self.operator} {self.right}"
	
# 	def __bool__(self):
# 		return hash(self.left) == hash(self.right)
	
# 	@property
# 	def params(self):
# 		out = []
# 		for item in self.content:
# 			if hasattr(item, "params"):
# 				out.extend(item.params)
# 		return out

# class Assignment(AutoObject):
	
# 	variable : str|Column
# 	value : Any

# 	def __repr__(self):
# 		return f"<Assignment {str(self)!r}>"

# 	def __str__(self):
# 		f"{self.variable} = {self.value}"

# class Column(SQLObject):
	
# 	__name__ : str
# 	name : str
# 	type : str
	
# 	@overload
# 	def __init__(self, name : str): ...
# 	@overload
# 	def __init__(self, name : str, type : str): ...
# 	@overload
# 	def __init__(self, __name__ : str, name : str, type : str): ...
	
# 	def __init__(self, __name__ : str, /, name : str=None, type : str=None):
# 		self.__name__ = __name__
# 		self.name = name or __name__
# 		self.type = type
# 		assert isinstance(self.name, str)
# 		assert namePattern.fullmatch(self.name) is not None, f"Name of column must be alphanumeric [a-zA-Z0-9_\-*], and {self.name} is not."
# 		assert self.type is None or (isinstance(self.type, str) and sqlite3TypePattern.fullmatch(self.type) is not None), f"Type of column must be valid sqlite3 type, and {self.type} is not."
	
# 	def __sql__(self):
# 		return f"{self.name} {self.type}"
	
# 	def __lt__(self, right):
# 		return Comparison(self, "<", right)
# 	def __gt__(self, right):
# 		return Comparison(self, ">", right)
# 	def __le__(self, right):
# 		return Comparison(self, "<=", right)
# 	def __ge__(self, right):
# 		return Comparison(self, ">=", right)
# 	def __eq__(self, right):
# 		return Comparison(self, "==", right)
# 	def __ne__(self, right):
# 		return Comparison(self, "!=", right)
# 	def __contains__(self, right):
# 		return Comparison(self, "IN", right)


# class Table(SQLObject):

# 	columns : tuple[Column] = tuple()
# 	options : tuple[Query|Word] = tuple()
# 	columnLookup : dict[Column,Column]

# 	def __sql__(self):
# 		sep = ",\n\t"
# 		return f"{self.name} ({sep.join(itertools.chain(map(sql, self.columns), map(sql, self.options)))})"
	
# 	def __hash__(self):
# 		return self.__sql__().__hash__()
	
# 	def __repr__(self):
# 		return f"<{__name__}.{self.__class__.__name__} columns={[c.__name__ for c in self.columns]} at {hex(id(self))}>"
	
# 	def __iter__(self):
# 		for column in self.columns:
# 			yield column
	
# 	@cached_property
# 	def columnLookup(self):
# 		return {col:col for col in self.columns}

# class Index(SQLObject):

# 	table : Any
# 	columns : tuple[Column]

# 	def __sql__(self):
# 		return f"{self} ON {self.table}({', '.join(map(str, self.columns))})"

class ALL(Column, name="*"): pass

class SQLITE_MASTER(Table, name="sqlite_master"):
	
	class SQL(Column, name="sql"): pass
	class NAME(Column, name="name"): pass
	class TYPE(Column, name="type"): pass
	class ROW_ID(Column, name="rowid"): pass
	class TABLE_NAME(Column, name="tbl_name"): pass
	class ROOT_PAGE(Column, name="rootpage"): pass
class SQL(Column, name="sql"): pass
class NAME(Column, name="name"): pass
class TYPE(Column, name="type"): pass
class ROW_ID(Column, name="rowid"): pass
class TABLE_NAME(Column, name="tbl_name"): pass
class ROOT_PAGE(Column, name="rootpage"): pass