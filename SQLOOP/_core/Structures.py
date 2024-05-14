
from typing import Any
from SQLOOP.Globals import *


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
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isinstance(right, NewMeta):
			return Comparison(self, "==", right)
		else:
			return NotImplemented
	def __ne__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isinstance(right, NewMeta):
			return Comparison(self, "!=", right)
		else:
			return NotImplemented
	def __lt__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isinstance(right, NewMeta):
			return Comparison(self, "<", right)
		else:
			return NotImplemented
	def __le__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isinstance(right, NewMeta):
			return Comparison(self, "<=", right)
		else:
			return NotImplemented
	def __gt__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isinstance(right, NewMeta):
			return Comparison(self, ">", right)
		else:
			return NotImplemented
	def __ge__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isinstance(right, NewMeta):
			return Comparison(self, ">=", right)
		else:
			return NotImplemented
	
	def __or__(self, other):
		return Union[self, other]

	def __hash__(self):
		return hash(tuple(sorted(filter(lambda x:x[0] == "__sql_name__" or (not x[0].startswith("_") and getattr(x[1], "__hash__", None) is not None), vars(self).items()), key=lambda x:x)))

class SQLObject(metaclass=NewMeta):

	__sql_name__ : str
	"""Can be set through the 'name' keyword argument in class creation:
	```python
	class MyObject(SQLObject, name="my_thing"):
		...
	```
	Defaults to a 'snake_case'-version of the class name (In the above example it would be "my_object")
	"""

	def __init_subclass__(cls, *, name=None, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		cls.__sql_name__ = name or camel2snake(cls.__name__)

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

class ColumnMeta(NewMeta):

	type : str

	def __new__(cls, clsName, bases, namespace, **kwargs):
		return super().__new__(cls, clsName, bases, namespace)
	
	def __sql__(self):
		return f"{self} {self.type}"

class Column(SQLObject, metaclass=ColumnMeta):

	type : str = SQL_TYPE_NAMES[None]

	def __init_subclass__(cls, *, type : str|Type=None, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		if type in SQL_TYPE_NAMES:
			cls.type = SQL_TYPE_NAMES[type]
		elif isinstance(type, str) and any(type.upper().startswith(name) for name in SQL_TYPES):
			cls.type = type
		else:
			raise TypeError(f"Column type must either be a string or a python type mapped to sql type names in {SQL_TYPES=}." "\n" f"It was instead: {type!r}")

class ColumnAlias(SQLObject, metaclass=ColumnMeta): pass

class HasColumns:
	
	columns : SQLDict[str,Column]

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
		cls.columns = SQLDict(filter(lambda v:isRelated(v, Column), vars(cls).values()))

class HasTables:
	
	tables : SQLDict[str,Column]

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
		cls.tables = SQLDict(filter(lambda v:isRelated(v, Table), vars(cls).values()))

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
		self.right = SanitizedValue(right) if not isRelated(right, SQLObject) else right

	def __repr__(self):
		return f"<Comparison {str(self)!r}>"

	def __str__(self):
		return f"{self.left} {self.operator} {self.right}"
	
	def __bool__(self):
		return getattr(hash(self.left), OPERATOR_DUNDERS[self.operator])(hash(self.right))
	
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

class Word(metaclass=Prefix):
	
	content : tuple
	sep : str = ", "

	def __init__(self, *args : tuple[Union["Table", Column, "Index", Comparison]], **kwargs : dict[str,Any]):
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

class TableMeta(NewMeta):

	columns : tuple[Column]

	def __contains__(self, column : Column):
		return column in self.columns

	def __sql__(self):
		sep = ",\n\t"
		return f"{self.__sql_name__} ({sep.join(itertools.chain(map(sql, self.columns), map(sql, self.options)))})"

class Table(SQLObject, HasColumns, metaclass=TableMeta):
	
	columns : SQLDict[str,Column]
	options : tuple[Query] = ()

	def __init_subclass__(cls, **kwargs):
		i = 0
		for name, value in vars(cls).copy().items():
			if not isRelated(value, Column):
				continue
			if name != value.__name__ and not hasattr(cls, value.__name__):
				setattr(cls, value.__name__, value)
			setattr(cls, alphabetize(i), ColumnMeta(f"{value.__name__}Alias", (ColumnAlias,), {}, name=alphabetize(i)))
			i += 1
		super().__init_subclass__(**kwargs)

	def __init__(self, database):
		self.database = database
	
	def __getitem__(self, items):
		from SQLOOP._core.Databases import Selector
		out = Selector(self.database._connection, (type(self), ))
		return out[items]
	
	def __call__(self, *args, **kwargs):
		from SQLOOP._core.Databases import Selector
		selects = tuple(filter(lambda x:not isinstance(x, Comparison), args))
		where = tuple(itertools.chain(filter(lambda x:not isinstance(x, Comparison), args), map(lambda x:Comparison(x[0], "=", x[1]), kwargs.items())))
		return iter(Selector(self.database._connection, (type(self), ))[where])

class IndexMeta(NewMeta):

	def __sql__(self):
		return f"{self} ON {self.table}({', '.join(map(str, self.columns))})"

class Index(SQLObject, HasColumns, metaclass=IndexMeta):

	table : Any

	def __sql__(self):
		return f"{self} ON {self.table}({', '.join(map(str, self.columns))})"

class SQL_TYPE(type):
	args : tuple
	def __init__(self, *args):
		self.arg = args
	def __str__(self):
		if self.args:
			return f"{type(self).__name__}({', '.join(self.args)})"
		else:
			return f"{type(self).__name__}"
class VARCHAR(SQL_TYPE): pass
class CHAR(SQL_TYPE): pass

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