
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
		return f"<{self.__bases__[0].__name__} {self.__name__!r} at 0x{id(self):0>16X} {' '.join(map(lambda pair : '{}={!r}'.format(*pair), filter(lambda x:not x[0].startswith('_') or x[0] == '__sql_name__', vars(self).items())))}>"
		
	def __str__(self):
		return self.__sql_name__
	
	def __sql__(self):
		return self.__sql_name__
	
	def __sub__(self, right):
		return Query(self, right)
	
	def __rsub__(self, left):
		return Query(self, left)
	
	def __eq__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isRelated(right, SQLObject):
			return Comparison(self, "==", right)
		else:
			return NotImplemented
	def __ne__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isRelated(right, SQLObject):
			return Comparison(self, "!=", right)
		else:
			return NotImplemented
	def __lt__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isRelated(right, SQLObject):
			return Comparison(self, "<", right)
		else:
			return NotImplemented
	def __le__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isRelated(right, SQLObject):
			return Comparison(self, "<=", right)
		else:
			return NotImplemented
	def __gt__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isRelated(right, SQLObject):
			return Comparison(self, ">", right)
		else:
			return NotImplemented
	def __ge__(self, right):
		if getattr(type(right), "__module__", None) == "builtins" or isinstance(right, SQLObject) or isRelated(right, SQLObject):
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
		cls.__sql_name__ = (name or camel2snake(cls.__name__)).lower()

class Hardcoded:

	value : Any

	def __init__(self, value) -> None:
		self.value = value
		if hasattr(value, "__doc__"):
			self.__doc__ = value.__doc__
	
	def __str__(self):
		if isinstance(self.value, str):
			return f"'{self.value}'"
		else:
			return str(self.value)
	
	def __sub__(self, right):
		return Query(self, right)
	
	def __rsub__(self, left):
		return Query(left, self)
	
	@property
	def params(self):
		return []

class ColumnMeta(NewMeta):

	type : str

	def __sql__(self):
		return f"{self} {self.type}"

class Column(SQLObject, metaclass=ColumnMeta):

	type : str = SQL_TYPE_NAMES[None]

	def __init_subclass__(cls, *, type : Union[str,ColumnMeta,"SQL_TYPE"]=None, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		if isinstance(type, SQL_TYPE):
			cls.type = type
		elif type in SQL_TYPE_NAMES:
			cls.type = SQL_TYPE_NAMES[type]
		elif isinstance(type, str) and any(type.upper().startswith(name) for name in SQL_TYPES):
			cls.type = type
		else:
			raise TypeError(f"Column type must either be a string or a python type mapped to sql type names in {SQL_TYPES=}." "\n" f"It was instead: {type!r}")

class ColumnAlias(SQLObject, metaclass=ColumnMeta):

	type : str
	fullName : str

	def __init_subclass__(cls, *, original, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		cls.fullName = str(original)
		cls.type = original.type

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

class SanitizedValue(metaclass=NewMeta):
	
	__sql_name__ : str = "SanitizedValue"
	value : Any

	def __init__(self, value : Any):
		self.value = value
	def __str__(self):
		return "?" if self.value is not None else "null"
	def __sql__(self):
		return self.value
	
	@property
	def params(self):
		if self.value is not None:
			return [self.value]
		else:
			return []

class Comparison:
	
	left : Any
	operator : str
	right : Any

	def __init__(self, left, operator, right, forceLeft=False, forceRight=False):
		self.left = SanitizedValue(left) if not isRelated(left, SQLObject) and not forceLeft else left
		if operator not in ["==", "!=", "<", "<=", ">", ">="]:
			raise ValueError(f"Not a valid operator for comparison: {operator=}")
		self.operator = operator
		self.right = SanitizedValue(right) if not isRelated(right, SQLObject) and not forceRight else right

	def __repr__(self):
		return f"<Comparison {str(self)!r}>"

	def __str__(self):
		return f"{self.left} {self.operator} {self.right}"
	
	def __bool__(self):
		return getattr(hash(self.left), OPERATOR_DUNDERS[self.operator])(hash(self.right))
	
	@property
	def params(self):

		out = []
		if hasattr(self.left, "params") and isinstance(self.left, object):
			value = self.left.params
			if not isinstance(value, (property, cached_property)):
				out.extend(self.left.params)
		if hasattr(self.right, "params") and isinstance(self.right, object):
			value = self.right.params
			if not isinstance(value, (property, cached_property)):
				out.extend(self.right.params)
		
		return out

class Assignment:
	
	left : str|Column
	right : Any

	def __init__(self, left : str|Column, right : Any, hardcode=False):
		self.left = left
		if hardcode:
			self.right = Hardcoded(right)
		else:
			self.right = right

	def __repr__(self):
		return f"<Assignment {str(self)!r}>"

	def __str__(self):
		return f"{self.left} = {self.right if isinstance(self.right , Hardcoded) else '?'}"
	
	@property
	def params(self):
		if not isinstance(self.right, (SQLObject, NewMeta, Hardcoded)):
			return [self.right]
		elif hasattr(self.right, "params"):
			return self.right.params

_NO_KEY_VALUE = object()
QUERY_CACHE = {}

class Prefix(type):

	def __str__(self):
		return self.__name__
	def __sub__(self, right):
		return Query(self, right)
	def __rsub__(self, left):
		return Query(left, self)
	def __mul__(self, other):
		return Query(self, ALL, other)
	def __iter__(self):
		return iter(Query(self))

class PragmaMeta(Prefix):
	def __sub__(self, right):
		if isinstance(right, str):
			return Query(self, Hardcoded(right))
		return Query(self, right)

class Word(metaclass=Prefix):
	
	content : tuple
	sep : str = ", "

	def __init__(self, *args : tuple[Union["Table", Column, "Index", Comparison]], **kwargs : dict[str,Any]):
		self.content = args + tuple(map(lambda keyVal : Comparison(keyVal[0], "==", keyVal[1], forceLeft=True), kwargs.items()))

	def __sub__(self, other):
		return Query(self, other)

	def __repr__(self):
		from SQLOOP._core.Functions import pluralize
		return f"<{pluralize(self.__class__.__base__.__name__)}.{self.__class__.__name__} content={self.content}>"
		
	def __str__(self) -> str:
		return f"{self.__class__.__name__} {self.sep.join(map(str, self.content))}" if len(self.content) else ""
	
	def __iter__(self):
		return iter(Query(self))

	def __sql__(self):
		return self.__str__()
	
	def __hash__(self):
		return sql(self).__hash__()
	
	@property
	def params(self):
		out = []
		for item in self.content:
			if hasattr(item, "params") and isinstance(item, object):
				value = item.params
				if not isinstance(value, (property, cached_property)):
					out.extend(value)
		return out
		
class EnclosedWord(Word):
	def __str__(self):
		return f"{self.__class__.__name__} ({self.sep.join(map(str, self.content))})"

class Query:

	words : tuple[Word|Any] = tuple()
	sep : str

	def __init__(self, *words : tuple[Word], sep : str=" "):
		self.sep = sep
		if len(words) == 1 and isinstance(words[0], tuple):
			words = words[0]
		newWords = []
		for word in map(lambda x:x if not isinstance(x, tuple) else SQLTuple(x), words):
			if isinstance(word, Query):
				newWords.extend(word.words)
			else:
				newWords.append(word)
		self.words = tuple(newWords)
	
	def __iter__(self):
		string = sql(self)
		params = self.params
		yield string
		yield params
	
	def __hash__(self):
		return hash(self.words)

	def __sql__(self):
		return str(self)+";"

	def __str__(self):
		if (ret := QUERY_CACHE.get(hash(self), _NO_KEY_VALUE)) is _NO_KEY_VALUE:
			ret = QUERY_CACHE[hash(self)] = self.sep.join(map(format, self.words))
		return ret
	
	def __format__(self, format_spec):
		return f"({format(str(self), format_spec)})"

	def __sub__(self, right : Self|Word):
		return Query(self, right)
	
	def __rsub__(self, left : Self|Word):
		return Query(self, left)

	def __mult__(self, right):
		self.words[-1] = self.words[-1](ALL)
		return Query(self, right)
	
	def __getattr__(self, key):
		if key in map(*this.__name__, Word.__subclasses__()):
			return next(filter(*this.__name__ == key, Word.__subclasses__()))
		return self.__getattribute__(key)
	
	@property
	def cols(self):
		from SQLOOP._core.Words import FROM, SELECT
		if isinstance(self.words[0], SELECT):
			if ALL not in self.words[0].content:
				return len(self.words[0].content)
			for i, word in enumerate(self.words):
				if isinstance(word, FROM):
					fullLength = sum(map(lambda x:len(x.columns) if isRelated(x, Table) else x.cols, word.content))
					break
				elif word is FROM:
					fullLength = sum(map(lambda x:len(x.columns) if isRelated(x, Table) else x.cols, itertools.takewhile(lambda x:isRelated(x, Table) or isinstance(x, Query),self.words[i+1:])))
					break
			else:
				raise MissingArgument(f"Could not find the 'FROM' of the following 'SELECT' statement:\n{str(self)}")
			return sum(map(lambda x:1 if x is not ALL else fullLength, self.words[0].content))
		
		elif self.words[0] is SELECT:
			from SQLOOP._core.Aggregates import Aggregate
			columns = tuple(itertools.takewhile(lambda x:not isRelated(x, Word) or isinstance(x, Aggregate), itertools.islice(self.words, 1, None)))
			if ALL not in columns:
				return len(columns)
			for i, word in itertools.islice(enumerate(self.words), 1+len(columns), None):
				if isinstance(word, FROM):
					fullLength = sum(map(lambda x:len(x.columns) if isRelated(x, Table) else x.cols, word.content))
					break
				elif word is FROM:
					fullLength = sum(map(lambda x:len(x.columns) if isRelated(x, Table) else x.cols, itertools.takewhile(lambda x:isRelated(x, Table) or isinstance(x, Query),self.words[i+1:])))
					break
			else:
				raise MissingArgument(f"Could not find the 'FROM' of the following 'SELECT' statement:\n{str(self)}")
			return sum(map(lambda x:1 if x is not ALL else fullLength, columns))
		else:
			return None
	
	@property
	def params(self):
		params = []
		for word in self.words:
			if hasattr(word, "params") and isinstance(word, object):
				value = word.params
				if not isinstance(value, (property, cached_property)):
					params.extend(word.params)
		return params

class TableMeta(NewMeta):

	columns : SQLDict[str,Column]

	def __contains__(self, column : Column):
		return column in self.columns

	def __sql__(self):
		sep = ",\n\t"
		return f"{self.__sql_name__} (\n\t{sep.join(itertools.chain(map(sql, self.columns), map(str, self.options)))}\n)"

class Table(SQLObject, HasColumns, metaclass=TableMeta):
	
	columns : SQLDict[str,Column]
	options : tuple[Query] = ()

	def __init_subclass__(cls, **kwargs):
		
		if any(not any(isinstance(word, (SQLTuple, Query)) for word in opt.words) for opt in cls.options):
			sep = "\n"
			raise ValueError(f"All table options must contain a tuple of columns/an expression. These did not: {sep.join(filter(lambda opt:any(not any(isinstance(word, (SQLTuple, Query)) for word in opt.words),cls.options)))}")
		numberOfColumns = sum(map(lambda x:1, filter(lambda x:isRelated(x, Column), vars(cls).values())))
		for i in range(numberOfColumns):
			if hasattr(cls, alphabetize(i)):
				break
		else:
			for i, value in enumerate(filter(lambda x:isRelated(x, Column), vars(cls).copy().values())):
				setattr(cls,
						alphabetize(i),
						ColumnMeta(
							f"{value.__name__}Alias",
							(ColumnAlias,),
							{},
							name=alphabetize(i),
							original=value))
		cls.__doc__ += "\n".join(["```python", *(f"{cls.__name__}.{name} = {col.__name__} # {col}" for name, col in vars(cls).items() if isRelated(col, Column)), "```"])
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

	table : Any

	def __sql__(self):
		return f"{self} ON {self.table} ({', '.join(map(str, self.columns))})"

class Index(SQLObject, HasColumns, metaclass=IndexMeta):

	table : Any

	def __sql__(self):
		return f"{self} ON {self.table} ({', '.join(map(str, self.columns))})"

class SQL_TYPE:
	args : tuple
	def __init__(self, *args):
		self.args = args
	def __str__(self):
		if self.args:
			return f"{type(self).__name__.replace('_', ' ')}({', '.join(map(str, self.args))})"
		else:
			return f"{type(self).__name__}"
class VARCHAR(SQL_TYPE): pass
class CHAR(SQL_TYPE): pass
class INT(SQL_TYPE): pass
class INTEGER(SQL_TYPE): pass
class TINYINT(SQL_TYPE): pass
class SMALLINT(SQL_TYPE): pass
class MEDIUMINT(SQL_TYPE): pass
class BIGINT(SQL_TYPE): pass
class UNSIGNED_BIG_INT(SQL_TYPE): pass
class INT2(SQL_TYPE): pass
class INT8(SQL_TYPE): pass
class CHARACTER(SQL_TYPE): pass
class VARYING_CHARACTER(SQL_TYPE): pass
class NCHAR(SQL_TYPE): pass
class NATIVE_CHARACTER(SQL_TYPE): pass
class NVARCHAR(SQL_TYPE): pass
class TEXT(SQL_TYPE): pass
class CLOB(SQL_TYPE): pass
class BLOB(SQL_TYPE): pass
class REAL(SQL_TYPE): pass
class DOUBLE(SQL_TYPE): pass
class DOUBLE_PRECISION(SQL_TYPE): pass
class FLOAT(SQL_TYPE): pass
class NUMERIC(SQL_TYPE): pass
class DECIMAL(SQL_TYPE): pass
class BOOLEAN(SQL_TYPE): pass
class DATE(SQL_TYPE): pass
class DATETIME (SQL_TYPE): pass

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