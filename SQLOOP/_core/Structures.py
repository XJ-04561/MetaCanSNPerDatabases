
from SQLOOP.Globals import *
from SQLOOP._core.Types import *

class SQLStructure(SQLOOP, type):

	__name__ : str
	__sql_name__ : str
	
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

	def __or__(self, other):
		return Union[self, other]

	def __hash__(self):
		return hash(self.__sql_name__)

class SQLObject(SQLOOP, metaclass=SQLStructure):
	def __hash__(self):
		from SQLOOP._core.Functions import forceHash
		return hash(self.__sql_name__)+forceHash(vars(self).items())

class Hardcoded(SQLOOP):

	value : Any

	def __init__(self, value) -> None:
		self.value = value
		if hasattr(value, "__doc__"):
			self.__doc__ = value.__doc__
	
	def __str__(self):
		return str(self.value)
	
	def __sub__(self, right):
		return Query(self, right)
	
	def __rsub__(self, left):
		return Query(left, self)
	
	@property
	def params(self):
		return []

class ColumnMeta(SQLStructure):

	type : "SQL_TYPE"
	table : "Table"
	constraint : "Query"

	def __str__(self):
		if self.table is not None:
			return f"{self.table}.{self.__sql_name__}"
		else:
			return super().__str__()

	def __sql__(self):
		if self.constraint is not None:
			return f"{self} {self.type} {self.constraint}"
		else:
			return f"{self} {self.type}"

class Column(SQLObject, metaclass=ColumnMeta):

	type : "SQL_TYPE"
	table : "Table" = None
	constraint : "Query" = None

	def __init_subclass__(cls, *, type : Union[str,ColumnMeta,SQL_TYPE]=None, table=None, constraint=None, **kwargs) -> None:
		super().__init_subclass__(**kwargs)
		from SQLOOP._core.Words import NULL
		if table is not None:
			cls.table = table
		if constraint is not None:
			cls.constraint = constraint
		if type is None:
			if not hasattr(cls, "type"):
				cls.type = NULL
		elif isinstance(type, SQL_TYPE) or isRelated(type, SQL_TYPE) or type is NULL:
			cls.type = type
		elif type in SQL_TYPE_NAMES and SQL_TYPE_NAMES[type] in map(*this.__name__, SQL_TYPE.__subclasses__()):
			string = SQL_TYPE_NAMES[type]
			for subClass in SQL_TYPE.__subclasses__():
				if subClass.__name__ == string:
					cls.type = subClass
					break
		elif isinstance(type, str) and type in map(*this.__name__, SQL_TYPE.__subclasses__()):
			for subClass in SQL_TYPE.__subclasses__():
				if subClass.__name__ == type:
					cls.type = subClass
					break
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
		cls.columns = SQLDict(filter(lambda v:isRelated(v, Column), vars(cls).values()))
		super().__init_subclass__(**kwargs)

class HasTables:
	
	tables : SQLDict[str,Column]

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
		cls.tables = SQLDict(filter(lambda v:isRelated(v, Table), vars(cls).values()))

class SanitizedValue(SQLObject, metaclass=SQLStructure):
	
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
			return [repr(self.value)] if type(self.value) is str else [self.value]
		else:
			return []

class Comparison(SQLOOP):
	
	left : Any
	operator : str
	right : Any

	def __init__(self, left, operator, right, forceLeft=False, forceRight=False):
		if isinstance(left, SQLOOP):
			self.left = left
		elif forceLeft:
			self.left = Hardcoded(left)
		else:
			self.left = SanitizedValue(left)
		
		assert operator in ["==", "!=", "<", "<=", ">", ">=", "=", "IN"], f"Not a valid operator for comparison: {operator=}"
		self.operator = operator

		if isinstance(right, SQLOOP):
			self.right = right
		elif forceRight:
			self.right = Hardcoded(right)
		else:
			self.right = SanitizedValue(right)

	def __repr__(self):
		return f"<{type(self).__name__} {str(self)!r}>"

	def __str__(self):
		return f"{self.left if not isinstance(self.left, SanitizedValue) else '?'} {self.operator} {self.right if not isinstance(self.right, SanitizedValue) else '?'}"
	
	def __getitem__(self, key):
		if isinstance(key, int):
			if key == 0:
				return self.left
			elif key == 1:
				return self.right
			else:
				raise IndexError()
		else:
			raise KeyError()
		
	
	def __bool__(self):
		return getattr(hash(self.left), OPERATOR_DUNDERS[self.operator])(hash(self.right))
	
	@property
	def params(self):

		out = []

		if isinstance(self.left, SanitizedValue):
			out.append(self.left.value)
		else:
			out.extend(getReadyAttr(self.left, "params", []))
		
		if isinstance(self.right, SanitizedValue):
			out.append(self.right.value)
		else:
			out.extend(getReadyAttr(self.right, "params", []))
		
		return out

class Assignment(Comparison):
	
	left : str|Column
	operator : str = "="
	right : Any

	def __init__(self, left : str|Column, right : Any, hardcode=False):
		super().__init__(left, self.operator, right, forceLeft=True, forceRight=hardcode)

_NO_KEY_VALUE = object()
QUERY_CACHE = {}

class Prefix(SQLOOP, type):

	def __str__(self):
		return self.__name__
	def __mul__(self, other):
		from SQLOOP._core.Schema import ALL
		return Query(self, ALL, other)

class PragmaMeta(Prefix):
	def __sub__(self, right):
		if isinstance(right, str):
			return Query(self, Hardcoded(right))
		return Query(self, right)

class Word(SQLOOP, metaclass=Prefix):
	
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
		return f"{self.__class__.__name__} {self.sep.join(map(str, self.content))}"

	def __sql__(self):
		return self.__str__()
	
	def __hash__(self):
		return sql(self).__hash__()
	
	def __contains__(self, item):
		return item in self.content
	
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

class Query(SQLOOP):

	words : tuple[Word|Any] = SQLTuple()
	startWord : Word
	sep : str = " "

	def __new__(cls, *args, **kwargs):
		
		from SQLOOP._core.Expressions import Expression
		from SQLOOP._core.Functions import recursiveWalk
		
		if cls is not Query:
			return super().__new__(cls)
		elif args and isinstance(args[0], Query) and type(args[0]) is not Query:
			return super().__new__(type(args[0]))
		else:
			for item in recursiveWalk(args):
				if isinstance(item, Word):
					startWord = type(item)
					break
				elif isRelated(item, Word):
					startWord = item
					break
			else:
				return super().__new__(cls)
			
			for expr in Expression.__subclasses__():
				if startWord in expr.startWords:
					return super().__new__(expr)
		return super().__new__(cls)

	@overload
	def __init__(self, iterable : Iterable[Word|SQLTuple], sep : str=" "): ...
	@overload
	def __init__(self, *words : Word|SQLTuple, sep : str=" "): ...

	def __init__(self, word, *words : tuple[Word], sep : str|None=None):
		if sep is not None:
			self.sep = sep
		if isinstance(word, Iterable):
			words = (*word, *words)
		else:
			words = (word, *words)
		self.words = SQLTuple(SQLTuple(word) if isinstance(word, tuple) else word for word in words)
	
	def __contains__(self, other):
		
		from SQLOOP._core.Functions import recursiveWalk
		for item in recursiveWalk(self.words):
			if item == other:
				return True
			elif isinstance(item, SQLOOP) and type(item) == other:
				return True
		else:
			return False
	
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
		return type(self)(self.words, right)
	
	def __rsub__(self, left : Self|Word):
		return type(self)(left, self.words)

	def __mult__(self, right):
		from SQLOOP._core.Schema import ALL
		return type(self)((*self.words[:-1], self.words[-1](ALL)), right)
	
	@cached_property
	def startWord(self):
		
		from SQLOOP._core.Functions import recursiveWalk
		for item in recursiveWalk(self.words):
			if isinstance(item, Word):
				return type(item)
			elif isRelated(item, Word):
				return item
			else:
				return type(item)

	@property
	def cols(self):
		from SQLOOP._core.Schema import ALL
		from SQLOOP._core.Words import FROM, SELECT
		if isinstance(self.words[0], SELECT):
			if ALL not in self.words[0].content:
				return len(self.words[0].content)
			if self.words[1] is FROM:
				allCols = len(self.words[2].columns)
			elif isinstance(self.words[1], FROM):
				allCols = sum(map(lambda x:len(x.columns), self.words[1].content))
			else:
				raise MissingArgument(f"Could not find the 'FROM' of the following 'SELECT' statement:\n{str(self)}")
			return sum(map(lambda x:1 if x is not ALL else allCols, self.words[0].content))
		else:
			return None
	
	@property
	def content(self):
		for word in self.words:
			if hasattr(word, "content"):
				return word.content
	
	@property
	def params(self):
		params = []
		for word in self.words:
			if isinstance(word, SQLOOP):
				params.extend(getReadyAttr(word, "params", []))
		return params

class TableMeta(SQLStructure):

	columns : SQLDict[str,Column]
	linkedColumns : dict[str,"LinkedColumn"]
	constraints : tuple

	def __contains__(self, column : Column):
		return column in self.columns

	def __getattribute__(self, name: str) -> Any:
		value = super().__getattribute__(name)
		if isRelated(value, Column):
			return self.linkedColumns[str(value)]
		else:
			return value
		
	def __len__(self):
		return len(self.columns)

	def __sql__(self):
		sep = ",\n\t"
		return f"{self.__sql_name__} (\n\t{sep.join(itertools.chain(map(sql, self.columns), map(str, self.constraints)))}\n)"
	
	def __getitem__(self, index : int|str|sql):
		"""Shorthand for Table.columns[key]"""
		return self.columns[index]

class Table(SQLObject, HasColumns, metaclass=TableMeta):
	
	columns : SQLDict[str,Column]
	linkedColumns : dict[str,"LinkedColumn"]
	constraints : tuple = ()

	def __init_subclass__(cls, **kwargs):
		
		super().__init_subclass__(**kwargs)
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
		if cls.__doc__ is not None:
			cls.__doc__ += "\n".join(["```python", *(f"{cls.__name__}.{name} = {col.__name__} # {col}" for name, col in vars(cls).items() if isRelated(col, Column)), "```"])
		else:
			cls.__doc__ = "\n".join(["```python", *(f"{cls.__name__}.{name} = {col.__name__} # {col}" for name, col in vars(cls).items() if isRelated(col, Column)), "```"])
		cls.linkedColumns = {str(col):SQLStructure(str(col), (LinkedColumn,), {}, type=col.type, table=cls) for col in cls.columns}

	def __init__(self, database):
		self.database = database
	
	def __getitem__(self, index : int|str|sql):
		return self.columns[index]
	
	def __call__(self, *args, **kwargs):
		from SQLOOP._core.Databases import Selector
		selects = tuple(filter(lambda x:not isinstance(x, Comparison), args))
		where = tuple(itertools.chain(filter(lambda x:not isinstance(x, Comparison), args), map(lambda x:Comparison(x[0], "=", x[1]), kwargs.items())))
		return iter(Selector(self.database._connection, (type(self), ))[where])

class IndexMeta(SQLStructure):

	table : Any

	def __sql__(self):
		return f"{self} ON {self.table} ({', '.join(map(str, self.columns))})"

class Index(SQLObject, HasColumns, metaclass=IndexMeta):

	table : Any

	def __sql__(self):
		return f"{self} ON {self.table} ({', '.join(map(str, self.columns))})"

class LinkedColumn(Column): pass
