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
		return "?"
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
		return f"{self.name} ({',\n\t'.join(ChainMap(map(sql, self.options), map(sql, self.columns)))})"
	
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