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
	def __new__(cls, obj) -> str: ...

class SQLObject(AutoObject):
	
	__name__ : str
	name : str
	
	def __repr__(self) -> str: ...
	def __str__(self) -> str: ...
	def __sql__(self) -> str: ...
	def __hash__(self) -> int: ...
	def __format__(self, format_spec : str) -> str: ...

class Comparison(AutoObject):
	
	left : Any
	operator : str
	right : Any

	def __repr__(self) -> str: ...

	def __str__(self) -> str: ...

class Assignment(AutoObject):
	
	variable : str|Column
	value : Any

class Column(SQLObject):
	
	type : str
	
	@overload
	def __init__(self, name : str):
		"""Create a Column-object only referencing the column name inside the database."""
		...
	@overload
	def __init__(self, name : str, type : str):
		"""Create a Column-object only referencing the column name and type inside the database."""
		...
	@overload
	def __init__(self, __name__ : str, name : str, type : str):
		"""Create a Column-object referencing the column name inside python, as well as the column name and type inside the database."""
		...

	def __sql__(self) -> str: ...
	def __lt__(self, right) -> Comparison: ...
	def __gt__(self, right) -> Comparison: ...
	def __le__(self, right) -> Comparison: ...
	def __ge__(self, right) -> Comparison: ...
	def __eq__(self, right) -> Comparison: ...
	def __ne__(self, right) -> Comparison: ...
	def __contains__(self, right) -> Comparison: ...

class Table(SQLObject):

	columns : tuple[Column] = tuple()
	options : tuple[Query|Word] = tuple()
	
	def __sql__(self) -> str: ...
	def __hash__(self) -> int: ...
	def __repr__(self) -> str: ...
	def __iter__(self) -> Generator[Column,None,None]: ...

class Index(SQLObject):

	table : Any
	columns : tuple[Column]

	def __sql__(self) -> str: ...

class Query:

	words : tuple[Word] = tuple()

	def __init__(self, *words : Query|Word): ...
	def __iter__(self) -> Generator[str|list,None,None]: ...
	def __str__(self) -> str: ...
	def __format__(self, format_spec) -> str: ...
	def __sub__(self, other : Query|Word) -> Query: ...
	@property
	def params(self) -> list[Any]: ...

class Word:
	
	content : tuple
	sep : str = ", "

	def __init__(self, *args : tuple[Table|Column|Index|Comparison], **kwargs : dict[str,Any]):
		"""Keyword arguments count as comparisons of the type "X == Y"."""
		...

	def __sub__(self, other : Any) -> Query: ...
	def __repr__(self) -> str: ...
	def __str__(self) -> str: ...
	def __sql__(self) -> str: ...
	def __hash__(self) -> int: ...
	def __format__(self, format_spec : str) -> str: ...
	@property
	def params(self) -> list[Any]: ...

class EnclosedWord(Word):
	def __str__(self) -> str: ...
	
class Prefix(type):

	def __str__(self) -> str: ...
	def __sub__(self, other) -> Query: ...

ALL				= Column("ALL",				"*",					"")
SQLITE_MASTER	= Table("SQLITE_MASTER", "sqlite_master", (Column("type"), Column("name"), Column("tbl_name"), Column("rootpage"), Column("sql")))