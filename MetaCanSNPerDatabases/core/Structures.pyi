
from MetaCanSNPerDatabases.Globals import *

class Column: pass
class Table: pass
class Index: pass
class PrimaryKey: pass
class ForeignKey: pass
class Unique: pass
class Database: pass

class sql(str):
	def __new__(cls, obj):
		return super().__new__(cls, obj.__sql__())

class SQLObject(AutoObject):
	
	__name__ : str
	name : str
	_database : Database
	
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

class Column(SQLObject):
	def __init__(self, __name__ : str, name : str, type : str): ...

class Table(SQLObject):
    def __init__(self, __name__ : str, name : str, columns : Iterable[Column], options : Iterable[PrimaryKey|ForeignKey|Unique]): ...

class Index(SQLObject):
	def __init__(self, __name__ : str, name : str, table : Table, columns : Column|Iterable[Column]): ...

class Query:
	words : tuple[Word] = tuple()
	def __init__(self, *words): ...

class Word:
	content : tuple
	sep : str = ", "
	def __init__(self, *args : tuple[Table|Column|Index|Comparison], **kwargs : dict[str,Any]): ...

class EnclosedWord(Word):
	def __str__(self):
		return f"{self.__class__.__name__} ({self.sep.join(map(str, self.content))})"
	
class Prefix(type):
	def __str__(self):
		return self.__name__
	def __sub__(self, other):
		return Query(self, other)

