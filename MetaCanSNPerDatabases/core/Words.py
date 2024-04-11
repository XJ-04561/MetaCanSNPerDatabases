
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.Structures import *
from MetaCanSNPerDatabases.core.SQL import *

class Word:
	
	__name__ : str
	__content__ : tuple
	sep : str = ", "

	def __init__(self, *args : tuple[Table|Column|Index|Comparison]):
		self.__content__ = args

	def __getattr__(self, key):
		return SQL_STATEMENT(self).__getattribute__(key)

	def __repr__(self):
		return f"<{pluralize(self.__class__.__base__.__name__)}.{self.__class__.__name__} content={self.__content__}>"
		
	def __str__(self) -> str:
		return f"{self.__name__} {self.sep.join(self.__content__)}"
	
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
		
class EnclosedWord(Word):
	def __str__(self):
		return f"{self.__name__} ({self.sep.join(map(str, self.__content__))})"


class PRIMARY_KEY(EnclosedWord):
	__name__ : str = "PRIMARY KEY"
	
class FOREIGN_KEY(EnclosedWord):
	__name__ : str = "FOREIGN KEY"
	table : Table
	def __init__(self, keys : tuple[Column], table : Table, foreignKeys : tuple[Column]=None):
		self.table = table
		self.__content__ = tuple(filter([keys, foreignKeys]))

	def __str__(self):
		return f"FOREIGN KEY ({self.sep.join(map(str, self.__content__[0]))}) REFERENCES {self.table}({self.sep.join(map(str, self.__content__[-1]))})"

class UNIQUE(EnclosedWord):
	__name__ : str = "UNIQUE"

class SELECT(Word):
	__name__ : str = "SELECT"

class FROM(Word):
	__name__ : str = "FROM"

class WHERE(Word):
	__name__ : str = "WHERE"
	sep : str = "AND"

class ORDER_BY(Word):
	__name__ : str = "ORDER BY"

class LIMIT(Word):
	__name__ : str = "LIMIT"

class TABLE(Word):
	__name__ : str = "TABLE"
