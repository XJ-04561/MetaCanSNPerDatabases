
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.Structures import *
from MetaCanSNPerDatabases.core.SQL import *


class PRIMARY_KEY(EnclosedWord):
	__name__ : str = "PRIMARY KEY"
	
class FOREIGN_KEY(EnclosedWord):
	__name__ : str = "FOREIGN KEY"
	table : Table

	@Overload
	def __init__(self, keys : tuple[Column], table : Table, foreignKeys : tuple[Column]):
		self.table = table
		self._left = keys
		self._right = foreignKeys
		self.__content__ = self._left + self._right

	@__init__.add
	def _(self, keys : tuple[Column], table : Table):
		self.table = table
		self.__content__= self._left = self._right  = keys

	def __str__(self):
		return f"FOREIGN KEY ({self.sep.join(map(str, self._left))}) REFERENCES {self.table}({self.sep.join(map(str, self._right))})"

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

class Words: pass
Words = PRIMARY_KEY | FOREIGN_KEY | UNIQUE | SELECT | FROM | WHERE | ORDER_BY | LIMIT | TABLE