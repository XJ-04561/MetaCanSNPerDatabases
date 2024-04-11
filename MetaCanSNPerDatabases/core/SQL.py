
from MetaCanSNPerDatabases.Globals import *


class sql(str):
	def __new__(cls, obj):
		return super().__new__(cls, obj.__sql__())
def params(object) -> list:
	return 

class Comparison(AutoObject):
	
	left : Any
	operator : str
	right : Any

	def __repr__(self):
		return f"<Comparison {str(self)!r}>"

	def __str__(self):
		f"{self.left} {self.operator} {self.right}"

class SQL_STATEMENT:

	words : tuple[Word] = tuple()

	def __init__(self, *words):
		self.words = words

	def __iter__(self):
		yield f"{' '.join(map(str, self.words))};"
		outParams = []
		list(map(outParams.extend, map(params, self.words)))
		yield outParams
	
	def __str__(self):
		return f"({' '.join(map(str, self.words))})"

	def SELECT(self, *columns):
		self.select = columns
		return self

	def FROM(self, *tables : tuple[Type[Table]]):
		self.tables = tables
		return self
	
	def WHERE(self, **wheres : dict[str,Any|Column]):
		self.where = wheres
		return self
	
	def ORDER_BY(self, *orderBy : tuple[Column]):
		self.orderBy = orderBy
		return self
	
	def LIMIT(self, limit : int):
		self.limit = limit