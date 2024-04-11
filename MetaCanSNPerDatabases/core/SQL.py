
from MetaCanSNPerDatabases.Globals import *
from Columns import *

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

class SQLWord:
	
	__name__ : str
	__content__ : Iterable

	def __str__(self) -> str:
		f"{self.__name__} {', '.join(self.__content__)}"

class SQL_STATEMENT:
	
	select : tuple[Column]
	tables : tuple[Table]
	where : dict[Column,Any]
	orderBy : tuple[Column]
	limit : tuple

	def __iter__(self):
		queryString = f"SELECT {', '.join(self.select)}"

		if len(self.tables) > 0:
			queryString += f" FROM {self.table:!sql}"
		elif self.table is not None:
			queryString += f" FROM {', '.join(map("{:!sql}".format, self.tables))}"
		else:
			raise DatabaseError(f"No table specified for SELECT {self.select}")
		
		return queryString, params

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