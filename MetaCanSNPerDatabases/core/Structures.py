
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.SQL import *

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
	
	type : str
	
	def __sql__(self):
		return f"{self.name} {self.type}"
	
	def __neg__(self):
		"""Only exists for 'ORDER BY'-usage. Will create a copy of the Column object with ' ASC' appended to the in-table name of the column."""
		return Column(self.__name__, f"{self.name} ASC", self.type)
	
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
	options : tuple[PrimaryKey|ForeignKey|Unique] = tuple()
	_database : Database

	
	def __sql__(self):
		return f"{self.name} ({',\n\t'.join(ChainMap(map(sql, self.options), map(sql, self.columns)))})"
	
	def __hash__(self):
		return self.__sql__().__hash__()

	def __len__(self) -> int:
		return self._conn.execute(f"SELECT COUNT(*) FROM {self};").fetchone()[0]
	
	def __repr__(self):
		return f"<{__name__}.{self.__class__.__name__} rows={len(self)} columns={[c.__name__ for c in self.columns]} at {hex(id(self))}>"
	
	def __iter__(self):
		for row in self._database.SELECT(Column("ALL", "*", "")).FROM(self):
			yield row
	
	def recreate(self) -> bool:
		try:
			try:
				self.clearIndexes()
				self._conn.execute(f"ALTER TABLE {self:!sql} RENAME TO {self:!sql}2;")
			except:
				# Table doesn't exist
				pass
			
			self.create()
			
			try:
				self._conn.execute(f"INSERT INTO {self:!sql} SELECT * FROM {self:!sql}2;")
			except:
				pass

			self.createIndex()
			return True
		except:
			return False

	@Overload
	def createIndex(self : Self):
		for indexName, *indexColumns in self._indexes:
			for (indexName,) in self._conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", [self._tableName]):
				self._conn.execute(f"DROP INDEX IF EXISTS {indexName};")
			if indexName.isalnum() and all(map(str.isalnum, indexColumns)):
				self._conn.execute(f"CREATE INDEX {indexName} ON {self._tableName}({', '.join(indexColumns)});")

	@createIndex.add
	def createIndex(self : Self, *cols : Column, name : str=None):
		if name is None:
			name = f"{self._tableName}By{''.join(map(Columns.NAMES_STRING.__getitem__, cols))}"
		self._conn.execute(f"CREATE INDEX {name} ON {self._tableName}({', '.join(map(Columns.LOOKUP[self._tableName].__getitem__, cols))});")
	
	def clearIndexes(self):
		for (indexName,) in self._conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", [self._tableName]):
			self._conn.execute(f"DROP INDEX IF EXISTS {indexName};")

class Index:

	table : Any
	columns : list[Column]

	def __sql__(self):
		return f"{self} ON {self.table}({', '.join(map(str, self.columns))})"

class Query:

	words : tuple[Word] = tuple()

	def __init__(self, *words):
		self.words = words

	def __iter__(self):
		yield f"{' '.join(map(str, self.words))};"
		yield self.params
	
	def __str__(self):
		return f"({' '.join(map(str, self.words))})"

	@property
	def params(self):
		return [word.params for word in self.words if hasattr(word, "params")]

	def __getattr__(self, key):
		if key in map(lambda obj : obj.__name__, Word.__subclasses__()):
			return next(filter(lambda obj : obj.__name__ == key, Word.__subclasses__()))
		return self.__getattribute__(key)

class Word:
	
	__name__ : str
	__content__ : tuple
	sep : str = ", "

	def __init__(self, *args : tuple[Table|Column|Index|Comparison]):
		self.__content__ = args

	def __getattr__(self, key):
		return getattr(Query(self), key)

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
	
	@property
	def params(self):
		return [getattr(item, "params", None) or item for item in self.__content__ if not isinstance(item, Column|Table|Index)]
		
class EnclosedWord(Word):
	def __str__(self):
		return f"{self.__name__} ({self.sep.join(map(str, self.__content__))})"