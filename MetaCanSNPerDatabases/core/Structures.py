
from MetaCanSNPerDatabases.Globals import *

class Column: pass
class Table: pass
class Index: pass
class PrimaryKey: pass
class ForeignKey: pass
class Unique: pass
class Database: pass

class Column(SQLObject):
	
	type : str
	
	def __sql__(self):
		return f"{self.name} {self.type}"
	
	def __neg__(self):
		"""Only exists for 'ORDER BY'-usage. Will create a copy of the Column object with ' ASC' appended to the in-table name of the column."""
		return Column(self.__name__, f"{self.name} ASC", self.type)

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

class PrimaryKey(SQLObject):

	columns : list[Column]

	def __sql__(self):
		return f"PRIMARY KEY ({', '.join(map(str, self.columns))})"
	
class ForeignKey(SQLObject):

	table : Table
	columns : list[Column]

	def __sql__(self):
		return f"FOREIGN KEY ({', '.join(map(str, self.columns))}) REFERENCES {self.table}({', '.join(map(str, self.columns))})"

class Unique(SQLObject):

	columns : list[Column]

	def __sql__(self):
		return f"UNIQUE ({', '.join(map(str, self.columns))})"