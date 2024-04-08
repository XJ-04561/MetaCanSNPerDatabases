
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
import MetaCanSNPerDatabases.core.Columns as Columns
from MetaCanSNPerDatabases.core.Columns import *
from MetaCanSNPerDatabases.core._Constants import *
from MetaCanSNPerDatabases.core.Indexes import Index



class Table:

	_conn : sqlite3.Connection
	_tableName : str
	_columns : list[str]
	_types : list[tuple[str]]
	_appendRows : list[str]
	_relationships : dict[str,Column]
	_indexes : list[tuple[str]]
	_mode : str

	def __init__(self, conn : sqlite3.Connection, mode : str):
		self._conn = conn
		self._mode = mode

	def __len__(self) -> int:
		return self._conn.execute(f"SELECT COUNT(*) FROM {self:!sql};").fetchone()[0]
	
	def __repr__(self):
		return f"<{__name__}.{self.__class__.__name__} rows={len(self)} columns={self._columns} at {hex(id(self))}>"
	
	def __str__(self):
		return self.__class__.__name__
	
	def __iter__(self):
		for row in self.get(Columns.ALL):
			yield row

	def __format__(self, format_spec : str):
		match format_spec.rsplit("!", 1)[-1]:
			case "!sql":
				return self._tableName.__format__(format_spec.rstrip("!sql"))
			case "!sql.CREATE":
				queryString = [f"{name} {' '.join(colType)}" for name, colType in zip(self._columns, self._types)]
				queryString += self._appendRows

				return f"{self._tableName} (\n\t\t" + ",\n\t\t".join(queryString) + "\n)"
			case _:
				return self.__class__.__name__.__format__(format_spec)

	def create(self) -> bool:
		try:
			self._conn.execute(f"CREATE TABLE IF NOT EXISTS {self:!sql.CREATE};")
			return True
		except:
			return False
	
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
	
	def get(self, *select : Column, orderBy : Column|tuple[Column]|None=None, **where : Any) -> Generator[tuple[Any],None,None]|None:
		from MetaCanSNPerDatabases.core.Functions import generateTableQuery
		for row in self._conn.execute(*generateTableQuery(self, *select, orderBy=orderBy, **where)):
			yield row

	def first(self, *select : Column, orderBy : Column|tuple[Column]|None=None, **where : Any) -> tuple[Any]:
		for row in self.get(*select, orderBy=orderBy, **where):
			return row
	
	def all(self, *select : Column, orderBy : Column|tuple[Column]|None=None, **where : Any) -> list[tuple[Any]]:
		return list(self.get(*select, orderBy=orderBy, **where))

class TreeTable(Table): pass
class SNPsTable(Table): pass
class ReferencesTable(Table): pass
class ChromosomesTable(Table): pass

class TreeTable(Table):

	_tableName = TABLE_NAME_TREE
	_columns = [
		Parent,
		NodeID,
		GenoType
	]
	_appendRows = TREE_APPEND
	_relationships = {
		SNPsTable : NodeID
	}
	@cached_property
	def indexes(self):
		return [
			Index(self, Parent)
		]

class SNPsTable(Table):

	_tableName = TABLE_NAME_SNP_ANNOTATION
	_columns = [
		NodeID,
		Position,
		Ancestral,
		Derived,
		SNPReference,
		Date,
		ChromID
	]
	_appendRows = SNP_APPEND
	_relationships = {
		TreeTable : NodeID,
		ChromosomesTable : ChromID
	}
	
	@cached_property
	def indexes(self):
		return [
			Index(self, Position),
			Index(self, ChromID),
			Index(self, NodeID)
		]

class ChromosomesTable(Table):
	
	_tableName = TABLE_NAME_CHROMOSOMES
	_columns = [		
		ChromID,
		Chromosome,
		GenomeID
	]
	_appendRows = CHROMOSOMES_APPEND
	_relationships = {
		SNPsTable : NodeID,
		ReferencesTable : GenomeID
	}

	@cached_property
	def indexes(self):
		return [
			Index(self, GenomeID)
		]

class ReferencesTable(Table):

	_tableName = TABLE_NAME_REFERENCES
	_columns = [
		GenomeID,
		Genome,
		Strain,
		GenbankID,
		RefseqID,
		Assembly
	]
	_appendRows = REFERENCE_APPEND
	_relationships = {
		ChromosomesTable : GenomeID
	}
	
	@cached_property
	def indexes(self):
		return [
			Index(self, Genome),
			Index(self, Assembly)
		]


class Tables: pass
Tables = SNPsTable | ReferencesTable | TreeTable | ChromosomesTable