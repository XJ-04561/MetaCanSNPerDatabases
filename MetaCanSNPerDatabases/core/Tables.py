
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
import MetaCanSNPerDatabases.core.Columns as Columns
from MetaCanSNPerDatabases.core.Columns import ColumnFlag
from MetaCanSNPerDatabases.core._Constants import *



class Table:

	_conn : sqlite3.Connection
	_tableName : str
	_columns : list[str]
	_types : list[tuple[str]]
	_appendRows : list[str]
	_indexes : list[tuple[str]]
	_mode : str

	def __init__(self, conn : sqlite3.Connection, mode : str):
		self._conn = conn
		self._mode = mode

	def __len__(self) -> int:
		return self._conn.execute(f"SELECT COUNT(*) FROM {self._tableName};").fetchone()[0]
	
	def __repr__(self):
		return object.__repr__(self)[:-1] + f" rows={len(self)} columns={self._columns}>"
	
	def __iter__(self):
		for row in self.get(Columns.ALL):
			yield row

	def create(self) -> bool:
		try:
			queryString = [f"{name} {' '.join(colType)}" for name, colType in zip(self._columns, self._types)]
			queryString += self._appendRows

			self._conn.execute(f"CREATE TABLE IF NOT EXISTS {self._tableName} (\n\t\t" + ",\n\t\t".join(queryString) + "\n);")
			return True
		except:
			return False
	
	def recreate(self) -> bool:
		try:
			try:
				self.clearIndexes()
				self._conn.execute(f"ALTER TABLE {self._tableName} RENAME TO {self._tableName}2;")
			except:
				# Table doesn't exist
				pass
			
			self.create()
			
			try:
				self._conn.execute(f"INSERT INTO {self._tableName} SELECT * FROM {self._tableName}2;")
			except:
				pass

			self.recreateIndexes()
			return True
		except:
			return False

	@dispatch(Self)
	def createIndex(self : Self):
		pass

	@dispatch(Self)
	def createIndex(self : Self, *cols : ColumnFlag, name : str=None):
		"""Create an index on this table `self` with the columns that are given in the method call. If no columns are specified, will create the default indexes defined in the `self._indexes` attribute.

		Args:
			*cols (ColumnFlarg, optional): Columns to be used in the index.
			name (str, optional): Name of index to be created, a value of None will generate a name based on the table and the columns involved. Defaults to None.
		"""
		if len(cols) > 0:
			if name is None:
				name = f"{self._tableName}By{''.join(map(Columns.NAMES_STRING.__getitem__, cols))}"
			self._conn.execute(f"CREATE INDEX {name} ON {self._tableName}({', '.join(map(Columns.LOOKUP[self._tableName].__getitem__, cols))});")
		else:
			for indexName, *indexColumns in self._indexes:
				for (indexName,) in self._conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", [self._tableName]):
					self._conn.execute(f"DROP INDEX IF EXISTS {indexName};")
				if indexName.isalnum() and all(map(str.isalnum, indexColumns)):
					self._conn.execute(f"CREATE INDEX {indexName} ON {self._tableName}({', '.join(indexColumns)});")

	def clearIndexes(self):
		for (indexName,) in self._conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", [self._tableName]):
			self._conn.execute(f"DROP INDEX IF EXISTS {indexName};")

	@overload
	def get(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, TreeParent : int=None, NodeID : int=None, Genotype : str=None, Position : int=None, Ancestral : Nucleotides=None, Derived : Nucleotides=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> Generator[tuple[Any],None,None]|None:
		pass
	
	@final
	def get(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> Generator[tuple[Any],None,None]|None:
		from MetaCanSNPerDatabases.core.Functions import generateTableQuery
		for row in self._conn.execute(*generateTableQuery(self, *select, orderBy=orderBy, **where)):
			yield row

	@overload
	def first(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, TreeParent : int=None, NodeID : int=None, Genotype : str=None, Position : int=None, Ancestral : Nucleotides=None, Derived : Nucleotides=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> tuple[Any]:
		pass
	
	@final
	def first(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> tuple[Any]:
		for row in self.get(*select, orderBy=orderBy, **where):
			return row
	
	@overload
	def all(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, TreeParent : int=None, NodeID : int=None, Genotype : str=None, Position : int=None, Ancestral : Nucleotides=None, Derived : Nucleotides=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> list[tuple[Any]]:
		pass
	
	@final
	def all(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> list[tuple[Any]]:
		return list(self.get(*select, orderBy=orderBy, **where))

class SNPTable(Table):

	_tableName = TABLE_NAME_SNP_ANNOTATION
	_columns = [
		COLUMN_NODE_ID,
		COLUMN_POSITION,
		COLUMN_ANCESTRAL,
		COLUMN_DERIVED,
		COLUMN_REFERENCE,
		COLUMN_DATE,
		COLUMN_CHROMOSOME_ID
	]
	_types = SNP_COLUMN_TYPES
	_appendRows = SNP_APPEND
	_indexes = SNP_INDEXES

class ReferenceTable(Table):

	_tableName = TABLE_NAME_REFERENCES
	_columns = [
		COLUMN_GENOME_ID,
		COLUMN_GENOME,
		COLUMN_STRAIN,
		COLUMN_GENBANK,
		COLUMN_REFSEQ,
		COLUMN_ASSEMBLY
	]
	_types = REFERENCES_COLUMN_TYPES
	_appendRows = REFERENCE_APPEND
	_indexes = REFERENCE_INDEXES

class TreeTable(Table):

	_tableName = TABLE_NAME_TREE
	_columns = [
		COLUMN_PARENT,
		COLUMN_NODE_ID,
		COLUMN_GENOTYPE
	]
	_types = TREE_COLUMN_TYPES
	_appendRows = TREE_APPEND
	_indexes = TREE_INDEXES

class ChromosomesTable(Table):
	
	_tableName = TABLE_NAME_CHROMOSOMES
	_columns = [		
		COLUMN_CHROMOSOME_ID,
		COLUMN_CHROMOSOME,
		COLUMN_GENOME_ID
	]
	_types = CHROMOSOMES_COLUMN_TYPES
	_appendRows = CHROMOSOMES_APPEND
	_indexes = CHROMOSOMES_INDEXES