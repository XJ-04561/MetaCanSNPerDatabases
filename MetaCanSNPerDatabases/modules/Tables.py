
from MetaCanSNPerDatabases.modules.Globals import *
import MetaCanSNPerDatabases.modules.Globals as Globals
import MetaCanSNPerDatabases.modules.Columns as Columns
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
from MetaCanSNPerDatabases.modules._Constants import *



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

	@overload
	def createIndex(self, *cols : ColumnFlag, name : str=None): pass

	@overload
	def createIndex(self, *cols : ColumnFlag, name : str=None): pass

	@final
	def createIndex(self, *cols : ColumnFlag, name : str=None):
		
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
	def get(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, TreeParent : int=None, TreeChild : int=None, NodeID : int=None, Genotype : str=None, SNPID : str=None, Position : int=None, Ancestral : Literal["A","T","C","G"]=None, Derived : Literal["A","T","C","G"]=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> Generator[tuple[Any],None,None]|None:
		pass
	
	@final
	def get(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> Generator[tuple[Any],None,None]|None:
		from MetaCanSNPerDatabases.modules.Functions import generateTableQuery
		for row in self._conn.execute(*generateTableQuery(self, *select, orderBy=orderBy, **where)):
			yield row

	@overload
	def first(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, TreeParent : int=None, TreeChild : int=None, NodeID : int=None, Genotype : str=None, SNPID : str=None, Position : int=None, Ancestral : Literal["A","T","C","G"]=None, Derived : Literal["A","T","C","G"]=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> tuple[Any]:
		pass
	
	@final
	def first(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> tuple[Any]:
		for row in self.get(*select, orderBy=orderBy, **where):
			return row
	
	@overload
	def all(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, TreeParent : int=None, TreeChild : int=None, NodeID : int=None, Genotype : str=None, SNPID : str=None, Position : int=None, Ancestral : Literal["A","T","C","G"]=None, Derived : Literal["A","T","C","G"]=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> list[tuple[Any]]:
		pass
	
	@final
	def all(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> list[tuple[Any]]:
		return list(self.get(*select, orderBy=orderBy, **where))

class SNPTable(Table):

	_tableName = TABLE_NAME_SNP_ANNOTATION
	_columns = [
		SNP_COLUMN_NODE_ID,
		SNP_COLUMN_POSITION,
		SNP_COLUMN_ANCESTRAL,
		SNP_COLUMN_DERIVED,
		SNP_COLUMN_REFERENCE,
		SNP_COLUMN_DATE,
		SNP_COLUMN_CHROMOSOMES_ID
	]
	_types = [
		SNP_COLUMN_NODE_ID_TYPE,
		SNP_COLUMN_POSITION_TYPE,
		SNP_COLUMN_ANCESTRAL_TYPE,
		SNP_COLUMN_DERIVED_TYPE,
		SNP_COLUMN_REFERENCE_TYPE,
		SNP_COLUMN_DATE_TYPE,
		SNP_COLUMN_CHROMOSOMES_ID_TYPE
	]
	_appendRows = SNP_APPEND
	_indexes = SNP_INDEXES

class ReferenceTable(Table):

	_tableName = TABLE_NAME_REFERENCES
	_columns = [
		REFERENCE_COLUMN_GENOME_ID,
		REFERENCE_COLUMN_GENOME,
		REFERENCE_COLUMN_STRAIN,
		REFERENCE_COLUMN_GENBANK,
		REFERENCE_COLUMN_REFSEQ,
		REFERENCE_COLUMN_ASSEMBLY
	]
	_types = [
		REFERENCE_COLUMN_GENOME_ID_TYPE,
		REFERENCE_COLUMN_GENOME_TYPE,
		REFERENCE_COLUMN_STRAIN_TYPE,
		REFERENCE_COLUMN_GENBANK_TYPE,
		REFERENCE_COLUMN_REFSEQ_TYPE,
		REFERENCE_COLUMN_ASSEMBLY_TYPE
	]
	_appendRows = REFERENCE_APPEND
	_indexes = REFERENCE_INDEXES

class TreeTable(Table):

	_tableName = TABLE_NAME_TREE
	_columns = [
		TREE_COLUMN_PARENT,
		TREE_COLUMN_CHILD,
		TREE_COLUMN_NAME
	]
	_types = [
		TREE_COLUMN_PARENT_TYPE,
		TREE_COLUMN_CHILD_TYPE,
		TREE_COLUMN_NAME_TYPE
	]
	_appendRows = TREE_APPEND
	_indexes = TREE_INDEXES

class ChromosomesTable(Table):
	
	_tableName = TABLE_NAME_CHROMOSOMES
	_columns = [		
		CHROMOSOMES_COLUMN_ID,
		CHROMOSOMES_COLUMN_NAME,
		CHROMOSOMES_COLUMN_GENOME_ID
	]
	_types = [
		CHROMOSOMES_COLUMN_ID_TYPE,
		CHROMOSOMES_COLUMN_NAME_TYPE,
		CHROMOSOMES_COLUMN_GENOME_ID_TYPE
	]
	_appendRows = CHROMOSOMES_APPEND
	_indexes = CHROMOSOMES_INDEXES