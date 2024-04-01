
from MetaCanSNPerDatabases.modules.Globals import *
import MetaCanSNPerDatabases.modules.Globals as Globals
import MetaCanSNPerDatabases.modules.Columns as Columns
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
from MetaCanSNPerDatabases.modules._Constants import *

from MetaCanSNPerDatabases.modules.Functions import generateTableQuery

class TableDefinitionMissmatch(Exception): pass


class Table:

	_conn : sqlite3.Connection
	_tableName : str
	_columns : list[str]
	_types : list[tuple[str]]
	_appendRows : list[str]
	_mode : Mode

	def __init__(self, conn : sqlite3.Connection, mode : Mode):
		self._conn = conn
		self._mode = mode

	def __len__(self) -> int:
		return self._conn.execute(f"SELECT COUNT(*) FROM {self._tableName};").fetchone()[0]
	
	def __repr__(self):
		return object.__repr__(self)[:-1] + f" rows={len(self)} columns={self._columns}>"

	def create(self) -> bool:
		try:
			queryString = [f"{name} {' '.join(colType)}" for name, colType in zip(self._columns, self._types)]
			queryString += self._appendRows

			self._conn.execute(f"CREATE TABLE IF NOT EXISTS {self._tableName} (\n\t\t{',\n\t\t'.join(queryString)}\n);")
			return True
		except:
			return False
	
	def recreate(self) -> bool:
		try:
			try:
				self._conn.execute(f"ALTER TABLE {self._tableName} RENAME TO {self._tableName}2;")
			except:
				# Table doesn't exist
				pass
			
			self.create()
			
			try:
				self._conn.execute(f"INSERT INTO {self._tableName} SELECT * FROM {self._tableName}2;")
			except:
				pass
			return True
		except:
			return False

	@overload
	def get(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, TreeParent : int=None, TreeChild : int=None, NodeID : int=None, Genotype : str=None, SNPID : str=None, Position : int=None, Ancestral : Literal["A","T","C","G"]=None, Derived : Literal["A","T","C","G"]=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> Generator[tuple[Any]]:
		pass
		"""
		, TreeParent : int=None, TreeChild : int=None, NodeID : int=None, Genotype : str=None, SNPID : str=None, Position : int=None, Ancestral : Literal["A","T","C","G"]=None, Derived : Literal["A","T","C","G"]=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None
		"""
	
	@final
	def get(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> Generator[tuple[Any]]:
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

Table.get.__doc__ = Table.first.__doc__ = Table.all.__doc__ = generateTableQuery.__doc__

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

class TreeTable(Table):

	_tableName = TABLE_NAME_TREE
	_columns = [
		TREE_COLUMN_PARENT,
		TREE_COLUMN_CHILD
	]
	_types = [
		TREE_COLUMN_PARENT_TYPE,
		TREE_COLUMN_CHILD_TYPE
	]
	_appendRows = TREE_APPEND

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