
from sqlite3 import Connection
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
import MetaCanSNPerDatabases.core.Columns as Columns
from MetaCanSNPerDatabases.core.Columns import Column
from MetaCanSNPerDatabases.core._Constants import *
from MetaCanSNPerDatabases.core.Tables import Table


from MetaCanSNPerDatabases.core.Tree import Branch

class IsLegacyCanSNPer2(sqlite3.Error): pass
class OutdatedCanSNPerDatabase(sqlite3.Error): pass

class Database:

	_connection : sqlite3.Connection
	_mode : str
	filename : str

	def __init__(self, database : sqlite3.Connection):
		from MetaCanSNPerDatabases.core.Tables import SNPsTable, ReferencesTable, TreeTable, ChromosomesTable
		self.filename = os.path.realpath(database.execute("PRAGMA database_list;").fetchone()[2])
		self._connection = database

		self.SNPsTable = SNPsTable(self._connection, self._mode)
		self.ReferencesTable = ReferencesTable(self._connection, self._mode)
		self.TreeTable = TreeTable(self._connection, self._mode)
		self.ChromosomesTable = ChromosomesTable(self._connection, self._mode)
	
	def __enter__(self):
		return self
	
	def __exit__(self):
		try:
			self._connection.commit()
		except:
			pass
		del self
	
	@property
	def __version__(self):
		return int(self._connection.execute("PRAGMA user_version;").fetchone()[0])

	def __del__(self):
		try:
			self._connection.close()
		except:
			pass
	
	def __repr__(self):
		return object.__repr__(self)[:-1] + f" version={self.__version__} tablesHash={self.tablesHash!r} tables={[(name, len(self.Tables[name])) for name in self.Tables]}>"
	
	@property
	def Tables(self) -> dict[str,Table]:
		return {name:self.__getattribute__(name) for name in sorted(filter(lambda s : s.endswith("Table"), self.__dict__))}

	def checkDatabase(self) -> int:
		if len(self._connection.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()) == 0:
			# Table is empty
			return -2
		elif self.__version__ == LEGACY_VERSION:
			return -3
		elif self.__version__ != CURRENT_VERSION:
			# Table does not have the right `user_version` set.
			return -4
		elif self.tablesHash != CURRENT_TABLES_HASH:
			# Table does not have the right schema version
			return -5
		elif self.indexesHash != CURRENT_INDEXES_HASH:
			# Table does not have the right indexes set
			return -6
		else:
			LOGGER.info(f"Database version is up to date! (v. {self.__version__})")
			return 0
	
	def validateDatabase(self, code: int, throwError : bool=True):
		match code:
			case 0: # We're good
				pass
			case -2: # Table is new
				LOGGER.exception(sqlite3.DatabaseError("Database is empty."))
				if throwError: raise sqlite3.DatabaseError("Database is empty.")
			case -3:
				LOGGER.exception(IsLegacyCanSNPer2("Database is a legacy CanSNPer database."))
				if throwError: raise IsLegacyCanSNPer2("Database is a legacy CanSNPer database. If opened in 'update' mode it can be converted.")
			case -4: # Version number missmatch
				LOGGER.exception(sqlite3.DatabaseError(f"Table does not have the right `user_version` set. (Determined version is v.{DATABASE_VERSIONS[self.tablesHash]} but user_version is v.{self.__version__})"))
				if throwError: raise sqlite3.DatabaseError(f"Table does not have the right `user_version` set. (Determined version is v.{DATABASE_VERSIONS[self.tablesHash]} but user_version is v.{self.__version__})")
			case -5: # Tables schema hash doesn't match the current version
				LOGGER.exception(OutdatedCanSNPerDatabase(f"Table schema does not match the most up to date schema. (Database: {self.tablesHash!r}, Latest MetaCanSNPerDatabases: {CURRENT_TABLES_HASH})"))
				if throwError: raise OutdatedCanSNPerDatabase(f"Table schema does not match the most up to date schema. (Database: {self.tablesHash!r}, Latest MetaCanSNPerDatabases: {CURRENT_TABLES_HASH})")
			case -6: # Indexes schema hash doesn't match the current version
				LOGGER.exception(OutdatedCanSNPerDatabase(f"Index schema does not match the most up to date schema. (Database: {self.indexesHash!r}, Latest MetaCanSNPerDatabases: {CURRENT_INDEXES_HASH})"))
				if throwError: raise OutdatedCanSNPerDatabase(f"Index schema does not match the most up to date schema. (Database: {self.indexesHash!r}, Latest MetaCanSNPerDatabases: {CURRENT_INDEXES_HASH})")
			case _:
				LOGGER.exception(sqlite3.DatabaseError(f"Unkown Database error. Current Database version is v.{CURRENT_VERSION}, and this database has version v.{self.__version__} (tablesHash={self.tablesHash!r})."))
				if throwError: raise sqlite3.DatabaseError(f"Unkown Database error. Current Database version is v.{CURRENT_VERSION}, and this database has version v.{self.__version__} (tablesHash={self.tablesHash!r}).")

	def rectifyDatabase(self, code : int):
		raise NotImplementedError("Not implemented in the base class.")

	@overload
	def get(self, *columnsToGet : Column, orderBy : Column|tuple[Column]|None=None, Parent : int=None, NodeID : int=None, Genotype : str=None, Position : int=None, Ancestral : Nucleotides=None, Derived : Nucleotides=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> Generator[tuple[Any],None,None]|None:
		pass

	@final
	def get(self, *select : Column, orderBy : Column|tuple[Column]|None=None, **where : Any) -> Generator[tuple[Any],None,None]|None:
		
		from MetaCanSNPerDatabases.core.Functions import generateQuery, interpretSQLtype
		for row in self._connection.execute(*generateQuery(*select, orderBy=orderBy, **where)):
			yield map(interpretSQLtype, row)
	
	@overload
	def first(self, *columnsToGet : Column, orderBy : Column|tuple[Column]|None=None, Parent : int=None, NodeID : int=None, Genotype : str=None, Position : int=None, Ancestral : Nucleotides=None, Derived : Nucleotides=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> tuple[Any]:
		pass
	
	@final
	def first(self, *select : Column, orderBy : Column|tuple[Column]|None=None, **where : Any) -> tuple[Any]:
		for row in self.get(*select, orderBy=orderBy, **where):
			return row
	
	@overload
	def all(self, *columnsToGet : Column, orderBy : Column|tuple[Column]|None=None, Parent : int=None, NodeID : int=None, Genotype : str=None, Position : int=None, Ancestral : Nucleotides=None, Derived : Nucleotides=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> list[tuple[Any]]:
		pass
	
	@final
	def all(self, *select : Column, orderBy : Column|tuple[Column]|None=None, **where : Any) -> list[tuple[Any]]:
		return list(self.get(*select, orderBy=orderBy, **where))

	@property
	def SNPs(self) -> Generator[tuple[str,int,str,str],None,None]:
		return self.SNPsTable.get(Columns.ALL)

	@property
	def references(self) -> Generator[tuple[int,str,str,str,str],None,None]:
		return self.ReferencesTable.get(Columns.ALL)
	
	@property
	def chromosomes(self) -> Generator[tuple[int,str],None,None]:
		return self.ChromosomesTable.get(Columns.ALL)

	@cached_property
	def tree(self) -> Branch:
		
		return Branch(self._connection, *self.TreeTable.first(Columns.NodeID, TreeParent=0))

	@property
	def indexes(self):
		return [row[0] for row in self._connection.execute("SELECT sql FROM sqlite_master WHERE type = 'index' ORDER BY name DESC;")]

	@property
	def indexesHash(self):
		from MetaCanSNPerDatabases.core.Functions import whitespacePattern
		indices = [name for table in self.Tables.values() for name, *_ in table._indexes]
		return hashlib.md5(
			whitespacePattern.sub(
				" ",
				"; ".join([
					x[0]
					for x in self._connection.execute(f"SELECT sql FROM sqlite_schema WHERE type='index' AND name IN ({', '.join(['?']*len(indices))}) ORDER BY sql DESC;", indices)
					if type(x) is tuple and x[0] is not None
				])
			).encode("utf-8")
		).hexdigest()

	@property
	def tablesHash(self):
		from MetaCanSNPerDatabases.core.Functions import whitespacePattern
		return hashlib.md5(
			whitespacePattern.sub(
				" ",
				"; ".join([
					x[0]
					for x in self._connection.execute(f"SELECT sql FROM sqlite_schema WHERE type='table' ORDER BY sql DESC;")
					if type(x) is tuple and x[0] is not None
				])
			).encode("utf-8")
		).hexdigest()
	
	def close(self):
		try:
			self._connection.close()
		except:
			pass

class DatabaseReader(Database):
	
	_mode = "r"

	def rectifyDatabase(self, code : int) -> Never:
		raise PermissionError("Can't rectify a database opened in read-only mode.")
	
class DatabaseWriter(Database):

	_mode = "w"

	def rectifyDatabase(self, code : int, copy : bool=True, refDir : Path|PathGroup=None):
		from MetaCanSNPerDatabases.core.Functions import updateFromLegacy
		if copy: shutil.copy(self.filename, self.filename+".backup")
		if refDir is None:
			refDir = CommonGroups.shared / f"{SOFTWARE_NAME}-Data" / pName(self.filename)
		match code:
			case 0: # We're good
				pass
			case -2: # Table is new
				self._connection.execute("BEGIN TRANSACTION;")
				for table in self.Tables.values():
					table.create()
				self._connection.execute(f"PRAGMA user_version = {CURRENT_VERSION:d};")
				self._connection.execute("COMMIT;")
			case -3: # Legacy CanSNPer table
				updateFromLegacy(self, refDir=refDir)
			case -4:
				self._connection.execute("BEGIN TRANSACTION;")
				self._connection.execute(f"PRAGMA user_version = {CURRENT_VERSION:d};")
				self._connection.execute("COMMIT;")
			case -5: # Transfer data from old tables into new tables
				self._connection.execute("BEGIN TRANSACTION;")
				self.clearIndexes()
				for table in self.Tables.values():
					table.recreate()
					table.createIndex()
				self._connection.execute("COMMIT;")
				self._connection.execute("BEGIN TRANSACTION;")
				for (table,) in self._connection.execute("SELECT name FROM sqlite_master WHERE type='table';"):
					if table not in TABLES:
						self._connection.execute(f"DROP TABLE {table};")
				self._connection.execute(f"PRAGMA user_version = {CURRENT_VERSION:d};")
				self._connection.execute("COMMIT;")
			case -6:
				self._connection.execute("BEGIN TRANSACTION;")
				self.clearIndexes()
				for table in self.Tables.values():
					table.createIndex()
				self._connection.execute("COMMIT;")

	def clearIndexes(self):
		for (indexName, ) in self._connection.execute("SELECT name FROM sqlite_schema WHERE type='index';"):
			self._connection.execute(f"DROP INDEX {indexName};")

	def addSNP(self, nodeID, snpID, position, ancestral, derived, reference, date, chromosomeID):
		self._connection.execute(f"INSERT (?,?,?,?,?,?,?,?) INTO {TABLE_NAME_SNP_ANNOTATION};", [nodeID, snpID, position, ancestral, derived, reference, date, chromosomeID])
	
	def addReference(self, genome, strain, genbank, refseq, assemblyName):
		self._connection.execute(f"INSERT (null,?,?,?,?,?) INTO {TABLE_NAME_REFERENCES};", [genome, strain, genbank, refseq, assemblyName])
	
	def addBranch(self, parent : int|str=None, name : str=None):
		if type(parent) is int:
			self._connection.execute(f"INSERT (?,null,?) INTO {TABLE_NAME_TREE};", [parent, name])
		else:
			self._connection.execute(f"INSERT ({TABLE_NAME_TREE}.child,null,?) INTO {TABLE_NAME_TREE} FROM {TABLE_NAME_TREE} WHERE {TABLE_NAME_TREE}.name = ?;", [name, parent])
	
	def addChromosome(self, chromosomeName : str=None, genomeID : int=None, genomeName : str=None):
		if genomeID is not None:
			self._connection.execute(f"INSERT (null,?,?) INTO {TABLE_NAME_CHROMOSOMES};", [chromosomeName, genomeID])
		elif genomeName is not None:
			self._connection.execute(f"INSERT (null,?,{TABLE_NAME_CHROMOSOMES}.id) INTO {TABLE_NAME_CHROMOSOMES} FROM {TABLE_NAME_CHROMOSOMES} WHERE {TABLE_NAME_CHROMOSOMES}.genome = ?;", [chromosomeName, genomeName])
	
	def commit(self):
		self._connection.commit()

@overload
def openDatabase(database : str, mode : ReadMode) -> DatabaseReader: pass

@overload
def openDatabase(database : str, mode : WriteMode) -> DatabaseWriter: pass

@final
def openDatabase(database : str, mode : Mode) -> DatabaseReader | DatabaseWriter | None:
	match mode:
		case "r":
			if not os.path.exists(database):
				raise FileNotFoundError(f"Database file {database} not found on the system.")
			elif not os.path.isabs(database):
				database = os.path.realpath(os.path.expanduser(database))
					
			# Convert to URI acceptable filename
			cDatabase = "/".join(filter(lambda s : s != "", database.replace('?', '%3f').replace('#', '%23').split(os.path.sep)))
			if not cDatabase.startswith("/"): # Path has to be absolute already, and windows paths need a prepended '/'
				cDatabase = "/"+cDatabase
			try:
				return DatabaseReader(sqlite3.connect(f"file:{cDatabase}?immutable=1", uri=True))
			except Exception as e:
				LOGGER.error("Failed to connect to database using URI: "+f"file:{cDatabase}?immutable=1")
				raise e
		case "w":
			if os.path.exists(database):
				return DatabaseWriter(sqlite3.connect(database))
			else:
				conn = sqlite3.connect(database)
				conn.execute(f"PRAGMA user_version = {CURRENT_VERSION};")
				ret = DatabaseWriter(conn)

				ret.ReferencesTable.create()
				ret.ChromosomesTable.create()
				ret.TreeTable.create()
				ret.SNPsTable.create()

				return ret
		case _:
			raise ValueError(f"Improper mode in which to open database. Can only be {'r'!r} (read) or {'w'!r} (write), not {mode!r}")
