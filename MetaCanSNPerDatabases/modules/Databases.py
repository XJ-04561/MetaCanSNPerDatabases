
from MetaCanSNPerDatabases.modules.Globals import *
import MetaCanSNPerDatabases.modules.Globals as Globals
import MetaCanSNPerDatabases.modules.Columns as Columns
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
from MetaCanSNPerDatabases.modules._Constants import *


from MetaCanSNPerDatabases.modules.Tree import Branch

class IsLegacyCanSNPer2(sqlite3.Error): pass
class OutdatedCanSNPerDatabase(sqlite3.Error): pass

class Database:

	_connection : sqlite3.Connection
	_mode : str
	filename : str

	def __init__(self, database : sqlite3.Connection):
		from MetaCanSNPerDatabases.modules.Tables import SNPTable, ReferenceTable, TreeTable, ChromosomesTable
		self.filename = database.execute("PRAGMA database_list;").fetchone()[2]
		self._connection = database

		self.SNPTable = SNPTable(self._connection, self._mode)
		self.ReferenceTable = ReferenceTable(self._connection, self._mode)
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
		return DATABASE_VERSIONS.get(self.schemaHash, 0)

	def __del__(self):
		try:
			self._connection.close()
		except:
			pass
	
	def __repr__(self):
		return object.__repr__(self)[:-1] + f" version={self.__version__} schemaHash={self.schemaHash!r} tables={list(zip(TABLES,map(len, TABLES)))}>"
	
	@property
	def Tables(self) -> dict:
		return {name:self.__getattribute__(name) for name in sorted(filter(lambda s : s.endswith("Table"), self.__dict__))}

	def checkDatabase(self) -> int:
		if len(self._connection.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()) == 0:
			# Table is empty
			return -2
		elif self.__version__ == LEGACY_VERSION:
			return -3
		elif self.__version__ != CURRENT_VERSION:
			LOGGER.warning(f"Database version (v.{self.__version__}) does not match the currently set version (v.{CURRENT_VERSION}).")
			if Globals.STRICT:
				raise sqlite3.DatabaseError(f"Database version (v.{self.__version__}) does not match the currently set version (v.{CURRENT_VERSION}).")
			else:
				# Table does not have the right schema version
				return -4
		elif CURRENT_VERSION != self._connection.execute('PRAGMA user_version;').fetchone()[0]:
			# Table does not have the right `user_version` set.
			LOGGER.warning(f"Table does not have the right `user_version` set. (Determined version is v.{self.__version__} but user_version is v.{self._connection.execute('PRAGMA user_version;').fetchone()[0]})")
			return -5
		else:
			LOGGER.info(f"Database version is up to date! (v. {self.__version__})")
			return 0
	
	def validateDatabase(self, code: int):
		match code:
			case 0: # We're good
				pass
			case -2: # Table is new
				raise sqlite3.DatabaseError("Database is empty.")
			case -3:
				raise IsLegacyCanSNPer2("Database is a legacy CanSNPer database. If opened in '--write' mode it can be converted.")
			case -4: # Transfer data from old tables into new tables
				raise OutdatedCanSNPerDatabase(f"Database version ({self.__version__}) does not match the currently set version. (user_version={self._connection.execute('PRAGMA user_version;').fetchone()[0]}, schemaHash={self.schemaHash!r})")
			case -5: # Version number missmatch
				raise sqlite3.DatabaseError(f"Table does not have the right `user_version` set. (Determined version is v.{self.__version__} but user_version is v.{self._connection.execute('PRAGMA user_version;').fetchone()[0]})")
			case _:
				raise sqlite3.DatabaseError(f"Unkown Database error. Current Database version is v.{CURRENT_VERSION}, and this database has version v.{self.__version__} (schemaHash={self.schemaHash!r}).")

	def rectifyDatabase(self, code : int):
		raise NotImplementedError("Not implemented in the base class.")

	@overload
	def get(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag,Literal["DESC","ASC"]]|list[tuple[ColumnFlag,Literal["DESC","ASC"]]]=[], TreeParent : int=None, TreeChild : int=None, NodeID : int=None, Genotype : str=None, SNPID : str=None, Position : int=None, Ancestral : Literal["A","T","C","G"]=None, Derived : Literal["A","T","C","G"]=None, SNPReference : str=None, Date : str=None, ChromID : int=None, Chromosome : str=None, GenomeID : int=None, Genome : str=None, Strain : str=None, GenbankID : str=None, RefseqID : str=None, Assembly : str=None) -> Generator[tuple[Any],None,None]:
		pass

	@final
	def get(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag,Literal["DESC","ASC"]]|list[tuple[ColumnFlag,Literal["DESC","ASC"]]]=[], **where : Any) -> Generator[tuple[Any],None,None]:
		
		from MetaCanSNPerDatabases.modules.Functions import generateQuery
		for row in self._connection.execute(*generateQuery(*select, orderBy=orderBy, **where)):
			yield row
	
	@property
	def SNPs(self) -> Generator[tuple[str,int,str,str],None,None]:
		return self.SNPTable.get(Columns.ALL)

	@property
	def references(self) -> Generator[tuple[int,str,str,str,str],None,None]:
		return self.ReferenceTable.get(Columns.ALL)
	
	@property
	def chromosomes(self) -> Generator[tuple[int,str],None,None]:
		return self.ChromosomesTable.get(Columns.ALL)

	@cached_property
	def tree(self) -> Branch:
		
		return Branch(self._connection, *self.TreeTable.first(Columns.TreeChild, TreeParent=0))

	@property
	def schemaHash(self):
		from MetaCanSNPerDatabases.modules.Functions import whitespacePattern
		return hashlib.md5(
			whitespacePattern.sub(
				" ",
				"; ".join([
					x[0]
					for x in self._connection.execute(f"SELECT sql FROM sqlite_schema ORDER BY sql DESC;")
					if type(x) is tuple and x[0] is not None
				])
			).encode("utf-8")
		).hexdigest()

class DatabaseReader(Database):
	
	_mode = "r"

	def rectifyDatabase(self, code):
		raise PermissionError("Can't rectify a database opened in read-only mode.")
	
class DatabaseWriter(Database):

	_mode = "w"

	def rectifyDatabase(self, code : int, copy : bool=True):
		from MetaCanSNPerDatabases.modules.Functions import updateFromLegacy
		if copy: shutil.copy(self.filename, self.filename+".backup")
		match code:
			case 0: # We're good
				pass
			case -2: # Table is new
				for table in self.Tables.values():
					table.create()
				self._connection.execute(f"PRAGMA user_version = {self.__version__:d};")
			case -3: # Legacy CanSNPer table
				updateFromLegacy(self.filename)
			case -4: # Transfer data from old tables into new tables
				for table in self.Tables.values():
					table.recreate()
				for (table,) in self._connection.execute("SELECT name FROM sqlite_master WHERE type='table';"):
					if table not in TABLES:
						self._connection.execute(f"DROP TABLE {table};")
				self._connection.execute(f"PRAGMA user_version = {self.__version__:d};")
			case -5:
				self._connection.execute(f"PRAGMA user_version = {self.__version__:d};")

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
def openDatabase(database : str, mode : Literal["r"]) -> DatabaseReader:
	pass

@overload
def openDatabase(database : str, mode : Literal["w"]) -> DatabaseWriter:
	pass

@final
def openDatabase(database : str, mode : Literal["r", "w"]) -> DatabaseReader | DatabaseWriter | None:
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
			return DatabaseWriter(sqlite3.connect(database))
