
from MetaCanSNPerDatabases.modules.Globals import *
import MetaCanSNPerDatabases.modules.Globals as Globals
import MetaCanSNPerDatabases.modules.Columns as Columns
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
from MetaCanSNPerDatabases.modules._Constants import *

from MetaCanSNPerDatabases.modules.Tables import Table, SNPTable, ReferenceTable, NodeTable, TreeTable, RankTable, GenomesTable
from MetaCanSNPerDatabases.modules.Tree import Branch
from MetaCanSNPerDatabases.modules.Functions import generateQuery, whitespacePattern

class Database:

	_connection : sqlite3.Connection
	_mode : str
	filename : str

	def __init__(self, database : sqlite3.Connection):
		self.filename = database.execute("PRAGMA database_list;").fetchone()[2]
		self._connection = database

		self.SNPTable = SNPTable(self._connection, self._mode)
		self.ReferenceTable = ReferenceTable(self._connection, self._mode)
		self.NodeTable = NodeTable(self._connection, self._mode)
		self.TreeTable = TreeTable(self._connection, self._mode)
		self.RankTable = RankTable(self._connection, self._mode)
		self.GenomesTable = GenomesTable(self._connection, self._mode)
	
	def __enter__(self):
		return self
	
	def __exit__(self):
		try:
			self.commit()
		except:
			pass
		del self
	
	@property
	def __version__(self):
		return DATABASE_VERSIONS.get(self.schemaHash, "Unknown")

	def __del__(self):
		try:
			self._connection.close()
		except:
			pass
	
	def __repr__(self):
		return object.__repr__(self)[:-1] + f" version={self.__version__} schemaHash={self.schemaHash} tables={list(zip(TABLES,map(len, TABLES)))}>"
	
	@property
	def Tables(self) -> list[Table]:
		return [self.__getattribute__(name) for name in sorted(filter(lambda s : s.endswith("Table"), self.__dict__))]

	def validateDatabase(self) -> int:
		if len(self._connection.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()) == 0:
			# Table is empty
			return -2
		elif self.__version__ != CURRENT_VERSION:
			LOGGER.warning(f"Database version (v.{self.__version__}) does not match the currently set version (v.{CURRENT_VERSION}).")
			if Globals.STRICT:
				raise sqlite3.DatabaseError(f"Database version (v.{self.__version__}) does not match the currently set version (v.{CURRENT_VERSION}).")
			else:
				# Table does not have the right schema version
				return -3
		elif CURRENT_VERSION != self._connection.execute('PRAGMA user_version;').fetchone()[0]:
			# Table does not have the right `user_version` set.
			LOGGER.warning(f"Table does not have the right `user_version` set. (Determined version is v.{self.__version__} but user_version is v.{self._connection.execute('PRAGMA user_version;').fetchone()[0]})")
			return -4
		else:
			LOGGER.info(f"Database version is up to date! (v. {self.__version__})")
			return 0
	
	def rectifyDatabase(self, code : int):
		raise NotImplementedError("Not implemented in the base class.")

	@overload
	def get(self, *columnsToGet : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag,Literal["DESC","ASC"]]|list[tuple[ColumnFlag,Literal["DESC","ASC"]]]=[], nodeID : int=None, snpID : str=None, genomeID : int=None, position : int=None, ancestral : Literal["A","T","C","G"]=None, derived : Literal["A","T","C","G"]=None, snpReference : str=None, date : str=None, genome : str=None, strain : str=None, genbankID : str=None, refseqID : str=None, assembly : str=None, chromosome : str=None) -> Generator[tuple[Any],None,None]:
		pass

	@final
	def get(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag,Literal["DESC","ASC"]]|list[tuple[ColumnFlag,Literal["DESC","ASC"]]]=[], **where : Any) -> Generator[tuple[Any],None,None]:
		for row in self._connection.execute(*generateQuery(*select, orderBy=orderBy, **where)):
			yield row
	
	@property
	def SNPs(self) -> Generator[tuple[str,int,str,str],None,None]:
		return self.SNPTable.get(Columns.ALL)

	@property
	def references(self) -> Generator[tuple[int,str,str,str,str],None,None]:
		return self.ReferenceTable.get(Columns.ALL)

	@property
	def nodes(self) -> Generator[tuple[int,str],None,None]:
		return self.NodeTable.get(Columns.ALL)

	@cached_property
	def tree(self) -> Branch:
		"""{nodeID:[child1, child2, ...]}"""
		for (nodeID,) in self._connection.execute(f"SELECT {TREE_COLUMN_CHILD} FROM {TABLE_NAME_TREE} WHERE {TREE_COLUMN_PARENT} = ?;", [2]):
			if nodeID != 2:
				return Branch(self._connection, nodeID)

	@property
	def schemaHash(self):
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
	
	def __init__(self, database : sqlite3.Connection):
		super().__init__(database)

		code = self.validateDatabase()

		match code:
			case -2:
				print("Database is empty.")
			case -3:
				print(f"Database out of date/has wrong schema. Current version is v.{CURRENT_VERSION}, but this database has version v.{self.__version__} (schemaHash={self.schemaHash!r}).")
			case -4:
				print(f"Table does not have the right `user_version` set. (Determined version is v.{self.__version__} but user_version is v.{self._connection.execute('PRAGMA user_version;').fetchone()[0]})")
			case 0:
				print(f"Database is up to date! (v.{self.__version__})")
			case _:
				print(f"Unkown Database error. Current Database version is v.{CURRENT_VERSION}, and this database has version v.{self.__version__} (schemaHash={self.schemaHash!r}).")
				print("Exiting.")
				exit(1)
		
		if Globals.STRICT:
			self.rectifyDatabase(code)
	
	def rectifyDatabase(self, code : int):
		match code:
			case 0: # We're good
				pass
			case -2: # Table is new
				raise sqlite3.DatabaseError("Database is empty.")
			case -3: # Transfer data from old tables into new tables
				raise sqlite3.DatabaseError(f"Database version ({self.__version__}) does not match the currently set version. (user_version={self._connection.execute('PRAGMA user_version;').fetchone()[0]}, schemaHash={self.schemaHash!r})")
			case -4: # Version number missmatch
				raise sqlite3.DatabaseError(f"Table does not have the right `user_version` set. (Determined version is v.{self.__version__} but user_version is v.{self._connection.execute('PRAGMA user_version;').fetchone()[0]})")
			case _:
				raise sqlite3.DatabaseError(f"Unkown Database error. Current Database version is v.{CURRENT_VERSION}, and this database has version v.{self.__version__} (schemaHash={self.schemaHash!r}).")

class DatabaseWriter(Database):

	_mode = "w"
	
	def __init__(self, database : sqlite3.Connection):
		super().__init__(database)

		code = self.validateDatabase()
		match code:
			case -2:
				print("Database is empty.")
			case -3:
				print(f"Database out of date/has wrong schema. Current version is v.{CURRENT_VERSION}, but this database has version v.{self.__version__} (schemaHash={self.schemaHash!r}).")
			case -4:
				print(f"Table does not have the right `user_version` set. (Determined version is v.{self.__version__} but user_version is v.{self._connection.execute('PRAGMA user_version;').fetchone()[0]})")
			case 0:
				print(f"Database is up to date! (v.{self.__version__})")
			case _:
				print(f"Unkown Database error. Current Database version is v.{CURRENT_VERSION}, and this database has version v.{self.__version__} (schemaHash={self.schemaHash!r}).")
				print("Exiting.")
				exit(1)
		
		if code not in [0, -2]:
			while (userString := input("Rectify database? (Recommended to make a backup prior) [Y/N]: ").strip().lower()) not in ["y", "n"]:
				pass
			match userString:
				case "y":
					self.rectifyDatabase(code)
				case "n":
					pass
		elif code == -2:
			self.rectifyDatabase(-2)
		else:
			pass

	def rectifyDatabase(self, code : int):
		match code:
			case 0: # We're good
				pass
			case -2: # Table is new
				for table in self.Tables:
					table.create()
				self._connection.execute("PRAGMA user_version = ? ;", [self.__version__])
			case -3: # Transfer data from old tables into new tables
				for table in self.Tables:
					table.recreate()
				for (table,) in self._connection.execute("SELECT name FROM sqlite_master WHERE type='table';"):
					if table not in TABLES:
						self._connection.execute(f"DROP TABLE {table};")
				self._connection.execute("PRAGMA user_version = ? ;", [self.__version__])
			case -4:
				self._connection.execute("PRAGMA user_version = ? ;", [self.__version__])

	def addSNP(self, nodeID, position, ancestral, derived, reference, date, genomeID):
		self._connection.execute(f"INSERT (?,?,?,?,?,?,?) INTO {TABLE_NAME_SNP_ANNOTATION};", [nodeID, position, ancestral, derived, reference, date, genomeID])
	
	def addReference(self, genomeID, genome, strain, genbank, refseq, assemblyName):
		self._connection.execute(f"INSERT (?,?,?,?,?,?) INTO {TABLE_NAME_REFERENCES};", [genomeID, genome, strain, genbank, refseq, assemblyName])

	def addNode(self, nodeID, genoType):
		self._connection.execute(f"INSERT (?,?) INTO {TABLE_NAME_NODES};", [nodeID, genoType])

	def addBranch(self, parentID, childID, rank):
		self._connection.execute(f"INSERT (?,?,?) INTO {TABLE_NAME_TREE};", [parentID, childID, rank])

	def addRank(self, rankID, rankName):
		self._connection.execute(f"INSERT (?,?) INTO {TABLE_NAME_RANKS};", [rankID, rankName])

	def addGenome(self, genomeID, genomeName):
		self._connection.execute(f"INSERT (?,?) INTO {TABLE_NAME_GENOMES};", [genomeID, genomeName])
	
	def commit(self):
		self._connection.commit()

type Mode = Literal["r", "w"]

@overload
def openDatabase(database : str, mode : Literal["r"]) -> DatabaseReader:
	pass

@overload
def openDatabase(database : str, mode : Literal["w"]) -> DatabaseWriter:
	pass

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
			return DatabaseWriter(sqlite3.connect(database))
