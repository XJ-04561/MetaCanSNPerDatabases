
from sqlite3 import Connection
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
from MetaCanSNPerDatabases.Exceptions import *
from MetaCanSNPerDatabases.core.Structures import *
from MetaCanSNPerDatabases.core.Aggregates import *
from MetaCanSNPerDatabases.core.Words import *
from MetaCanSNPerDatabases.core.Tree import Branch

if False:
	from MetaCanSNPerDatabases.core.Columns import *
	from MetaCanSNPerDatabases.core.Tables import *
	from MetaCanSNPerDatabases.core.Indexes import *


class Selector:
	columns : tuple[Column]
	tables : tuple[Table]
	joinColumns : dict[tuple[Table,Table],Column]
	wheres : tuple[Comparison]

	appended : Query

	_connection : Connection
	databaseTables : set[Table]
	def __init__(self, connection : Connection, tables : tuple[Table]):
		self._connection = connection
		self.databaseTables = set(tables)
		self.appended = Query()
	
	def __iter__(self):
		for row in self._connection.execute(*SELECT (*self.columns) - FROM (*self.tables) - WHERE(*self.wheres) - self.appended):
			yield row
		return

	def __sub__(self, append : Word|Query):
		self.appended -= append

	@Overload
	def __getitem__(self, columns : tuple[Column]):
		from MetaCanSNPerDatabases.core.Functions import getSmallestFootprint
		self.tables = tuple(getSmallestFootprint(set(columns), [[set(table.columns), table, 0] for table in self.databaseTables]))
		return self
	
	@__getitem__.add
	def __getitem__(self, tables : tuple[Table]):
		if not hasattr(self, "columns"):
			self.columns = (ALL, )
		self.tables = tables
		return self
	
	@__getitem__.add
	def __getitem__(self, comparisons : tuple[Comparison]):
		from MetaCanSNPerDatabases.core.Functions import getShortestPath
		wheres = []
		for comp in comparisons:
			if isinstance(comp.left, Column) and all(comp.left not in table for table in self.tables):
				targetTables = []
				for table in self.databaseTables:
					if comp.left in table.columns:
						targetTables.append(table)
				
				path = getShortestPath(self.tables, targetTables)
				subQuery = SELECT (path[-1][0]) - FROM (path[-1][1]) - WHERE(comp)
				for col, table in path[-2::-1]:
					subQuery = SELECT (col) - FROM (table) - WHERE(comp.left in subQuery)
				wheres.append(path[0][0] in subQuery)
			else:
				wheres.append(comp)
		self.wheres = tuple(wheres)
		return self

class Fetcher:
	"""Fetches data from a cursor. Consumes the cursor object during iteration/indexation."""
	_cursor : sqlite3.Cursor
	def __init__(self, cursor):
		self._cursor = cursor
	
	def __iter__(self):
		for row in self._cursor:
			yield row
	
	def __getitem__(self, rowNumber) -> Any|tuple[Any]:
		for i in range(rowNumber):
			next(self._cursor)
		if len(row := next(self._cursor)) == 1:
			return row[0]
		else:
			return row

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
	
	def __del__(self):
		try:
			self._connection.close()
		except:
			pass
	
	def __repr__(self):
		return object.__repr__(self)[:-1] + f" version={self.__version__} tablesHash={self.tablesHash!r} tables={[(name, self(SELECT(COUNT(ALL)) - FROM(table))) for name, table in self.Tables.items()]}>"

	@Overload
	def __call__(self, query : Query) -> Generator[tuple[Any],None,None]:
		return Fetcher(self._connection.execute(*query))
	
	@__call__.add
	def __call__(self, query : str, params : list[Any]) -> Generator[tuple[Any],None,None]:
		for row in self._connection.execute(query, params):
			yield row
	
	def __getitem__(self, tuple):
		out = Selector(self._connection, list(self.Tables.values()))
		return out[tuple]
	
	@property
	def __version__(self):
		return int(self._connection.execute("PRAGMA user_version;").fetchone()[0])

	def setTables(self, tables : tuple[Table]):
		self.tables = set(tables)

	def setIndexes(self, indexes : tuple[Index]):
		self.indexes = set(indexes)

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

	@property
	def indexesHash(self):
		from MetaCanSNPerDatabases.core.Functions import whitespacePattern
		return hashlib.md5(
			whitespacePattern.sub(
				" ",
				"; ".join([
					x[0]
					for x in self._connection.execute(f"SELECT sql FROM sqlite_schema WHERE type='index' ORDER BY sql DESC;")
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
				self(BEGIN - TRANSACTION)
				for table in self.tables:
					self(CREATE - TABLE - sql(table))
				self(PRAGMA (user_version = CURRENT_VERSION))
				self(COMMIT)
			case -3: # Legacy CanSNPer table
				updateFromLegacy(self, refDir=refDir)
			case -4:
				self(BEGIN - TRANSACTION)
				self(PRAGMA (user_version = CURRENT_VERSION))
				self(COMMIT)
			case -5: # Transfer data from old tables into new tables
				self(BEGIN - TRANSACTION)
				self.clearIndexes()
				for table in self.tables:
					self(ALTER - TABLE - table - RENAME - TO - f"{table}2")
					self(CREATE - TABLE - sql(table))
					self(INSERT - INTO - table - (SELECT (ALL) - FROM(f"{table}2") ))
					self(DROP - TABLE - f"{table}2")
				for index in self.indexes:
					self(CREATE - INDEX - sql(index))
				self(COMMIT)
				self(BEGIN - TRANSACTION)
				for (table,) in self._connection.execute("SELECT name FROM sqlite_master WHERE type='table';"):
					if not any(table == validTable.name for validTable in self.tables):
						self(DROP - TABLE - table)
				self(PRAGMA (user_version = CURRENT_VERSION))
				self(COMMIT)
			case -6:
				self(BEGIN - TRANSACTION)
				self.clearIndexes()
				for index in self.indexes:
					self(CREATE - INDEX - sql(index))
				self(COMMIT)

	@Overload
	def createIndex(self : Self, index : Index) -> bool:
		"""Create a given Index-object inside the database"""
		from MetaCanSNPerDatabases.core.Columns import ALL
		from MetaCanSNPerDatabases.core.Words import CREATE, INDEX
		try:
			self(CREATE - INDEX - sql(index))
			return True
		except:
			return False

	@createIndex.add
	def createIndex(self : Self, table : Table, *cols : Column, name : str=None) -> bool:
		if name is None:
			name = f"{table}By{''.join(map(str.capitalize, map(str, cols)))}"
		from MetaCanSNPerDatabases.core.Columns import ALL
		from MetaCanSNPerDatabases.core.Words import CREATE, INDEX
		try:
			self(CREATE - INDEX - Index(name, table, cols))
			return True
		except:
			return False

	def clearIndexes(self):
		for (indexName,) in self(SELECT("name") - FROM(SQLITE_MASTER) - WHERE(type = "index")):
			self(DROP - INDEX - IF - EXISTS(indexName))

	def commit(self):
		self._connection.commit()

@Overload
def openDatabase(database : str, mode : ReadMode) -> DatabaseReader:
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

@openDatabase.add
def openDatabase(database : str, mode : WriteMode) -> DatabaseWriter:
	if os.path.exists(database):
		return DatabaseWriter(sqlite3.connect(database))
	else:
		ret = DatabaseWriter(sqlite3.connect(database))
		ret.rectifyDatabase(-2, copy=False)
		return ret
