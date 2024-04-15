
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
	mode : str
	filename : str
	tables : set[Table]
	columns : set[Column]
	indexes : set[Index]
	assertions : list[Assertion] = baseAssertions
	"""A look-up list of assertions and the exceptions to be raised should the assertion fail. Assertions are checked last to first."""

	def __init__(self, filename : str, mode : Mode):
		self.mode = mode
		self.filename = filename if os.path.isabs(filename) else os.path.realpath(os.path.expanduser(filename))
		match mode:
			case "w":
				self._connection = sqlite3.connect(filename)
			case "r":
				if not os.path.exists(filename):
					raise FileNotFoundError(f"Database file {filename} not found on the system.")
				
				# Convert to URI acceptable filename
				cDatabase = "/".join(filter(lambda s : s != "", filename.replace('?', '%3f').replace('#', '%23').split(os.path.sep)))
				if not cDatabase.startswith("/"): # Path has to be absolute already, and windows paths need a prepended '/'
					cDatabase = "/"+cDatabase
				
				self._connection = sqlite3.connect(cDatabase)
			case _:
				raise ValueError(f"{mode!r} is not a recognized file-stream mode. Only 'w'/'r' allowed.")
	
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
		string = [f"<{self.__class__.__name__} at {hex(id(self))} version={self.__version__} tablesHash={self.tablesHash!r}>"]
		for table in self.tables:
			string.append(f"\t<Table.{table.__name__} rows={self(SELECT - COUNT(ALL) - FROM(table))} columns={table.columns}>")
		string.append(f"</{self.__class__.__name__}>")
		return "\n".join(string)

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

	@property
	def columns(self):
		return set().union(map(column for table in self.tables for column in table.columns))

	def setTables(self, tables : tuple[Table]):
		self.tables = set(tables)

	def setIndexes(self, indexes : tuple[Index]):
		self.indexes = set(indexes)

	def checkDatabase(self):
		"""Will raise appropriate exceptions when in read-mode. Will attempt to fix the database if in write mode."""
		for assertion in self.assertions:
			if assertion.condition(database=self):
				if self.mode == "r": # Raise exception
					assertion.exception(self)
				elif self.mode == "w": # Try to rectify
					assertion.rectify(self)


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

	def close(self):
		try:
			self._connection.close()
		except:
			pass
