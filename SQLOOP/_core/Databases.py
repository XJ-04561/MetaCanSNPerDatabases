
from sqlite3 import Connection
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from SQLOOP.core import *
from SQLOOP._core.Structures import *

class Fetcher:
	"""Fetches data from a cursor. Consumes the cursor object during iteration/indexation."""
	query : Query
	_connection : sqlite3.Connection
	cols : int
	resultsLength : int

	def __init__(self, connection : sqlite3.Connection, query : Query):
		self._connection = connection
		self._cursor = self._connection(*query)
		self.query = query
		self.position = 0
	
	def __next__(self):
		match self.cols:
			case None:
				raise StopIteration()
			case 1:
				return next(self._cursor)[0]
			case _:
				return next(self._cursor)
	
	def __getitem__(self, rowNumber) -> Any|tuple[Any]:
		if not rowNumber < self.resultsLength:
			q = iter(self.query)
			raise ResultsShorterThanLookup(f"When looking for row number {rowNumber} in query: '{next(q)}', {next(q)}")
		elif self.cols is None:
			q = iter(self.query)
			raise NoResultsFromQuery(f"When looking for row number {rowNumber} in query: '{next(q)}', {next(q)}")
		
		if self.cols == 1:
			return self._connection(self.query - LIMIT(rowNumber,rowNumber))[0]
		else:
			return self._connection(self.query - LIMIT(rowNumber,rowNumber))
	
	@cached_property
	def cols(self):
		if not isinstance(self.query.words[0], SELECT):
			return None
		return len(self.query.words[0].content)
	
	@cached_property
	def resultsLength(self):
		return self._connection(Query(SELECT(COUNT(ALL)), self.query.words[1:])).fetchone()[0]


class Selector:
	columns : tuple[Column]
	tables : tuple[Table]
	joinColumns : dict[tuple[Table,Table],Column]
	wheres : tuple[Comparison]

	appended : Query

	_connection : Connection
	databaseTables : set[Table]
	databaseColumns : set[Column]

	def __init__(self, connection : Connection, tables : tuple[Table]):
		self._connection = connection
		self.databaseTables = set(tables)
		self.databaseColumns = set().union(map(*this.columns, self.databaseTables))
		self.appended = Query()
		self.wheres = tuple()
	
	def __iter__(self):
		return Fetcher(self._connection, SELECT (*self.columns) - FROM (*self.tables) - WHERE (*self.wheres) - self.appended)

	def __sub__(self, append : Word|Query):
		self.appended -= append

	@overload
	def __getitem__(self, columns : tuple[Column]): ...
	@overload
	def __getitem__(self, tables : tuple[Table]): ...
	@overload
	def __getitem__(self, comparisons : tuple[Comparison]): ...
	@final
	def __getitem__(self, items : tuple[Column|Table|Comparison]):
		if isinstance(items[0], Column):
			assert len(set(items).difference(self.databaseColumns)) == 0, f"Given columns don't exist in the database: {set(items).difference(self.databaseColumns)}"
			self.columns = items
			self.tables = tuple(getSmallestFootprint(set(items), [[set(table.columns), table, 0] for table in self.databaseTables]))
		elif isinstance(items[0], Table):
			assert len(set(items).difference(self.databaseTables)) == 0, f"Given tables don't exist in the database: {set(items).difference(self.databaseTables)}"
			if not hasattr(self, "columns"):
				self.columns = (ALL, )
			self.tables = items
			self.wheres = self.wheres + tuple()
		elif isinstance(items[0], Comparison):
			assert all(col in self.databaseColumns for comp in items for col in [comp.left, comp.right] if isinstance(col, Column)), f"Columns in comparisons are not found in the database: {set(col for comp in items for col in [comp.left, comp.right] if isinstance(col, Column) and col not in self.databaseColumns)}"
			wheres = list(self.wheres)
			for comp in items:
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
		else:
			raise NoMatchingDefinition(self.__qualname__+".__getitem__", items)
		return self

class Database:
	"""Usage:
	```python
	database = Database("database.db", "w")

	database.checkDatabase() # Will try to fix issues with the database

	otherDatabase = Database("otherDatabase.db", "r")

	otherDatabase.checkDatabase() # Will raise exception if there are issues with the database
	```
	"""

	LOG = logging.Logger(SOFTWARE_NAME, level=logging.FATAL)

	_connection : sqlite3.Connection
	mode : str
	filename : str
	tables : set[Table]
	columns : set[Column]
	indexes : set[Index]
	assertions : list[Assertion] = Globals.ASSERTIONS
	"""A look-up list of assertions and the exceptions to be raised should the assertion fail. Assertions are checked last to first."""

	DATABASE_VERSIONS : dict[str,int] = {}

	CURRENT_VERSION = 0
	CURRENT_TABLES_HASH = NoHash()
	CURRENT_INDEXES_HASH = NoHash()

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
				cDatabase = "/".join(filter(*this != "", filename.replace('?', '%3f').replace('#', '%23').split(os.path.sep)))
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

	@overload
	def __call__(self, query : Query) -> Generator[tuple[Any],None,None]: ...
	@overload
	def __call__(self, query : str, params : list[Any]) -> Generator[tuple[Any],None,None]: ...
	@final
	def __call__(self, query : str|Query, params : list[Any]=None) -> Generator[tuple[Any],None,None]:
		if isinstance(query, str) and isinstance(params, list[Any]):
			for row in self._connection.execute(query, params):
				yield row
		elif isinstance(query, Query) and params is None:
			if isinstance(query.words[0], SELECT):
				if len(query.words[0].content) == 1:
					for row in self._connection.execute(*query):
						yield row[0]
				else:
					for row in self._connection.execute(*query):
						yield row
			else:
				self._connection.execute(*query)
				return None


	def __getitem__(self, tuple):
		out = Selector(self._connection, list(self.Tables.values()))
		return out[tuple]

	@property
	def __version__(self):
		return int(self._connection.execute("PRAGMA user_version;").fetchone()[0])

	@property
	def columns(self):
		return set().union(map(column for table in self.tables for column in table.columns))

	@cached_property
	def valid(self):
		
		return not any(map(*this.condition(database=self), self.assertions))

	def fix(self):

		for assertion in filter(*this.condition(database=self), self.assertions):
			assertion.rectify(self)
		del self.valid

	@property
	def exception(self):

		for assertion in filter(*this.condition(database=self), self.assertions):
			return assertion.exception(self)

	@property
	def indexesHash(self):
		return hashQuery(self, SELECT("sql") - FROM - SQLITE_MASTER - WHERE(type='index') - ORDER - BY("sql DESC"))

	@property
	def tablesHash(self):
		return hashQuery(self, SELECT("sql") - FROM - SQLITE_MASTER - WHERE(type='table') - ORDER - BY("sql DESC"))

	def createIndex(self : Self, index : Index) -> bool:
		"""Create a given Index-object inside the database"""
		try:
			self(CREATE - INDEX - sql(index))
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
