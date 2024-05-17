
from sqlite3 import Connection
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from SQLOOP._core.Structures import *
from SQLOOP._core.Words import *
from SQLOOP._core.Aggregates import *

class Fetcher:
	"""Fetches data from a cursor. Consumes the cursor object during iteration/indexation."""
	query : Query
	_connection : sqlite3.Connection
	cols : int
	resultsLength : int

	def __new__(cls, connection : sqlite3.Connection, query : Query, *args, **kwargs):
		
		assert len(query.words) > 0, "Query must not be empty."
		obj = super().__new__(cls)
		obj.__init__(connection, query)
		if obj.singlet:
			return next(obj)
		return super().__new__(cls)

	def __init__(self, connection : sqlite3.Connection, query : Query):
		if getattr(self, "_connection", None) is connection:
			return
		self._connection = connection
		try:
			self._cursor = self._connection.execute(*query)
		except Exception as e:
			raise type(e)(f"Query: ({str(query)}, {query.params})\n"+e.args[0], *e.args[1:])
		self.query = query
		self.position = 0
	
	def __iter__(self):
		return self

	def __next__(self):
		if not self.position < self.resultsLength:
			raise StopIteration()
		match self.cols:
			case None:
				raise StopIteration()
			case 1:
				self.position += 1
				return next(self._cursor)[0]
			case _:
				self.position += 1
				return next(self._cursor)
	
	def __getitem__(self, rowNumber) -> Any|tuple[Any]:
		if not rowNumber < self.resultsLength:
			q = iter(self.query)
			raise ResultsShorterThanLookup(f"When looking for row number {rowNumber} in query: '{next(q)}', {next(q)}")
		elif self.cols is None:
			q = iter(self.query)
			raise NoResultsFromQuery(f"When looking for row number {rowNumber} in query: '{next(q)}', {next(q)}")
		
		if self.cols == 1:
			return self._connection.execute(*self.query - LIMIT(rowNumber,rowNumber)).fetchone()[0]
		else:
			return self._connection.execute(*self.query - LIMIT(rowNumber,rowNumber)).fetchone()
	
	def __len__(self):
		return self.resultsLength

	@cached_property
	def cols(self):
		return self.query.cols
	
	@cached_property
	def singlet(self):
		if isinstance(self.query.words[0], SELECT):
			if all(map(lambda x:isinstance(x, Aggregate), self.query.words[0].content)):
				return True
		elif self.query.words[0] is SELECT:
			if all(map(lambda x:isinstance(x, Aggregate), itertools.islice(self.query.words, 1, self.cols+1))):
				return True
		return False

	@cached_property
	def resultsLength(self):
		if self.singlet:
			return 1
		if isinstance(self.query.words[0], SELECT):
			return self._connection.execute(*Query(SELECT(COUNT(ALL)), *self.query.words[1:])).fetchone()[0]
		elif self.query.words[0] is SELECT:
			return self._connection.execute(*Query(SELECT(COUNT(ALL)), *itertools.dropwhile(lambda x:not isRelated(x, Word) or isinstance(x, Aggregate), itertools.islice(self.query.words, 1, None)))).fetchone()[0]
		else:
			return 0


class Selector:
	columns : tuple[Column]
	tables : tuple[Table]
	joinColumns : dict[tuple[Table,Table],Column]
	wheres : tuple[Comparison]

	appended : Query

	database : "Database"
	databaseTables : set[Table]
	databaseColumns : set[Column]

	def __init__(self, database : "Database"):
		self.database = database
		self.databaseTables = set(database.tables)
		self.databaseColumns = set(database.columns)
		self.appended = Query()
		self.wheres = tuple()
	
	def __iter__(self):
		from pprint import pprint
		return Fetcher(self.database._connection, SELECT (*self.columns) - FROM (*self.tables) - WHERE (*self.wheres) - self.appended)

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
		if not isinstance(items, tuple):
			items = (items,)
		if len(items) == 0 and items[0] is ALL:
			self.columns = SQLDict(items)
			if getattr(self, "tables", None) is None:
				self.tables = SQLDict()
		elif isRelated(items[0], Column):
			assert len(set(filter(lambda x:x is not ALL, items)).difference(self.databaseColumns)) == 0, f"Given columns don't exist in the database: {set(items).difference(self.databaseColumns)}"
			from SQLOOP._core.Functions import getSmallestFootprint
			self.columns = SQLDict(items)
			self.tables = SQLDict(getSmallestFootprint(set(filter(lambda x:x is not ALL, items)), self.databaseTables))
		elif isRelated(items[0], Table):
			assert len(set(items).difference(self.databaseTables)) == 0, f"Given tables don't exist in the database: {set(items).difference(self.databaseTables)}"
			if not hasattr(self, "columns"):
				self.columns = SQLDict(ALL, )
			self.tables = SQLDict(items)
			self.wheres = self.wheres
		elif isinstance(items[0], Comparison):
			assert all(col in self.databaseColumns for comp in items for col in [comp.left, comp.right] if isRelated(col, Column)), f"Columns in comparisons are not found in the database: {set(col for comp in items for col in [comp.left, comp.right] if isRelated(col, Column) and col not in self.databaseColumns)}"
			from SQLOOP._core.Functions import createSubqueries

			local, distant = [], []
			for comp in items:
				if not isRelated(comp.left, Column) or comp.left is ALL or any(comp.left in table for table in self.tables):
					local.append(comp)
				else:
					distant.append(comp)
			self.wheres = (*self.wheres, *local, *createSubqueries(self.tables, self.database.tables, distant))
		else:
			raise NoMatchingDefinition(self.__qualname__+".__getitem__", items)
		return self
DEBUG = False
class Database:
	"""Usage:
	```python
	database = Database("database.db", "w")

	database.valid
	# Is True if all assertions for a good database holds

	database.fix()
	# Will attempt to fix all problems which cause assertions to not hold

	database.exception
	# Is the exception that the first broken assertion wants to raise.
	# Use is:
	# raise database.exception

	class MyTable(Table, name="my_table"):
		class ID(Column, name="id"): pass
		class Name(Column, name="name"): pass
		class Value(Column, name="value"): pass
	
	ID = MyTable.ID
	Name = MyTable.Name
	Value = MyTable.Value
	
	database[Value][MyTable][Name == "Fredrik"]
	# Would return a generator which yields each entry value in table "my_table" from column "Value" where that entry's "Name" column has the value "Fredrik"
	```
	"""

	DATABASE_VERSIONS : dict[str,int] = {}
	CURRENT_VERSION = 0
	CURRENT_TABLES_HASH : int
	CURRENT_INDEXES_HASH : int
	
	@ClassProperty
	def CURRENT_TABLES_HASH(self) -> int:
		return hash(whitespacePattern.sub(" ", "; ".join(sorted(map(sql, self.tables)))))
	@ClassProperty
	def CURRENT_INDEXES_HASH(self) -> int:
		return hash(whitespacePattern.sub(" ", "; ".join(sorted(map(sql, self.indexes)))))

	LOG = logging.Logger(SOFTWARE_NAME, level=logging.FATAL)

	_connection : sqlite3.Connection
	mode : str
	filename : str
	columns : SQLDict[Column] = SQLDict()
	"""A hybrid between a tuple and a dict. Can be indexed using number in column order or by column
	in-sql name (Column.__sql_name__). When iterated, returns dict.values() instead of the usual dict.keys()."""
	tables : SQLDict[Table] = SQLDict()
	"""A hybrid between a tuple and a dict. Can be indexed using number in table order or by table
	in-sql name (Table.__sql_name__). When iterated, returns dict.values() instead of the usual dict.keys()."""
	indexes : SQLDict[Index] = SQLDict()
	"""A hybrid between a tuple and a dict. Can be indexed using number in index order or by index
	in-sql name (Index.__sql_name__). When iterated, returns dict.values() instead of the usual dict.keys()."""
	assertions : list[Assertion] = Globals.ASSERTIONS
	"""A look-up list of assertions and the exceptions to be raised should the assertion fail. Assertions are checked last to first."""

	def __init__(self, filename : str, mode : Mode):

		if type(self) is Database:
			raise NotImplementedError("The base Database class is not to be used, please subclass `Database` with your own appropriate tables and indexes.")
		self.mode = mode
		self.filename = filename if os.path.isabs(filename) or filename == ":memory:" else os.path.realpath(os.path.expanduser(filename))
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
	
	def __init_subclass__(cls, *, baseAssertions : tuple=(), **kwargs):
		super().__init_subclass__(**kwargs)
		if baseAssertions:
			cls.assertions = cls.assertions + baseAssertions
		cls.columns = SQLDict()
		cls.tables = SQLDict()
		cls.indexes = SQLDict()
		for item in map(lambda name:getattr(cls, name), tuple(dir(cls))):
			if isRelated(item, Table):
				cls.tables.append(item)
			elif isRelated(item, Index):
				cls.indexes.append(item)
		
		for table in cls.tables:
			for column in table.columns:
				if column not in cls.columns:
					cls.columns.append(column)
		

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
		string = [f"<{self.__class__.__name__} at {hex(id(self))} version={self.__version__} tablesHash={self.tablesHash:X}>"]
		for table in self.tables:
			string.append(
				f"\t<Table.{table.__name__} rows={self(SELECT - COUNT(ALL) - FROM(table)) if self(SELECT - COUNT(ALL) - FROM - SQLITE_MASTER - WHERE (type='table', name=str(table))) == 1 else 'N/A'} columns={table.columns}>")
		string.append(f"</{self.__class__.__name__}>")
		return "\n".join(string)

	@overload
	def __call__(self, query : Query|Word|type[Word]) -> Generator[tuple[Any],None,None]|Any|None: ...
	@overload
	def __call__(self, query : str, params : list[Any]) -> Generator[tuple[Any],None,None]|Any|None: ...
	@final
	def __call__(self, query : str|Query|Word|type[Word], params : list[Any]=[]) -> Generator[tuple[Any],None,None]|Any|None:
		if isinstance(query, str):
			if query.strip().lower().startswith("select"):
				return (row for row in self._connection.execute(query, params))
			else:
				self._connection.execute(query, params)
				return None
		elif isinstance(query, Query):
			if isinstance(query.words[0] , SELECT) or query.words[0] is SELECT:
				return Fetcher(self._connection, query)
			else:
				self._connection.execute(*query)
				return None
		elif isinstance(query, (Word, type(Word))):
			self._connection.execute(*Query(query))
			return None
		else:
			raise ValueError(f"Trying to call database with seomthing other than a 'str' or a 'Query' object.\ndatabase({query}, {params})")
			

	def __getitem__(self, tuple):
		out = Selector(self)
		return out[tuple]

	@property
	def __version__(self):
		return int(self._connection.execute("PRAGMA user_version;").fetchone()[0])

	@property
	def columns(self):
		return set().union(map(column for table in self.tables for column in table.columns))

	@property
	def valid(self):
		"""Check if all of the class-specific assertions hold."""
		return all(map(*this.condition(self), self.assertions))

	def fix(self):
		"""Apply the pre-defined fix for all class-specified assertions."""
		for assertion in self.assertions:
			if not assertion.condition(self):
				assertion.rectify(self)

	@property
	def exception(self):
		"""Return the exception object that represents the problem that breaks the first found assertion."""
		for assertion in filter(*this.condition(database=self), self.assertions):
			return assertion.exception(self)

	@property
	def indexesHash(self) -> int:
		"""md5 hash of the original SQL text that created all indexes in the database."""
		return hash(whitespacePattern.sub(" ", "; ".join(sorted(map(lambda s:tableCreationCommand.sub("",s), self(SELECT - SQL - FROM - SQLITE_MASTER - WHERE (type='index')))))))

	@property
	def tablesHash(self) -> int:
		"""md5 hash of the original SQL text that created all tables in the database."""
		return hash(whitespacePattern.sub(" ", "; ".join(sorted(map(lambda s:tableCreationCommand.sub("",s), self(SELECT - SQL - FROM - SQLITE_MASTER - WHERE (type='table')))))))

	def createIndex(self : Self, index : Index) -> bool:
		"""Create a given Index-object inside the database. Returns True if succesfull, returns False otherwise."""
		try:
			self(CREATE - INDEX - sql(index))
			return True
		except:
			return False

	def clearIndexes(self) -> bool:
		"""Drops all indexes from the database. Returns True if successfull, returns False if unsuccesfull."""
		self(BEGIN - TRANSACTION)
		try:
			for (indexName,) in self(SELECT("name") - FROM(SQLITE_MASTER) - WHERE(type = "index")):
				self(DROP - INDEX - IF - EXISTS(indexName))
			self(COMMIT)
			return True
		except:
			self(ROLLBACK)
			return False

	def commit(self):
		self._connection.commit()

	def close(self):
		try:
			self._connection.close()
		except:
			pass
