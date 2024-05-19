
from sqlite3 import Connection
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from SQLOOP._core.Structures import *
from SQLOOP._core.Schema import *
from SQLOOP._core.Types import *
from SQLOOP._core.Expressions import *
from SQLOOP._core.Words import *
from SQLOOP._core.Aggregates import *

class Fetcher:
	"""Fetches data from a cursor. Consumes the cursor object during iteration/indexation."""
	query : SelectStatement
	_connection : sqlite3.Connection
	cols : int
	resultsLength : int

	def __new__(cls, connection : sqlite3.Connection, query : SelectStatement, *args, **kwargs):
		
		assert isinstance(query, SelectStatement), "Fetcher must be given a SelectStatement."
		
		if query.singlet:
			try:
				return (query @ connection).fetchone()
			except StopIteration:
				return None
		return super().__new__(cls)

	def __init__(self, connection : sqlite3.Connection, query : SelectStatement):

		self._connection = connection
		try:
			self._cursor = query @ self._connection
		except Exception as e:
			raise type(e)(f"Query: ({str(query)}, {query.params})\n"+e.args[0], *e.args[1:])
		self.query = query
	
	def __iter__(self):
		return self

	def __next__(self):
		match self.query.columns:
			case None:
				raise StopIteration()
			case 1:
				return next(self._cursor)[0]
			case _:
				return next(self._cursor)
	
	def __getitem__(self, rowNumber) -> Any|tuple[Any]:
		if self.query.columns == 1:
			return (((self.query - LIMIT(rowNumber,rowNumber)) @ self._connection).fetchone() or [None])[0]
		else:
			return ((self.query - LIMIT(rowNumber,rowNumber)) @ self._connection).fetchone()

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
			raise TypeError(f"{items}")

		return self

class DatabaseMeta(type):
	
	columns : SQLDict[Column] = SQLDict()
	"""A hybrid between a tuple and a dict. Can be indexed using number in column order or by column
	in-sql name (Column.__sql_name__). When iterated, returns dict.values() instead of the usual dict.keys()."""
	tables : SQLDict[Table] = SQLDict()
	"""A hybrid between a tuple and a dict. Can be indexed using number in table order or by table
	in-sql name (Table.__sql_name__). When iterated, returns dict.values() instead of the usual dict.keys()."""
	indexes : SQLDict[Index] = SQLDict()
	"""A hybrid between a tuple and a dict. Can be indexed using number in index order or by index
	in-sql name (Index.__sql_name__). When iterated, returns dict.values() instead of the usual dict.keys()."""

	def __contains__(self, other):
		
		if isinstance(other, SQLOOP):
			if isThing(other, Column):
				return other in self.columns
			elif isThing(other, Table):
				return other in self.tables
			elif isThing(other, Index):
				return other in self.indexes
		return False
	
	def __repr__(self):
		return f"{object.__repr__(self)}\n\t<columns>\n\t\t{'\n\t\t'.join(map(repr, self.columns))}\n\t</columns>\n\t<tables>\n\t\t{'\n\t\t'.join(map(repr, self.tables))}\n\t</tables>\n\t<indexes>\n\t\t{'\n\t\t'.join(map(repr, self.indexes))}\n\t</indexes>\n</{self.__name__}>"

class Database(metaclass=DatabaseMeta):
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
	
	def __init_subclass__(cls, *, assertions : tuple=(), **kwargs):
		super().__init_subclass__(**kwargs)
		if assertions:
			cls.assertions = cls.assertions + assertions
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
		elif isinstance(query, SQLOOP):
			if isinstance(query, SelectStatement):
				return Fetcher(self._connection, query)
			else:
				query @ self._connection
				return None
		else:
			raise ValueError(f"Trying to call database with seomthing other than a 'str' or a 'Query' object.\ndatabase({query}, {params})")
	
	def __contains__(self, other):
		if isinstance(other, SQLOOP):
			if isThing(other, Column):
				return other in self.columns
			elif isThing(other, Table):
				return other in self.tables
			elif isThing(other, Index):
				return other in self.indexes
		return False

	def __getitem__(self, items):
		out = Selector(self)
		return out[items]

	@property
	def __version__(self):
		return int(self._connection.execute("PRAGMA user_version;").fetchone()[0])

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
