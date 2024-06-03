
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from SQLOOP._core.Structures import *
from SQLOOP._core.Schema import *
from SQLOOP._core.Types import *
from SQLOOP._core.Expressions import *
from SQLOOP._core.Words import *
from SQLOOP._core.Aggregates import *
from SQLOOP._core.ThreadConnection import ThreadConnection

class Fetcher:
	"""Fetches data from a cursor. Consumes the cursor object during iteration/indexation."""

	LOG = Globals.LOGGER.getChild("Fetcher")

	query : SelectStatement
	_connection : Connection
	cols : int
	resultsLength : int

	def __new__(cls, connection : Connection, query : SelectStatement, *args, **kwargs):
		
		assert isinstance(query, SelectStatement), "Fetcher must be given a SelectStatement."

		if query.singlet:
			try:
				res = query @ connection
				ret = (res.fetchone() or [None])[0] if query.cols == 1 else res.fetchone()
				cls.LOG.debug(f"Got {ret!r} from: {str(query)!r}, {query.params}")
				return ret
			except sqlite3.Error as e:
				cls.LOG.debug(f"Got [{type(e).__name__}: {e}] from: {str(query)!r}, {query.params}")
				raise e
			except StopIteration:
				cls.LOG.debug(f"Got None from: {str(query)!r}, {query.params}")
				return None
		cls.LOG.debug(f"Created Fetcher from: {str(query)!r}, {query.params}")
		return super().__new__(cls)

	def __init__(self, connection : Connection, query : SelectStatement):

		self._connection = connection
		try:
			self._cursor = query @ self._connection
		except Exception as e:
			raise type(e)(f"Query: ({str(query)}, {query.params})\n"+e.args[0], *e.args[1:])
		self.query = query
	
	def __repr__(self):
		return f"{object.__repr__(self)[:-1]} query={str(self.query)} params={self.query.params}>"

	def __iter__(self):
		if self.query.cols == 1:
			for (entry,) in self._cursor:
				yield entry
		else:
			for entry in self._cursor:
				yield entry

	def __next__(self):
		match self.query.cols:
			case None:
				raise StopIteration()
			case 1:
				return next(self._cursor)[0]
			case _:
				return next(self._cursor)
	
	# def __getitem__(self, rowNumber) -> Any|tuple[Any]:
	# 	if self.query.cols == 1:
	# 		return (((self.query - LIMIT(rowNumber+1,rowNumber+1)) @ self._connection).fetchone() or [None])[0]
	# 	else:
	# 		return ((self.query - LIMIT(rowNumber+1,rowNumber+1)) @ self._connection).fetchone()
	
	def __len__(self):
		return (SelectStatement(SELECT(COUNT(ALL)), *self.query.words[1:]) @ self._connection).fetchone()[0]

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
		ntt = "\n\t\t"
		return f"{object.__repr__(self)}\n\t<columns>\n\t\t{ntt.join(map(repr, self.columns))}\n\t</columns>\n\t<tables>\n\t\t{ntt.join(map(repr, self.tables))}\n\t</tables>\n\t<indexes>\n\t\t{ntt.join(map(repr, self.indexes))}\n\t</indexes>\n</{self.__name__}>"

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
		return int(hashlib.md5(" ".join("; ".join(sorted(map(lambda x:sql(x).strip(), self.tables))).strip().split()).encode("utf-8")).hexdigest(), base=16)
	@ClassProperty
	def CURRENT_INDEXES_HASH(self) -> int:
		return int(hashlib.md5(" ".join("; ".join(sorted(map(lambda x:sql(x).strip(), self.indexes))).strip().split()).encode("utf-8")).hexdigest(), base=16)

	LOG = logging.Logger(SOFTWARE_NAME, level=logging.FATAL)

	factory : type = Connection
	factoryFunc : Callable = sqlite3.connect
	_connection : ThreadConnection
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
	
	@overload
	def __init__(self, filename : str, mode : Mode, factory : type=Connection): ...
	def __init__(self, filename : str, mode : Mode, factory : type=None):

		if type(self) is Database:
			raise NotImplementedError("The base Database class is not to be used, please subclass `Database` with your own appropriate tables and indexes.")
		self.mode = mode
		self.filename = filename if os.path.isabs(filename) or filename == ":memory:" else os.path.realpath(os.path.expanduser(filename))
		if factory:
			self.factory = factory
		match mode:
			case "w":
				self._connection = ThreadConnection(filename, factory=self.factory, identifier=id(self))
			case "r":
				if not os.path.exists(filename):
					raise FileNotFoundError(f"Database file {filename} not found on the system.")
				
				# Convert to URI acceptable filename
				cDatabase = "/".join(filter(*this != "", filename.replace('?', '%3f').replace('#', '%23').split(os.path.sep)))
				if not cDatabase.startswith("/"): # Path has to be absolute already, and windows paths need a prepended '/'
					cDatabase = "/"+cDatabase
				
				self._connection = ThreadConnection(filename, factory=self.factory, identifier=id(self))
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
			self.close()
		except:
			pass
	
	def __repr__(self):
		string = [f"<{self.__class__.__name__} at {hex(id(self))} version={self.__version__} tablesHash={self.tablesHash:X}>"]
		for table in self.tables:
			try:
				string.append(
					f"\t<Table.{table.__name__} rows={self(SELECT (COUNT(ALL)) - FROM(table)) if self(SELECT (COUNT(ALL)) - FROM (SQLITE_MASTER) - WHERE (type='table', name=str(table))) == 1 else 'N/A'} columns={table.columns}>")
			except Exception as e:
				self.LOG.exception(e)
				string.append(f"\t<Table.{table.__name__} rows=N/A columns=N/A>")
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

	def __getitem__(self, items : tuple[Column|Table|Comparison]):
		if not type(items) is tuple:
			items = (items, )
		from SQLOOP._core.Functions import getSmallestFootprint, createSubqueries, recursiveWalk
		columns = tuple(filter(lambda x:isRelated(x, Column) or isinstance(x, Aggregate) or isinstance(x, Operation), items)) or (ALL)

		comps = tuple(filter(lambda x:isinstance(x, Comparison), items))

		realColumns = set()
		for col in recursiveWalk(columns):
			if isRelated(col, Column) and col is not ALL:
				realColumns.add(col)
		tables = tuple(filter(lambda x:isRelated(x, Table), items)) or getSmallestFootprint(set(self.tables), realColumns, secondaryColumns=set(map(*this.left, comps)))
		
		connections = tuple(table.linkedColumns[col] == otherTable.linkedColumns[col] for i, table in enumerate(tables) for col in table for otherTable in tables[i+1:] if col in otherTable)

		joinedColumns = {col for t in tables for col in t.columns}

		distant, local = binner(lambda x:x.left in joinedColumns, comps, default=2)
		if distant:
			wheres = connections + local + createSubqueries(SQLDict(tables), self.tables, distant)
		else:
			wheres = connections + local
		
		order = tuple(filter(lambda x:isinstance(x, Query) and len(x.words) == 2 and isRelated(x.words[0], Column) and (x.words[1] is DESC or x.words[1] is ASC), items))

		query = SELECT (*columns) - FROM (*tables)
		if wheres:
			query = query - WHERE (*wheres)
		if order:
			query = query - ORDER - BY (*order)
		return self(query)

	@property
	def __version__(self):
		try:
			return int(self._connection.execute("PRAGMA user_version;").fetchone()[0])
		except:
			return None

	@property
	def valid(self):
		"""Check if all of the class-specific assertions hold."""
		try:
			return all(map(*this.condition(self), self.assertions))
		except Exception as e:
			self.LOG.exception(e)
			return None

	def fix(self):
		"""Apply the pre-defined fix for all class-specified assertions."""
		try:
			for assertion in self.assertions:
				if not assertion.condition(self):
					assertion.rectify(self)
		except Exception as e:
			self.LOG.exception(e)

	@property
	def exception(self):
		"""Return the exception object that represents the problem that breaks the first found assertion."""
		for assertion in self.assertions:
			try:
				if not assertion.condition(database=self):
					return assertion.exception(self)
			except Exception as e:
				self.LOG.exception(e)
				return assertion.exception(self)

	@property
	def indexesHash(self) -> int:
		"""hash of the original SQL text that created all indexes in the database."""
		try:
			return int(hashlib.md5(" ".join("; ".join(sorted(map(lambda x:f"{x[0]} {x[1][x[1].index('ON'):]}".strip(), map(lambda x:(x[0], x[1].strip()), filter(lambda x:x[1], self(SELECT (NAME, SQL) - FROM (SQLITE_MASTER) - WHERE (type='index'))))))).strip().split()).encode("utf-8")).hexdigest(), base=16)
		except Exception as e:
			self.LOG.exception(e)
			return 0

	@property
	def tablesHash(self) -> int:
		"""hash of the original SQL text that created all tables in the database."""
		try:
			return int(hashlib.md5(" ".join("; ".join(sorted(map(lambda x:f"{x[0]} {x[1][x[1].index('('):]}".strip(), map(lambda x:(x[0], x[1].strip()), filter(lambda x:x[1], self(SELECT (NAME, SQL) - FROM (SQLITE_MASTER) - WHERE (type='table'))))))).strip().split()).encode("utf-8")).hexdigest(), base=16)
		except Exception as e:
			self.LOG.exception(e)
			return 0
		
	def reopen(self, mode : Mode):
		self.close()
		self.__init__(self.filename, mode=mode)

	def createIndex(self : Self, index : Index) -> bool:
		"""Create a given Index-object inside the database. Returns True if succesfull, returns False otherwise."""
		try:
			self(CREATE - INDEX - sql(index))
			return True
		except Exception as e:
			self.LOG.exception(e)
			return False

	def clearIndexes(self) -> bool:
		"""Drops all* indexes from the database. Returns True if successfull, returns False if unsuccesfull.
		*Not all, autoincrement indexes can't be removed."""
		try:
			for indexName in self(SELECT(NAME) - FROM(SQLITE_MASTER) - WHERE(type = "index")):
				try:
					self(DROP - INDEX - IF - EXISTS(Hardcoded(indexName)))
				except:
					pass
			return True
		except Exception as e:
			self.LOG.exception(e, stacklevel=logging.DEBUG)
			return False

	def commit(self):
		self._connection.commit()

	def close(self):
		try:
			self._connection.close(identifier=id(self))
		except:
			pass
		