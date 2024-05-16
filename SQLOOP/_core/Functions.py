
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from threading import Lock

class LimitDict(dict):
	
	_size : int
	lock : Lock
	def __init__(self, size : int):
		self.lock = Lock()
		self._size = size
	def __setitem__(self, key, value):
		with self.lock:
			if len(self) < self._size:
				super().__setitem__(key, value)
			else:
				super().pop(next(iter(self)))
				super().__setitem__(key, value)
	def __delitem__(self, key):
		with self.lock:
			super().__delitem__(key)
	def setdefault(self, key, value):
		if key not in self:
			self.__setitem__(key, value)
	def pop(self, key, default=None):
		with self.lock:
			return super().pop(key, default=default)
	def popitem(self) -> tuple:
		with self.lock:
			return super().popitem()


def prodHash(obj, start=True):
	if isinstance(obj, Iterable):
		N = 0
		totalSize = 0
		for el in obj:
			n, size = prodHash(el, start=False)
			N += n << 64*totalSize
			totalSize += size
		if start:
			return N
		else:
			return N, totalSize
	elif hasattr(obj, "__hash__"):
		return hash(obj) if start else (hash(obj), 1)
	else:
		return id(obj) if start else (hash(obj), 1)

class CacheMeta(type):

	def __getattr__(self, name):
		return getattr(function, name)

class AnyCache(metaclass=CacheMeta):

	def __init__(self, func : "function", size=1000, doc=None, cache=None):
		self._cache = cache or LimitDict(size)
		self.func = func
		self.__doc__ = doc or func.__doc__
		self.__name__ = func.__name__
		self.__qualname__ = func.__qualname__

	def __call__(self, *args, __self__=None, **kwargs):
		if hasattr(self.func, "__self__"):
			hashKey = prodHash(itertools.chain((self.func.__self__,), args, kwargs.items()))
		else:
			hashKey = prodHash(itertools.chain(args, kwargs.items()))
		if hashKey not in self._cache:
			if __self__ is not None:
				self._cache[hashKey] = self.func(*args, **kwargs)
			else:
				self._cache[hashKey] = self.func(*args, **kwargs)
		return self._cache[hashKey]
	
	def __get__(self, instance, owner):
		if instance is not None:
			return AnyCache(self.func.__get__(instance), cache=self._cache)
		else:
			return self

	def __set_name__(self, owner, name):
		self.owner = owner
		self.__name__ = name

def isType(instance, cls):
	
	if isinstance(instance, type):
		if not isinstance(cls, Generic|GenericAlias):
			return isRelated(instance, cls)
		if not hasattr(instance, "__iter__"):
			return False
		if not isRelated(instance, get_origin(cls)):
			return False
		
		args = get_args(cls)
		if isinstance(args[0], Generic|GenericAlias) and get_origin(args[0]) is All:
			args = itertools.repeat(Union[get_args(args[0])])
		elif sum(1 for _ in instance) < len(args)-1:
			return False
		elif isinstance(args[-1], Generic|GenericAlias) and get_origin(args[-1]) is Rest:
			args = itertools.chain(args[:-1], itertools.repeat(Union[get_args(args[-1])]))
		elif sum(1 for _ in instance) != len(args):
			return False
		
		for item, tp in zip(instance, get_args(cls), strict=True):
			if not isType(item, tp):
				return False
		return True
	elif isinstance(cls, Generic|GenericAlias):
		if isinstance(instance, type):
			return isRelated(instance, cls)
		if not isType(instance, get_origin(cls)):
			return False
		if isType(instance, dict):
			keys, values = get_args(cls)
			return all(isinstance(key, keys) for key in instance) and all(isinstance(value, values) for value in instance.values())
		
		args = get_args(cls)
		if get_origin(args[0]) is All:
			args = itertools.repeat(Union[get_args(args[0])])
		elif get_origin(args[-1]) is Rest:
			args = itertools.chain(args[:-1], itertools.repeat(Union[get_args(args[-1])]))
		
		for v, tp in zip(instance, args):
			if not isType(v, tp):
				return False
		return True
	else:
		return isinstance(instance, cls)

pluralPattern = re.compile(r"s$|x$|z$|sh$|ch$")
hiddenPattern = re.compile(r"^_[^_].*")

def pluralize(string : str) -> str:
	match pluralPattern.search(string):
		case None:
			return f"{string}s"
		case _:
			return f"{string}es"
		

def formatType(columns : tuple["Column"]):

	d = {"unknown" : True}
	d.setdefault(False)
	for tp in map(*this.type, columns):
		d |= sqlite3TypePattern.fullmatch(tp).groupdict()
		
		match next(filter(d.get, ["integer", "decimal", "char", "date", "datetime", "text", "unknown"])):
			case "integer":
				yield "{:>7d}"
			case "varchar":
				yield "{:>" + str(int(d.get("number"))+2) + "s}"
			case "date":
				yield "{:>12s}"
			case "datetime":
				yield "{:>16s}"
			case "text":
				yield "{:>12s}"
			case "unknown":
				yield "{:>12}"

def hashQuery(database : "Database", query : "Query"):
	"""Converts entries returned into strings via str or repr (if __str__ not implemented) and then replaces whitespace
	with a simple " " and joines all the entries with "; " before getting the hash of the final `str` object."""
	return hash(
		whitespacePattern.sub(
			" ",
			"; ".join(
				map(
					lambda x:str(x) if hasattr(x, "__str__") else repr(x),
					database(query)
				)
			)
		)
	)
def hashSQL(items : Iterable):
	return hash(whitespacePattern.sub(" ", "; ".join(map(sql, items))))

def correctDatabase(cls, filepath):
	database = cls(filepath, "w")
			
	for _ in range(len(database.assertions)):
		if database.valid:
			break
		database.fix()
	else:
		raise database.exception
		# Will raise exception, since database was still faulty after 10 attempts at fixing it.

def verifyDatabase(cls, filepath):
	return cls(filepath, "r").valid

@AnyCache
def getSmallestFootprint(columns : set["Column"], tables : set["Table"]):
	
	if len(columns) == 0:
		return []
	elif len(tables) == 0:
		raise ColumnNotFoundError(f"Columns: {columns} could not be found in any of the given tables: ")
	else:
		bestTable = max(tables, key=lambda x:len(columns.intersection(x.columns)))
		try:
			return [bestTable] + getSmallestFootprint(columns.difference(bestTable.columns), tables.difference({bestTable}))
		except ColumnNotFoundError as e:
			e.add_note(f"{columns=}")
			e.add_note(f"{tables=}")
			raise e

@overload
def getShortestPath(table1 : "Table", table2 : "Table", tables : set["Table"]) -> tuple[tuple["Table","Column"]]:
	...
@overload
def getShortestPath(sources : tuple["Table"], destinations : tuple["Table"], tables : set["Table"]) -> tuple[tuple["Table","Column"]]:
	...

def shortPath(startTables, columns, allTables):
	
	hits = {}
	paths = [[t] for t in startTables]
	while paths:
		nPaths = []
		for p in paths:
			connected = filter(lambda t:not p[-1].columns.isdisjoint(t.columns) and all(t2.columns.isdisjoint(t.columns) for t2 in p[:-1]), allTables)
			for t in connected:
				nPaths.append([*p, t])
		paths = nPaths
		
			

	colSet = set(columns)
	visited = set()
	paths = [list(filter(lambda t:not colSet.isdisjoint(t.columns), allTables))]
	while paths[-1] != []:

	layers = []
	while True:
		layers.append( list(map(lambda t:t[1] and not colSet.isdisjoint(allTables[t[0]].columns), enumerate(layers[-1]))))
		newLayer = SQLDict()
		for table in filter(lambda t:not colSet.isdisjoint(t.columns), layers[-1]):
			if table not in layers[-1]
			newLayer.append(table)
		

@AnyCache
def getShortestPath(*args) -> tuple[tuple["Table","Column"]]:
	from SQLOOP.core import Table, Column

	if isinstance(args, tuple) and tuple[tuple[Table],tuple[Table], set[Table]]:
		sources : tuple[Table] = args[0]
		destinations : tuple[Table] = args[1]
		tables : set[Table] = args[2]

		shortestPath = range(len(tables)+1)
		for source in sources:
			for destination in destinations:
				if len(path := getShortestPath(source, destination, tables)) < len(shortestPath):
					shortestPath = path
		if isinstance(shortestPath, range):
			raise TablesNotRelated(f"Source tables {sources} are not connected to destination tables {destinations}")
		else:
			return shortestPath
	elif isType(args, tuple[Table,Table,set[Table]]):
		table1 : Table = args[0]
		table2 : Table = args[1]
		tables : set[Table] = args[2]
		if table1 == table2:
			return tuple()
		elif len(tables) == 0:
			return (None, )
		else:
			cols = set(table1.columns)
			paths = {}
			for table in tables:
				if len(commonCols := cols.intersection(table)):
					paths[tuple(commonCols)[0], table] = getShortestPath(table, table2, tables.difference({table}))
			if len(paths) == 0:
				return (None, )
			
			(commonCol, table), path = max(filter(*this[1][-1] != None, paths.items()), key=next(this[1].__len__()))
			return ((commonCol, table),) + path

from SQLOOP._core.Structures import Column, Table, Query
from SQLOOP._core.Databases import Database