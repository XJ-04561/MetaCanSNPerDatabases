
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from threading import Lock

LOGGER = Globals.LOGGER.getChild("Functions")

class ImpossiblePathing(Error):
	msg = "Unable to find path through which to conditionally select from {tables} based on columns {columns}."

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


def forceHash(obj):
	if hasattr(obj, "__hash__"):
		try:
			return hash(obj)
		except TypeError:
			pass
	if isinstance(obj, Iterable):
		return sum(forceHash(el) for el in obj)
	else:
		return id(obj)

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
			hashKey = forceHash(itertools.chain((self.func.__self__,), args, kwargs.items()))
		else:
			hashKey = forceHash(itertools.chain(args, kwargs.items()))
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

def recursiveWalk(iterable):
	"""Generator that iterates in-order through an iterable and down through all their iterable elements. Going all the
	way down through an element before progressing to the next element."""
	for item in iterable:
		if isinstance(item, Iterable):
			for innerItem in recursiveWalk(item):
				yield innerItem
		else:
			yield item

def hashQuery(database : "Database", query : "Query"):
	"""Converts entries returned into strings via str or repr (if __str__ not implemented) and then replaces whitespace
	with a simple " " and joines all the entries with "; " before getting the hash of the final `str` object."""
	return int(hashlib.md5(
		whitespacePattern.sub(
			" ",
			"; ".join(
				map(
					lambda x:str(x) if hasattr(x, "__str__") else repr(x),
					database(query)
				)
			)
		).encode("utf-8") ).hexdigest(), base=16)
def hashSQL(items : Iterable):
	return int(hashlib.md5(whitespacePattern.sub(" ", "; ".join(map(sql, items))).encode("utf-8")).hexdigest(), base=16)

def correctDatabase(cls, filepath):
	database = cls(filepath, "w")
	
	database.fix()

	if not database.valid:
		raise database.exception
		# Will raise exception, since database was still faulty
	
	database.close()

def verifyDatabase(cls, filepath):
	return cls(filepath, "r").valid

@AnyCache
def getSmallestFootprint(tables : set["Table"], columns : set["Column"], secondaryColumns : set["Column"]=None) -> tuple["Table"]|None:
	
	mustHaves = set(filter(None, map(*this.table, columns)))
	candidates = []
	for i in range(len(tables)):
		for subTables in filter(mustHaves.issubset, itertools.combinations(tables, i+1)):
			if all(map(lambda c:any(map(lambda t:c in t, subTables)), columns)):
				candidates.append(subTables)
	if secondaryColumns is not None:
		return max(candidates, key=lambda candTables:sum(any(c in t for t in candTables) for c in secondaryColumns))
	else:
		return next(iter(candidates)) if candidates else ()
	

def recursiveSubquery(startCol : "Column", tables : SQLDict["Table"], values : list[Union["Comparison", "Query"]]) -> "Comparison":
	from SQLOOP._core.Words import IN, SELECT, FROM, WHERE
	if len(tables) == 0:
		raise ValueError(f"SubQuerying ran out of tables to subquery! {values=}")
	elif len(tables) == 1:
		return startCol - IN (SELECT (startCol) - FROM (tables[0]) - WHERE (*values))
	else:
		commonColumn = tables[0].columns.intersection(tables[1].columns)[0]
		return startCol - IN (SELECT (startCol) - FROM (tables[0]) - WHERE (recursiveSubquery(commonColumn, tables[1:], values)))


def subqueryPaths(startTables : SQLDict["Table"], columns : SQLDict["Column"], allTables : SQLDict["Table"]) -> list[list[list["Table"], SQLDict["Column"]]]:
	LOG = LOGGER.getChild("subqueryPaths")
	LOG.debug(f"Called with signature: ({startTables=}, {columns=}, {allTables=})")
	if not columns:
		return []
	visited : set[Table] = set(startTables)
	paths : list[list[Table]] = [[t] for t in startTables]
	while paths:
		LOG.debug(f"Generation: {paths=}")
		nPaths = []
		for p in paths:
			for t in allTables:
				if t in visited:
					continue
				if p[-1].columns.isdisjoint(t.columns):
					continue
				if any(not t2.columns.isdisjoint(t.columns) for t2 in p[:-1]):
					continue
				nPaths.append((*p, t))
				visited.add(t)
		LOG.debug(f"New Generation: {nPaths=}")
		best, hits = max(map(lambda x:(x, columns.intersection(x[-1].columns)), nPaths), key=lambda x:len(x[0]))
		if len(hits) == len(columns):
			return ((best, hits),)
		elif len(hits) > 0:
			return ((best, hits),) + subqueryPaths(startTables | best, columns.without(hits), allTables.without(best))
		paths = nPaths
	return []

def createSubqueries(startTables : SQLDict["Table"], allTables : SQLDict["Table"], values : tuple["Comparison"]):
	LOG = LOGGER.getChild("createSubqueries")
	LOG.debug(f"Called with signature: ({startTables=}, {allTables=}, {values=})")
	_allTables = allTables.difference(startTables)
	paths = subqueryPaths(startTables, SQLDict(map(lambda x:x.left, values)), _allTables)
	subqueries = {}
	for path, targetColumns in reversed(paths):
		subValues = []
		for comp in values:
			if comp.left in targetColumns:
				subValues.append(comp)
		
		if path[-1] in subqueries:
			subValues.append(subqueries.pop(path[-1]))
		subqueries[path[0]] = recursiveSubquery(path[0].columns.intersection(path[1].columns)[0], path[1:], subValues)
		if subqueries[path[0]] is None:
			raise ImpossiblePathing(tables=startTables, columns=f"({', '.join(map(lambda x:str(x.left), values))})")
	return tuple(subqueries.values())

try:
	from SQLOOP._core.Structures import Column, Table, Query, Comparison
	from SQLOOP._core.Databases import Database
except:
	pass