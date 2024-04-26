
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from SQLOOP._core.Words import *

def isType(value, typed):
	if get_origin(typed) in [Dict, dict]:
		value = value.items()
		typed = Iterable[All[tuple[get_args(typed)]]]
	if get_origin(typed) in [None,Union]:
		return isinstance(value, typed)
	elif isinstance(value, get_origin(typed)):
		subTypes = get_args(typed)
		if get_origin(subTypes[0]) is All:
			tp = Union[get_args(subTypes[0])]
			for v in value:
				if not isType(v, tp):
					return False
		elif subTypes[-1] is Rest:
			for v, subType in zip(value, ChainMap(itertools.repeat(Any), subTypes[:-1])):
				if not isType(v, tp):
					return False
		elif get_origin(subTypes[-1]) is Rest:
			rest = Union[get_args(subTypes[-1])]
			for v, subType in zip(value, ChainMap(itertools.repeat(rest), subTypes[:-1])):
				if not isType(v, subType):
					return False
		else:
			if len(value) != (subTypes):
				return False
			for v, subType in zip(value, subTypes):
				if not isType(v, subType):
					return False
		return True
	return False

pluralPattern = re.compile(r"s$|x$|z$|sh$|ch$")
hiddenPattern = re.compile(r"^_[^_].*")

def pluralize(string : str) -> str:
	match pluralPattern.search(string):
		case None:
			return f"{string}s"
		case _:
			return f"{string}es"

def formatType(columns : tuple[Column]):

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

def hashQuery(database : Database, query : Query):
	return hashlib.md5(
			whitespacePattern.sub(
				" ",
				"; ".join([
					x[0]
					for x in database(query)
					if type(x) is tuple and x[0] is not None
				])
			).encode("utf-8")
		).hexdigest()

@cache
def getSmallestFootprint(columns : set[Column], tables : set[tuple[set[Column],Table]]):
	
	if len(columns) == 0:
		return []
	elif len(tables) == 0:
		raise ColumnNotFoundError(f"Columns: {columns} could not be found in any of the given tables: ")
	else:
		colsAndTable = max(tables, key=next(this[0].intersection(columns).__len__()))
		try:
			return [colsAndTable[1]] + getSmallestFootprint(columns.difference(colsAndTable[0]), tables.difference((colsAndTable,)))
		except ColumnNotFoundError as e:
			e.add_note(tables[-1])
			raise e

@overload
def getShortestPath(table1 : Table, table2 : Table, tables : set[Table]) -> tuple[tuple[Table,Column]]:
	...
@overload
def getShortestPath(sources : tuple[Table], destinations : tuple[Table], tables : set[Table]) -> tuple[tuple[Table,Column]]:
	...
@cache
def getShortestPath(*args) -> tuple[tuple[Table,Column]]:
	if isType(args, tuple[tuple[Table],tuple[Table], set[Table]]):
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

def downloadDatabase(databaseName : str, dst : str, reportHook=lambda block, blockSize, totalSize : None) -> str|None:
	from urllib.request import urlretrieve
	
	for source in SOURCES:
		try:
			(filename, msg) = urlretrieve(source.format(databaseName=databaseName), filename=dst, reporthook=reportHook) # Throws error if 404
			return filename
		except Exception as e:
			LOGGER.info(f"Database {databaseName!r} not found/accessible on {source!r}.")
			LOGGER.info(e)
	LOGGER.error(f"No database named {databaseName!r} found online. Sources tried: {SOURCES}")
	return None
