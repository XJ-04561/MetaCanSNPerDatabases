
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
from MetaCanSNPerDatabases._core.Structures import Overload
from MetaCanSNPerDatabases._core.Words import *

pluralPattern = re.compile(r"s$|x$|z$|sh$|ch$")
hiddenPattern = re.compile(r"^_[^_].*")

def pluralize(string : str) -> str:
	match pluralPattern.search(string):
		case None:
			return f"{string}s"
		case _:
			return f"{string}es"

whitespacePattern = re.compile(r"\s+")
sqlite3TypePattern = re.compile(r"(?P<integer>INTEGER)|(?P<decimal>DECIMAL)|(?P<char>(VAR)?CHAR[(](?P<number>[0-9]*)[)])|(?P<date>DATE)|(?P<datetime>DATETIME)|(?P<text>TEXT)")

def formatType(columns : tuple[Column]):

	d = {"unknown" : True}
	d.setdefault(False)
	for tp in map(lambda col : col.type, columns):
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
def getSmallestFootprint(columns : set[Column], tables : list[list[set[Column],Table,int]]):

	if len(columns) == 0:
		return []
	elif len(tables) == 0:
		raise ColumnNotFoundError(f"Columns: {columns} could not be found in any of the given tables: ")
	else:
		for i, (cols, *_) in enumerate(tables):
			tables[i][2] = len(cols.intersection(columns))
		tables.sort(key=lambda x:x[2])
		try:
			return [tables[-1][1]] + getSmallestFootprint(columns.difference(tables[-1][0]), tables[:-1])
		except ColumnNotFoundError as e:
			e.add_note(tables[-1])
			raise e

@Overload
def getShortestPath(table1 : Table, table2 : Table, tables : set[Table]) -> tuple[tuple[Table,Column]]:

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
		
		(commonCol, table), path = max(filter(lambda tupe : tupe[1][-1] is not None, paths.items()), key=lambda colPath : len(colPath[1]))
		return ((commonCol, table),) + path

@getShortestPath.add
def getShortestPath(sources : tuple[Table], destinations : tuple[Table], tables : set[Table]) -> tuple[tuple[Table,Column]]:
	
	shortestPath = range(len(tables)+1)
	for source in sources:
		for destination in destinations:
			if len(path := getShortestPath(source, destination, tables)) < len(shortestPath):
				shortestPath = path
	if isinstance(shortestPath, range):
		raise TablesNotRelated(f"Source tables {sources} are not connected to destination tables {destinations}")
	else:
		return shortestPath

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
