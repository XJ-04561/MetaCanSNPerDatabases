
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals

LOGGER = Globals.LOGGER.getChild("Functions")

class ImpossiblePathing(SQLOOPError):
	msg = "Unable to find path through which to conditionally select from {tables} based on columns {columns}."

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

@cache
def getSmallestFootprint(tables : set["Table"], columns : set["Column"], secondaryColumns : set["Column"]=None) -> tuple["Table"]|None:
	
	LOG = LOGGER.getChild("getSmallestFootprint")
	LOG.debug(f"Called with signature: ({tables=}, {columns=}, {secondaryColumns=})")
	mustHaves = set(filter(None, map(*this.table, columns)))
	candidates = []
	for i in range(len(tables)):
		for subTables in filter(mustHaves.issubset, itertools.combinations(tuple(tables), i+1)):
			for col in columns:
				if not any(col in t for t in subTables):
					break
			else:
				candidates.append(subTables)
		if candidates:
			break
	LOG.debug(f"Candidates: {candidates}")
	if secondaryColumns is not None:
		ret = max(candidates, key=lambda candTables:sum(any(c in t for t in candTables) for c in secondaryColumns))
	else:
		ret = next(iter(candidates)) if candidates else ()
	
	LOG.debug(f"Returned {ret}")
	return ret

def disambiguateColumn(column, tables):
	from SQLOOP._core.Aggregates import Aggregate, GROUP_CONCAT
	from SQLOOP._core.Structures import Operation
	from SQLOOP._core.Schema import ALL

	if column is ALL:
		return ALL
	elif isRelated(column, Column):
		match sum(column in t for t in tables):
			case 0:
				raise ColumnNotFoundError(f"Column {column} not found in any of tables: {tables}")
			case 1:	
				return column
			case _:
				return next(column in t for t in tables)
	elif isinstance(column, Aggregate):
		if isRelated(column.X, Column):
			match sum(column.X in t for t in tables):
				case 0:
					raise ColumnNotFoundError(f"Column {column.X} not found in any of tables: {tables}")
				case 1:
					if isinstance(column, GROUP_CONCAT):
						return type(column)(column.X, column.Y)
					else:
						return type(column)(column.X)
				case _:
					if isinstance(column, GROUP_CONCAT):
						return type(column)(next(column.X in t for t in tables), column.Y)
					else:
						return type(column)(next(column.X in t for t in tables))
		else:
			return type(column)(disambiguateColumn(column.X))
	elif isinstance(column, Operation):
		return type(column)(disambiguateColumn(column.left), column.operator, disambiguateColumn(column.right))
	else:
		return column
			

def recursiveSubquery(startCol : "Column", tables : SQLDict["Table"], values : list[Union["Comparison", "Query"]]) -> "Comparison":
	from SQLOOP._core.Words import IN, SELECT, FROM, WHERE
	LOG = LOGGER.getChild("recursiveSubquery")
	if Globals.MAX_DEBUG: LOG.debug(f"Called with signature: ({startCol=}, {tables=}, {values=})")
	if len(tables) == 0:
		raise ValueError(f"SubQuerying ran out of tables to subquery! {values=}")
	elif len(tables) == 1:
		return startCol - IN (SELECT (startCol) - FROM (tables[0]) - WHERE (*values))
	else:
		commonColumn = tables[0].columns.intersection(tables[1].columns)[0]
		return startCol - IN (SELECT (startCol) - FROM (tables[0]) - WHERE (recursiveSubquery(commonColumn, tables[1:], values)))


def subqueryPaths(startTables : SQLDict["Table"], columns : SQLDict["Column"], allTables : SQLDict["Table"]) -> list[list[list["Table"], SQLDict["Column"]]]:
	LOG = LOGGER.getChild("subqueryPaths")
	if Globals.MAX_DEBUG: LOG.debug(f"Called with signature: ({startTables=}, {columns=}, {allTables=})")
	if not columns:
		return []
	visited : set[Table] = set(startTables)
	paths : list[list[Table]] = [(t,) for t in startTables]
	while paths:
		if Globals.MAX_DEBUG: LOG.debug(f"Generation: {paths=}")
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
		if Globals.MAX_DEBUG: LOG.debug(f"New Generation: {nPaths=}")
		best, hits = max(map(lambda x:(x, columns.intersection(x[-1].columns)), nPaths), key=lambda x:len(x[1]))
		if len(hits) == len(columns):
			return ((best, hits),)
		elif len(hits) > 0:
			return ((best, hits),) + subqueryPaths(startTables.without(best), columns.without(hits), allTables.without(best))
		else:
			paths = nPaths
	return ()

def createSubqueries(startTables : SQLDict["Table"], allTables : SQLDict["Table"], values : tuple["Comparison"]):
	LOG = LOGGER.getChild("createSubqueries")
	if Globals.MAX_DEBUG: LOG.debug(f"Called with signature: ({startTables=}, {allTables=}, {values=})")
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