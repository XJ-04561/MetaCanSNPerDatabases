
from MetaCanSNPerDatabases.modules.Globals import *
import MetaCanSNPerDatabases.modules.Globals as Globals
import MetaCanSNPerDatabases.modules.Columns as Columns
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
from MetaCanSNPerDatabases.modules._Constants import *
from MetaCanSNPerDatabases.modules.Databases import DatabaseWriter

import inspect

class MissingReferenceFile(Exception): pass

def private(func):
	print("\n".join(map(repr, inspect.stack()[0])))
	return func

whitespacePattern = re.compile(r"\s+")
sqlite3TypePattern = re.compile(r"(?P<integer>INTEGER)|(?P<varchar>VARCHAR[(](?P<number>[0-9]*)[)])|(?P<date>DATE)|(?P<text>TEXT)")

def formatType(tps):

	d = {"unknown" : True}
	d.setdefault(False)
	for tp in tps:
		d |= sqlite3TypePattern.fullmatch(tp).groupdict()
		
		match next(filter(d.get, ["integer", "varchar", "date", "text", "unknown"])):
			case "integer":
				yield "{:>9d}"
			case "varchar":
				yield "{:>" + str(int(d.get("number"))+2) + "s}"
			case "date":
				yield "{:>12s}"
			case "text":
				yield "{:>20s}"
			case "unknown":
				yield "{:>20}"

def loadFromReferenceFile(database : DatabaseWriter, file : TextIO, refDir : str="."):
	file.seek(0)
	if "genome	strain	genbank_id	refseq_id	assembly_name" == file.readline():
		for row in file:
			genome, strain, genbank_id, refseq_id, assembly_name = row.strip().split("\t")
			database.addReference(genome, strain, genbank_id, refseq_id, assembly_name)
			try:
				chrom = open(os.path.join(refDir, f"{assembly_name}.fna"), "r").readline()[1:].split()[0]
				database.addChromosome(chromosomeName=chrom, genomeName=genome)
			except FileNotFoundError as e:
				raise MissingReferenceFile(f"Could not find reference file {os.path.join(refDir, f'{assembly_name}.fna')!r}. The file {file.__name__!r} does not specify chromosomes, and so the reference fasta file is required. To set the directory in which to look for .fna references, use the flag '--refDir'")

	elif "chromosomes	genome	strain	genbank_id	refseq_id	assembly_name" == file.readline():
		for row in file:
			chromosomes, genome, strain, genbank_id, refseq_id, assembly_name = row.strip().split("\t")
			database.addReference(genome, strain, genbank_id, refseq_id, assembly_name)
			for chrom in chromosomes.split(";"):
				database.addChromosome(chromosomeName=chrom, genomeName=genome)
	else:
		ValueError("File is not of accepted format.")

def loadFromTreeFile(database : DatabaseWriter, file : TextIO):
	file.seek(0)
	database.addBranch(parent=0, name=file.readline().strip())
	for row in file:
		*_, parent, child = row.rstrip(file.newlines).rsplit("\t", 2)
		database.addBranch(parent=parent, name=child)
		

def loadFromSNPFile(database : DatabaseWriter, file : TextIO):
	file.seek(0)
	if "snp_id	strain	reference	genome	position	derived_base	ancestral_base" == file.readline().strip():
		for row in file:
			nodeName, strain, reference, genome, position, ancestral, derived = row.rstrip(file.newlines).split("\t")
			database.addSNP(nodeName=nodeName, strain=strain, reference=reference, genome=genome, position=int(position), ancestral=ancestral, derived=derived)
	else:
		ValueError("File is not of accepted format.")

def updateFromLegacy(database : DatabaseWriter):
	"""Update from CanSNPer2 to MetaCanSNPer v.1 format."""

	# References
	pass # Nothing as of this version

	# Chromosomes
	database.ChromosomesTable.create()
	for i, assembly in database.ReferenceTable.get(Columns.GenomeID, Columns.Assembly):
		database._connection.execute(f"INSERT (?, ?, ?) INTO {TABLE_NAME_CHROMOSOMES};", [
			i,
			open(f"{assembly}.fna").readline()[1:].split()[0],
			i
		])
	
	# SNPs
	database._connection.execute("ALTER TABLE snp_annotation RENAME TO snp_annotation_old;")
	database.SNPTable.create()
	database._connection.execute(f"INSERT INTO {TABLE_NAME_SNP_ANNOTATION} (null, node_id-1, position, ancestral_base, derived_base, reference, date, genome_i) FROM snp_annotation_old;")
	database._connection.execute("DROP TABLE snp_annotation_old;")
	
	# Tree
	database._connection.execute("ALTER TABLE tree RENAME TO tree_old;")
	database.TreeTable.create()
	database._connection.execute(f"INSERT INTO {TABLE_NAME_TREE} (tree_old.parent-1, null, nodes.name) FROM tree_old, nodes WHERE tree_old.parent = nodes.id;")
	database._connection.execute(f"DELETE FROM {TABLE_NAME_TREE} WHERE parent = 0 OR child = 0;")
	database._connection.execute(f"UPDATE TABLE {TABLE_NAME_TREE} SET parent=0 WHERE child = 1;")
	database._connection.execute("DROP TABLE nodes;")
	database._connection.execute("DROP TABLE tree_old;")

def downloadDatabase(databaseName : str, dst : str) -> str:
	from urllib.request import urlretrieve
	
	for source in SOURCES:
		try:
			(filename, msg) = urlretrieve(source.format(databaseName=databaseName), filename=dst) # Throws error if 404
			return filename
		except Exception as e:
			LOGGER.info(f"Database {databaseName!r} not found/accessible on {source!r}.")
			LOGGER.exception(e, stacklevel=logging.INFO)
	LOGGER.error(f"No database named {databaseName!r} found online. Sources tried: {SOURCES}")
	return None

@cache
def generateTableQuery(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag,Direction]|tuple[tuple[ColumnFlag,Direction]]=None, **where : Any) -> Generator[tuple[Any],None,None]:
	"""All positional arguments should be `ColumnFlag` objects and they are used to
	determine what information to be gathered from the database.
	
	All keyword arguments (except `orderBy`) are the conditions by which each row
	is selected. For example, if you inted to get the row for a specific genbankID
	then you would use the keyword argument as such: `genbankID="GCA_123123123.1"`.
	
	`orderBy` is used to sort the selected data according to `ColumnFlag` with or
	without a direction string ("DESC" or "ASC"). Input can be a list to use many
	columns for sorting, and any item of the list can be with or without a
	direction specified."""
	query = f"SELECT {', '.join(map(Columns.LOOKUP[self._tableName].__getitem__, select))} FROM {self._tableName}"
	params = []
	if where != {}:
		_tmp = []
		for col, val in where.items():
			_tmp.append(f'{col} = ?')
			params.append(val)
		query += f" WHERE {' AND '.join(_tmp)}"
		del _tmp
	
	if orderBy is None:
		orderBy = tuple()
	elif isinstance(orderBy, ColumnFlag):
		orderBy = [(orderBy, "DESC")]
	elif isinstance(orderBy, tuple) and not isinstance(orderBy[0], tuple):
		orderBy = [orderBy]
	
	if len(orderBy) > 0:
		# Create an ordered list of all "ORDER BY X [DIRECTION]"-statements
		orderBy = [tupe if type(tupe) is tuple else (tupe, "DESC") for tupe in orderBy]
		query += f" ORDER BY {', '.join(map(' '.join, orderBy))}"
	
	return query, params

@cache
def generateQuery(*select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag,Direction]|tuple[tuple[ColumnFlag,Direction]]=None, **where : Any) -> tuple[str,list[Any]]:
	"""All positional arguments should be `ColumnFlag` objects and they are used to
	determine what information to be gathered from the database.
	
	All keyword arguments (except `orderBy`) are the conditions by which each row
	is selected. For example, if you inted to get the row for a specific genbankID
	then you would use the keyword argument as such: `genbankID="GCA_123123123.1"`.
	
	`orderBy` is used to sort the selected data according to `ColumnFlag` with or
	without a direction string ("DESC" or "ASC"). Input can be a list to use many
	columns for sorting, and any item of the list can be with or without a
	direction specified."""

	# Find out which tables are involved in this query. Also makes some necessary assertions for the rest of the function.
	tables = {Columns.UNIQUELOOKUP[col] for col in Columns.UNIQUES.intersection(select, where, map(lambda x : x if type(x) is ColumnFlag else x[0], orderBy))}
	for col in Columns.COMMONS.intersection(select, where, map(lambda x : x[0], orderBy)):
		assert col in Columns.NAMES, f"No such column `{col}` in `Columns.NAMES`"
		if tables.isdisjoint(Columns.COMMONLOOKUP[col]):
			for table in Columns.COMMONLOOKUP[col]:
				if not tables.isdisjoint(Columns.RELATIONSHIPS[table].values()):
					tables.add(table)
					break
			assert table in tables, f"Not implemented to join two disjoint tables, {table=} could not be joined with any of {tables=}"

	def getTable(col):
		for table in tables:
			if col in Columns.LOOKUP[table]:
				return table

	# If selecting all columns then change the selection string into "*", otherwise create a list of "WHERE" statements
	if Columns.ALL in select:
		selection = "*"
	else:
		selection = [f"{table}.{Columns.LOOKUP[table][col]}" for table, col in zip(map(getTable, select), select)]

	# Simply join all the tables into a comma(+space) separated list
	source = ", ".join(tables)
	
	# Create "WHERE"-statements that are meant to show how the tables are connected, like: table1.colA = table2.colB
	connections = []
	for i, table in enumerate(tables):
		for otherTable in tables.intersection(Columns.RELATIONSHIPS[table].values()):
			col = Columns.RELATIONS[table,otherTable]
			connections.append(f"{table}.{Columns.LOOKUP[table][col]} = {table}.{Columns.LOOKUP[otherTable][col]}")
			break

	params = list(where.values())
	conditions = " AND ".join([f"{table}.{Columns.LOOKUP[table][col]} = ?" for table, col in zip(map(getTable, params), params)])

	# Create an ordered list of all "ORDER BY X [DIRECTION]"-statements
	if orderBy is None:
		orderBy = tuple()
	elif isinstance(orderBy, ColumnFlag):
		orderBy = [(orderBy, "DESC")]
	elif isinstance(orderBy, tuple) and not isinstance(orderBy[0], tuple):
		orderBy = [orderBy]

	if len(orderBy) > 0:
		orderBy = [tupe if type(tupe) is tuple else (tupe, "DESC") for tupe in orderBy]
		
		andTable = lambda col, direction : (getTable(col), col, direction)

		keyColumn = ", ".join([f"{table}.{Columns.LOOKUP[table][col]} {direction}" for table, col, direction in map(andTable, orderBy)])
	
		return f"SELECT {selection} FROM {source} WHERE {conditions} ORDER BY {keyColumn};", params
	else:
		return f"SELECT {selection} FROM {source} WHERE {conditions};", params