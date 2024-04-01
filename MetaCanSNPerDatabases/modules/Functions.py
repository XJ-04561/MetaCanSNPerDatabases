
from MetaCanSNPerDatabases.modules.Globals import *
import MetaCanSNPerDatabases.modules.Globals as Globals
import MetaCanSNPerDatabases.modules.Columns as Columns
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
from MetaCanSNPerDatabases.modules._Constants import *
from MetaCanSNPerDatabases.modules.Databases import DatabaseWriter

import inspect

class MissingReferenceFile(Exception): pass

def interpretSQLtype(flag, val):
	match Columns.TYPE_LOOKUP[flag][0]:
		case "INTEGER":
			return int(val)
		case _:
			return val

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
	database._connection.execute("ALTER TABLE snp_references RENAME TO snp_references_old;")
	database.ReferenceTable.create()
	database._connection.execute(f"INSERT INTO {TABLE_NAME_REFERENCES} FROM snp_references_old;")
	database._connection.execute("DROP TABLE snp_reference_old;")

	# Chromosomes
	database.ChromosomesTable.create()
	for i, assembly in database.ReferenceTable.get(Columns.GenomeID, Columns.Assembly):
		database._connection.execute(f"INSERT INTO {TABLE_NAME_CHROMOSOMES} VALUES (?, ?, ?);", [i, open(f"{assembly}.fna", "r").readline()[1:].split()[0], i])
	
	# SNPs
	database._connection.execute("ALTER TABLE snp_annotation RENAME TO snp_annotation_old;")
	database.SNPTable.create()
	database._connection.execute(f"INSERT INTO {TABLE_NAME_SNP_ANNOTATION} ({SNP_COLUMN_NODE_ID}, {SNP_COLUMN_POSITION}, {SNP_COLUMN_ANCESTRAL}, {SNP_COLUMN_DERIVED}, {SNP_COLUMN_REFERENCE}, {SNP_COLUMN_DATE}, {SNP_COLUMN_CHROMOSOMES_ID}) SELECT node_id-1, position, ancestral_base, derived_base, reference, date, genome_i FROM snp_annotation_old;")
	database._connection.execute("DROP TABLE snp_annotation_old;")
	
	# Tree
	database._connection.execute("ALTER TABLE tree RENAME TO tree_old;")
	database.TreeTable.create()
	database._connection.execute(f"INSERT INTO {TABLE_NAME_TREE} ({TREE_COLUMN_PARENT}, {TREE_COLUMN_CHILD}, {TREE_COLUMN_NAME}) SELECT tree_old.parent-1, null, nodes.name FROM tree_old, nodes WHERE tree_old.parent = nodes.id;")
	database._connection.execute(f"DELETE FROM {TABLE_NAME_TREE} WHERE parent = 0 OR child = 0;")
	database._connection.execute(f"UPDATE {TABLE_NAME_TREE} SET parent=0 WHERE child = 1;")
	database._connection.execute("DROP TABLE nodes;")
	database._connection.execute("DROP TABLE tree_old;")

	database._connection.execute(f"PRAGMA user_version = {CURRENT_VERSION};")

class DownloadFailed(Exception): pass

def downloadDatabase(databaseName : str, dst : str) -> str|None:
	from urllib.request import urlretrieve
	
	for source in SOURCES:
		try:
			(filename, msg) = urlretrieve(source.format(databaseName=databaseName), filename=dst) # Throws error if 404
			return filename
		except Exception as e:
			LOGGER.info(f"Database {databaseName!r} not found/accessible on {source!r}.")
			LOGGER.info(e)
	LOGGER.error(f"No database named {databaseName!r} found online. Sources tried: {SOURCES}")
	return None

@cache
def generateTableQuery(self, *select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> tuple[str,list[Any]]:
	"""All positional arguments should be `ColumnFlag` objects and they are used to
	determine what information to be gathered from the database.
	
	All keyword arguments (except `orderBy`) are the conditions by which each row
	is selected. For example, if you inted to get the row for a specific genbankID
	then you would use the keyword argument as such: `genbankID="GCA_123123123.1"`.
	
	`orderBy` is used to sort the selected data according to `ColumnFlag`.
	Direction is indicated by negating the the flag. A positive flag is the default
	of "DESC" and negative flags indicate "ASC"."""
	query = f"SELECT {', '.join(map(Columns.LOOKUP[self._tableName].__getitem__, select))} FROM {self._tableName}"
	params = []
	if where != {}:
		_tmp = []
		for col, val in where.items():
			_tmp.append(f'{col} = ?')
			params.append(val)
		query += f" WHERE {' AND '.join(_tmp)}"
		del _tmp
	
	if isinstance(orderBy, ColumnFlag):
		orderBy = (orderBy,)
	
	if orderBy is not None and len(orderBy) > 0:
		# Create an ordered list of all "ORDER BY X [DIRECTION]"-statements
		orderBy = [f"{Columns.LOOKUP[self._tableName][flag]} {'DESC' if flag > 0 else 'ASC'}" for flag in orderBy]
		query += f" ORDER BY {', '.join(orderBy)}"
	
	return query, params

@cache
def generateQuery(*select : ColumnFlag, orderBy : ColumnFlag|tuple[ColumnFlag]|None=None, **where : Any) -> tuple[str,list[Any]]:
	"""All positional arguments should be `ColumnFlag` objects and they are used to
	determine what information to be gathered from the database.
	
	All keyword arguments (except `orderBy`) are the conditions by which each row
	is selected. For example, if you inted to get the row for a specific genbankID
	then you would use the keyword argument as such: `genbankID="GCA_123123123.1"`.
	
	`orderBy` is used to sort the selected data according to `ColumnFlag`.
	Direction is indicated by negating the the flag. A positive flag is the default
	of "DESC" and negative flags indicate "ASC"."""

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

	if isinstance(orderBy, ColumnFlag):
		orderBy = (orderBy,)

	if orderBy is not None and len(orderBy) > 0:
		andTable = lambda flag : (getTable(col), flag, "DESC" if flag > 0 else "ASC")

		keyColumn = ", ".join([f"{table}.{Columns.LOOKUP[table][col]} {direction}" for table, col, direction in map(andTable, orderBy)])
	
		return f"SELECT {selection} FROM {source} WHERE {conditions} ORDER BY {keyColumn};", params
	else:
		return f"SELECT {selection} FROM {source} WHERE {conditions};", params