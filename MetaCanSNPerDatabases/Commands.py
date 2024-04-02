
from MetaCanSNPerDatabases import *
from MetaCanSNPerDatabases.modules.Functions import formatType, loadFromReferenceFile, loadFromTreeFile, loadFromSNPFile, DownloadFailed
from MetaCanSNPerDatabases.modules.Globals import *
import argparse, sys

class MissingArgument(Exception): pass

def read(databasePath : str=None, table : str=None, tables : list[str]=None, **kwargs):

	database = openDatabase(databasePath, "r")
	
	code = database.checkDatabase()

	database.validateDatabase(code)

	print(database)
	if tables is None and table is None:
		print(database.TreeTable)
		print(database.SNPTable)
		print(database.ChromosomesTable)
		print(database.ReferenceTable)
	elif tables is None:
		if table not in database.Tables:
			raise NameError(f"The following table is not a valid table name: {table}\nCurrent existing table names are: {sorted(database.Tables.keys())}")
		rowFormat = " | ".join(formatType([tp for tp, *_ in database.Tables[table]._types]))
		for row in database.Tables[table]:
			rowFormat.format(*row)
	else:
		if len(unidentifieds := [t for t in tables if t not in database.Tables]) > 0:
			raise NameError(f"The following tables are not valid table names: {unidentifieds}\nCurrent existing table names are: {sorted(database.Tables.keys())}")
		for table in tables:
			rowFormat = " | ".join(formatType([tp for tp, *_ in database.Tables[table]._types]))
			for row in database.Tables[table]:
				rowFormat.format(*row)

def write(databasePath : str=None, rectify : bool=False, SNPFile : str=None, treeFile : str=None, referenceFile : str=None, **kwargs):

	database : DatabaseWriter = openDatabase(databasePath, "w")

	code = database.checkDatabase()
	if rectify:
		n = 0
		while code != 0 or n < 10:
			database.rectifyDatabase(code)
			if code == (code := database.checkDatabase()):
				print("Rectified database but got the same error code again.")
				database.validateDatabase(code)
			n+=1
		if n == 10 and code != 0:
			print("Repeatedly attempted to rectify the database, but failed to. This exception was the last one caught.")
			database.validateDatabase(code)
	else:
		database.validateDatabase(code)
	
	print(database)
	
	if SNPFile is not None:
		loadFromReferenceFile(database, referenceFile)
		loadFromTreeFile(database, treeFile)
		loadFromSNPFile(database, SNPFile)
	else:
		LOGGER.warning("No files given to build database from. Created an empty database with the current MetaCanSNPer structure.")

	database.commit()
	database.close()

def update(databasePaths : list[str]=None, refDir : str=".", noCopy=False, **kwargs):

	import os
	for databaseName in databasePaths:
		try:
			database = openDatabase(databaseName, "w")
			LOGGER.debug(f"Database {databaseName} is of version v.{database.__version__}")
			code = database.checkDatabase()
			database.validateDatabase(code, throwError=False)

			oldCwd = os.path.realpath(os.curdir)
			
			database.rectifyDatabase(code, copy=not noCopy, refDir=refDir)

			database.close()
			print(f"Finished updating {databaseName!r}")
		except Exception as e:
			try:
				database.close()
			except:
				pass
			LOGGER.exception(e)
			print(f"Failed in updating {databaseName!r} due to:\n{type(e).__name__}: {e}")

def download(databaseNames : list[str]=[], outDir : str=".", **kwargs):
	for databaseName in databaseNames:
		try:
			if downloadDatabase(databaseName, os.path.join(outDir, databaseName)) is None:
				raise DownloadFailed(f"Failed to download {databaseName} to {os.path.join(outDir, databaseName)}.")
			print(f"Finished downloading {databaseName!r}")
		except Exception as e:
			LOGGER.exception(e)
			print(f"Failed in downloading {databaseName!r}")

def test(**kwargs):

	print("Testing Download:")
	download(kwargs)

	print("Testing Update:")
	update(kwargs)

	print("Testing Read:")
	for databaseName in kwargs.database:
		print(f"  {databaseName.replace(os.path.realpath('.'), '.').replace(os.path.expanduser('~'), '~')}")
		LOGGER.debug(f"{databaseName.replace(os.path.realpath('.'), '.').replace(os.path.expanduser('~'), '~')}")

		database = openDatabase(databaseName, "r")
		LOGGER.debug(repr(database))

		print(f"    Arbitrary `.get` from one table only.")
		LOGGER.debug(f"Arbitrary `.get` from one table only.")
		database.get(Columns.Position, Columns.Ancestral, Columns.Derived, ChromID=1)
		LOGGER.debug("\n".join(database.get(Columns.Position, Columns.Ancestral, Columns.Derived, ChromID=1)))

		print(f"    Get one entry from a table.")
		LOGGER.debug(f"Get one entry from a table.")
		chromID, chromosome, genomeID = database.ChromosomesTable.first()
		LOGGER.debug(f"{chromID=}, {chromosome=}, {genomeID=}")

		genomeID, genome, *rest = database.ChromosomesTable.first()
		LOGGER.debug(f"{genomeID=}, {genome=}, {rest=}")

		print(f"    Arbitrary `.get` from one table referencing adjacent table.")
		LOGGER.debug(f"Arbitrary `.get` from one table referencing adjacent table.")
		LOGGER.debug("\n".join(database.get(Columns.Position, Columns.Ancestral, Columns.Derived, Chromosome=chromosome)))

		print(f"    Arbitrary `.get` from one table referencing across more than one chained table.")
		LOGGER.debug(f"Arbitrary `.get` from one table referencing across more than one chained table.")
		LOGGER.debug("\n".join(database.get(Columns.Position, Columns.Ancestral, Columns.Derived, Genome=genome)))

def main():

	parser = argparse.ArgumentParser(prog="MetaCanSNPerDatabases")

	modeGroup : argparse._SubParsersAction = parser.add_subparsers(title="Mode", dest="mode", description="Mode with which to open the database.", metavar="MODES")

	readParser : argparse.ArgumentParser = modeGroup.add_parser("read", help="Print out data from tables in database.")
	readParser.add_argument("database", type=os.path.realpath)
	readParser.add_argument("--table",		nargs="+",		default=[],		choices=["TreeTable", "SNPTable", "ChromosomesTable", "ReferenceTable"])
	readParser.set_defaults(func=read)

	writeParser : argparse.ArgumentParser = modeGroup.add_parser("write",	help="Create a database with or without data. Data for database is given through the appropriate File flags.")
	writeParser.add_argument("database", type=os.path.realpath)
	filesGroup = writeParser.add_argument_group(title="Input Files")
	
	filesGroup.add_argument("--SNPFile", help="If used, make sure that the related references and tree nodes are present in the database or in the other flagged files.")
	filesGroup.add_argument("--referenceFile")
	filesGroup.add_argument("--treeFile")

	writeParser.add_argument("--refDir", help="Directory where the reference genomes are located. This is only required if your --referenceFile doesn't have a `chromosomes` column.")

	optionalGroup = writeParser.add_argument_group(title="Optional Flags")
	optionalGroup.add_argument("--rectify",	action="store_true", help="If used, will edit the database structure if it doesn't comply with the current set schema. If not used, will continue operations without rectifying, but the program might crash due to the difference in schema.")

	writeParser.set_defaults(func=write)

	updateParser : argparse.ArgumentParser = modeGroup.add_parser("update", help="Update an existing database to follow the current standard schema.")
	updateParser.add_argument("database", nargs="+", type=os.path.realpath)
	updateParser.add_argument("--refDir")
	updateParser.add_argument("--noCopy", action="store_true")
	updateParser.set_defaults(func=update)
	
	downloadParser : argparse.ArgumentParser = modeGroup.add_parser("download", help="Download a database from one of the internally defined sources.")
	downloadParser.add_argument("database", nargs="+", type=os.path.basename)
	downloadParser.add_argument("--outDir", default=os.path.realpath("."))
	downloadParser.set_defaults(func=download)

	testParser : argparse.ArgumentParser = modeGroup.add_parser("test", help="Test out the features of MetaCanSNPerDatabases to see if your environment is suitable for using it.")
	testParser.add_argument("database", nargs="+", type=os.path.realpath)
	testParser.add_argument("--refDir")
	testParser.add_argument("--noCopy", action="store_true")
	testParser.add_argument("--outDir", default=os.path.realpath("."))
	testParser.set_defaults(func=test)

	parser.add_argument("--version", action="store_true")
	parser.add_argument("--debug", action="store_true")
	parser.add_argument("--info", action="store_true")
	parser.add_argument("--noLog", action="store_true")

	if len(sys.argv) <= 1:
		parser.print_help()
		exit(0)
	elif "--version" in sys.argv:
		print(f"MetaCanSNPerDatabases v. {CURRENT_VERSION}")
		exit(0)
	elif all(mode not in sys.argv for mode in ["read", "write", "update", "download", "test"]):
		print("No mode chosen check usage to see which mode is appropriate for your intended use.", file=sys.stderr)
		parser.print_help()
		exit(1)

	args : argparse.Namespace = parser.parse_args(sys.argv[1:])
	
	if args.debug:
		LOGGER.setLevel(logging.DEBUG)
	elif args.info:
		LOGGER.setLevel(logging.INFO)
	if args.noLog:
		LOGGER.disabled = True

	LOGGER.debug(f"{args.database=}")

	try:
		args.func(**dict(args._get_kwargs()))
	except Exception as e:
		LOGGER.exception(e)
		print(f"{type(e).__name__}:", e, file=sys.stderr)
		exit(1)

	print("Done!")

	exit(0)

