
from MetaCanSNPerDatabases import *
from MetaCanSNPerDatabases.modules.Functions import formatType, loadFromReferenceFile, loadFromTreeFile, loadFromSNPFile
from MetaCanSNPerDatabases.modules.Globals import *
import argparse, sys

class MissingArgument(Exception): pass

def read(args):

	database = openDatabase(args.filepath, "r")

	code = database.checkDatabase()

	database.validateDatabase(code)

	print(database)
	if "table" not in args:
		print(database.TreeTable)
		print(database.SNPTable)
		print(database.ChromosomesTable)
		print(database.ReferenceTable)
	else:
		if len(unidentifieds := [table for table in args.table if table not in database.Tables]) > 0:
			raise NameError(f"The following tables are not valid table names: {unidentifieds}\nCurrent existing table names are: {sorted(database.Tables.keys())}")
		for table in args.table:
			rowFormat = " | ".join(formatType([tp for tp, *_ in database.Tables[table]._types]))
			for row in database.Tables[table]:
				rowFormat.format(*row)

def write(args : argparse.Namespace):

	database = openDatabase(args.filepath, "w")

	code = database.checkDatabase()
	if args.rectify:
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
	
	if args.SNPFile:
		loadFromReferenceFile(database, args.referenceFile)
		loadFromTreeFile(database, args.treeFile)
		loadFromSNPFile(database, args.SNPFile)
	else:
		LOGGER.warning("No files given to build database from. Created an empty database with the current MetaCanSNPer structure.")

	database.commit()

def update(args):

	import os

	database = openDatabase(args.filepath, "w")
	code = database.checkDatabase()
	database.validateDatabase(code)

	oldCwd = os.curdir
	os.chdir(args.refDir)

	database.rectifyDatabase(code, copy=False)

	os.chdir(oldCwd)

def main():

	parser = argparse.ArgumentParser(prog="MetaCanSNPerDatabases")

	modeGroup : argparse._SubParsersAction = parser.add_subparsers(title="Mode", dest="mode", description="Mode with which to open the database.", metavar="MODES")

	readParser : argparse.ArgumentParser = modeGroup.add_parser("read")
	parser.add_argument("--table",		nargs="+",		default=None)
	readParser.set_defaults(func=read)

	writeParser : argparse.ArgumentParser = modeGroup.add_parser("write",	help="Create a database with or without data. Data for database is given through the appropriate File flags.")
	filesGroup = parser.add_argument_group(title="Input Files")
	
	filesGroup.add_argument("--SNPFile", help="If used, make sure that the related references and tree nodes are present in the database or in the other flagged files.")
	filesGroup.add_argument("--referenceFile")
	filesGroup.add_argument("--treeFile")

	filesGroup.add_argument("--refDir", help="Directory where the reference genomes are located. This is only required if your --referenceFile doesn't have a `chromosomes` column.")

	optionalGroup = parser.add_argument_group(title="Optional Flags")
	optionalGroup.add_argument("--rectify",	action="store_true", help="If used, will edit the database structure if it doesn't comply with the current set schema. If not used, will continue operations without rectifying, but the program might crash due to the difference in schema.")
	
	writeParser.set_defaults(func=write)

	updateParser : argparse.ArgumentParser = modeGroup.add_parser("update")
	updateParser.add_argument("--refDir")
	updateParser.set_defaults(func=update)

	parser.add_argument("filepath")

	parser.add_argument("--version", action="store_true")

	if len(sys.argv) <= 1:
		parser.print_help()
		exit(0)
	elif "--version" in sys.argv:
		print(f"MetaCanSNPerDatabases v. {CURRENT_VERSION}")
		exit(0)
	elif sys.argv[1] not in ["read", "write", "update"]:
		print("No mode chosen check usage to see which mode is appropriate for your intended use.", file=sys.stderr)
		parser.print_help()
		exit(1)

	args : argparse.Namespace = parser.parse_args(sys.argv[1:])
	
	args.func(args)

	print("Done!")

main()