
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

	modeGroup : argparse._SubParsersAction = parser.add_subparsers(title="Mode", description="Mode with which to open the database.", required=True)

	readParser : argparse.ArgumentParser = modeGroup.add_parser("read",		metavar="read",		action="store_true")
	parser.add_argument("--table",		nargs="+",		default=None)
	readParser.set_defaults(func=read)

	writeParser : argparse.ArgumentParser = modeGroup.add_parser("write",		metavar="write",	action="store_true")
	filesGroup = parser.add_argument_group(title="Input Files")
	
	filesGroup.add_argument("--SNPFile")
	filesGroup.add_argument("--referenceFile", required="--SNPFile" in sys.argv)
	filesGroup.add_argument("--treeFile", required="--SNPFile" in sys.argv)

	optionalGroup = parser.add_argument_group(title="Optional Flags")
	optionalGroup.add_argument("--rectify",	action="store_true", help="If used, will edit the database structure if it doesn't comply with the current set schema. If not used, will continue operations without rectifying, but the program might crash due to the difference in schema.")
	
	writeParser.set_defaults(func=write)

	updateParser : argparse.ArgumentParser = modeGroup.add_parser("update",	metavar="update",	action="store_true")
	parser.add_argument("--refDir",		default=None,	required=("-u" in sys.argv or "--update" in sys.argv))
	updateParser.set_defaults(func=update)

	parser.add_argument("filepath")

	args : argparse.Namespace = parser.parse_args(sys.argv)
	args.func(args)

	print("Done!")

main()