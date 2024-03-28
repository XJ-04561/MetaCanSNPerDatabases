
from MetaCanSNPerDatabases import *

def main():
	import argparse, sys

	parser = argparse.ArgumentParser(prog="MetaCanSNPerDatabases")

	parser.add_argument("filepath")

	modeGroup = parser.add_mutually_exclusive_group("Mode")
	modeGroup.add_argument("-r", "--read",		metavar="read",		action="store_true")
	modeGroup.add_argument("-w", "--write",		metavar="write",	action="store_true")

	parser.add_argument("--table", default=None)

	args = parser.parse_args(sys.argv)

	mode = "r" if args.read else "w"
	databaseName = args.filepath

	match mode:
		case "r":
			try:
				database = openDatabase(databaseName, "r")
			except (IsLegacyCanSNPer2, OutdatedCanSNPerDatabase) as e:
				msg = f"Database is in {'legacy CanSNPer' if type(e) is IsLegacyCanSNPer2 else 'an outdated'} format. Would you like to make a backup and create an up-to-date database with (more or less) the same data?\nIf so type [y/n]: "
				
				while (usr_string := input(msg).strip().lower()) not in ["y", "n"]: pass
				
				match usr_string:
					case "y":
						openDatabase(databaseName, "w")
					case "x":
						print("Exiting.")
						exit(1)
			
			database = openDatabase(databaseName, "r")

			print(database)

			print(database.TreeTable)
			print(database.NodesTable)
			print(database.SNPTable)
			print(database.ChromosomesTable)
			print(database.ReferenceTable)

			if args.table is not None:
				if args.table in database.Tables:
					for row in database.Tables[args.table].get(Columns.ALL):
						print(("{:<12}, "*len(row)).format(*row))
				else:
					raise NameError("No such table")
		case "w":
			try:
				database = openDatabase(databaseName, "w")
			except Exception as e:
				e.add_note(repr(openDatabase(databaseName, "r")))
				raise e
			
			print(database)

			print(database.TreeTable)
			print(database.NodesTable)
			print(database.SNPTable)
			print(database.ChromosomesTable)
			print(database.ReferenceTable)

			referencesPath = input("References-file path: ").strip()
			chromosomesPath = input("Chromosomes-file path: ").strip()
			SNPAnnotationsPath = input("SNP-annotations-file path: ").strip()
			NodesPath = input("Nodes-file path: ").strip()
			TreeNodes = input("Tree-file path: ").strip()

			database.commit()
	print("Done!")

main()