
from MetaCanSNPerDatabases import *

def main():
	mode = input("Create or read from database? [w/r]: ").strip()

	match mode:
		case "r":
			database = openDatabase(input("Database path: ").strip(), "r")

			print(database.SNPTable)
			print(database.ReferenceTable)
			print(database.NodeTable)
			print(database.TreeTable)
			print(database.RankTable)
			print(database.GenomesTable)
		case "w":
			database = openDatabase(input("Database path: ").strip(), "w")

			print(database.SNPTable)
			print(database.ReferenceTable)
			print(database.NodeTable)
			print(database.TreeTable)
			print(database.RankTable)
			print(database.GenomesTable)

			genomesPath = input("Genomes-file path: ").strip()
			referencesPath = input("References-file path: ").strip()
			SNPAnnotationsPath = input("SNP-annotations-file path: ").strip()
			NodesPath = input("Nodes-file path: ").strip()
			TreeNodes = input("Tree-file path: ").strip()

			database.commit()
	print("Done!")