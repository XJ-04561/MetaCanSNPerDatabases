

import MetaCanSNPerDatabases.Globals as Globals
from MetaCanSNPerDatabases import Database, Assertion, DatabaseError
from MetaCanSNPerDatabases.core import *

SOFTWARE_NAME = "MetaCanSNPer"

import logging, os
Globals.LOGGER = logging.Logger(f"{SOFTWARE_NAME}Databases", level=logging.WARNING)
LOGGER_FILEHANDLER = logging.FileHandler(f"{SOFTWARE_NAME}Databases.log")
LOGGER_FILEHANDLER.setFormatter(logging.Formatter("[%(name)s] %(asctime)s - %(levelname)s: %(message)s"))
Globals.LOGGER.addHandler(LOGGER_FILEHANDLER)

LEGACY_HASH = "7630f33662e27489b7bb7b3b121ca4ff"
LEGACY_VERSION = 0

Globals.DATABASE_VERSIONS = {
	LEGACY_HASH							: LEGACY_VERSION, # Legacy CanSNPer
	"175c47f1ad61ec81a7d11d8a8e1887ff"	: 2  # MetaCanSNPer Alpha version
}

Globals.CURRENT_VERSION = 2
Globals.CURRENT_TABLES_HASH = ""
Globals.CURRENT_INDEXES_HASH = ""

Globals.SOURCES = [
	"https://github.com/XJ-04561/MetaCanSNPer-data/raw/master/database/{databaseName}", # MetaCanSNPer
	"https://github.com/FOI-Bioinformatics/CanSNPer2-data/raw/master/database/{databaseName}" # Legacy CanSNPer
]

SOURCED = {"refseq":"F", "genbank": "A"}
NCBI_FTP_LINK = "ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GC{source}/{n1}/{n2}/{n3}/{genome_id}_{assembly}/{genome_id}_{assembly}_genomic.fna.gz"


TABLE_NAME_SNP_ANNOTATION	= "snp_annotation"
TABLE_NAME_REFERENCES		= "snp_references"
TABLE_NAME_TREE				= "tree"
TABLE_NAME_CHROMOSOMES		= "chromosomes"

TABLES = [TABLE_NAME_SNP_ANNOTATION, TABLE_NAME_REFERENCES, TABLE_NAME_TREE, TABLE_NAME_CHROMOSOMES]

COLUMN_PARENT,			COLUMN_PARENT_TYPE			= "parent",			"INTEGER"
COLUMN_GENOTYPE,		COLUMN_GENOTYPE_TYPE		= "name",			"TEXT"
COLUMN_NODE_ID,			COLUMN_NODE_ID_TYPE			= "node_id",		"INTEGER"
COLUMN_POSITION,		COLUMN_POSITION_TYPE		= "position",		"INTEGER"
COLUMN_ANCESTRAL,		COLUMN_ANCESTRAL_TYPE		= "ancestral_base", "VARCHAR(1)"
COLUMN_DERIVED,			COLUMN_DERIVED_TYPE			= "derived_base",	"VARCHAR(1)"
COLUMN_REFERENCE,		COLUMN_REFERENCE_TYPE		= "reference",		"VARCHAR(20)"
COLUMN_DATE,			COLUMN_DATE_TYPE			= "date",			"DATETIME"
COLUMN_CHROMOSOME_ID,	COLUMN_CHROMOSOME_ID_TYPE	= "chromosome_id",	"INTEGER"
COLUMN_CHROMOSOME,		COLUMN_CHROMOSOME_TYPE		= "chromosome",		"VARCHAR(30)"
COLUMN_GENOME_ID,		COLUMN_GENOME_ID_TYPE		= "genome_id",		"INTEGER"
COLUMN_GENOME,			COLUMN_GENOME_TYPE			= "genome",			"VARCHAR(30)"
COLUMN_STRAIN,			COLUMN_STRAIN_TYPE			= "strain",			"VARCHAR(30)"
COLUMN_GENBANK,			COLUMN_GENBANK_TYPE			= "genbank_id",		"VARCHAR(30)"
COLUMN_REFSEQ,			COLUMN_REFSEQ_TYPE			= "refseq_id",		"VARCHAR(30)"
COLUMN_ASSEMBLY,		COLUMN_ASSEMBLY_TYPE		= "assembly_name",	"VARCHAR(30)"


TREE_APPEND = [
    f"PRIMARY KEY {COLUMN_NODE_ID}",
	f"UNIQUE ({COLUMN_PARENT}, {COLUMN_NODE_ID})",
    f"UNIQUE {COLUMN_NODE_ID}",
    f"NOT NULL {COLUMN_PARENT}",
    f"NOT NULL {COLUMN_GENOTYPE}"
]
SNP_APPEND = [
    f"PRIMARY KEY ({COLUMN_CHROMOSOME_ID}, {COLUMN_POSITION}, {COLUMN_NODE_ID})",
	f"FOREIGN KEY ({COLUMN_CHROMOSOME_ID}) REFERENCES {TABLE_NAME_CHROMOSOMES} ({COLUMN_CHROMOSOME_ID})",
    f"FOREIGN KEY ({COLUMN_NODE_ID}) REFERENCES {TABLE_NAME_TREE} ({COLUMN_NODE_ID})",
    f"UNIQUE {COLUMN_POSITION}"
]
CHROMOSOMES_APPEND = [
	f"PRIMARY KEY ({COLUMN_CHROMOSOME_ID}, {COLUMN_GENOME_ID})",
	f"FOREIGN KEY ({COLUMN_GENOME_ID}) REFERENCES {TABLE_NAME_REFERENCES} ({COLUMN_GENOME_ID})",
    f"UNIQUE {COLUMN_CHROMOSOME_ID}",
    f"NOT NULL {COLUMN_CHROMOSOME}",
    f"NOT NULL {COLUMN_GENOME_ID}"
]
REFERENCE_APPEND = [
    f"PRIMARY KEY {COLUMN_GENOME_ID}",
    f"UNIQUE {COLUMN_GENBANK}",
    f"UNIQUE {COLUMN_REFSEQ}",
    f"UNIQUE {COLUMN_ASSEMBLY}"
]

"""Columns"""

Parent			= Column("Parent",			COLUMN_PARENT,			COLUMN_PARENT_TYPE)
NodeID = Child	= Column("NodeID",			COLUMN_NODE_ID,			COLUMN_GENOTYPE_TYPE)
GenoType		= Column("GenoType",		COLUMN_GENOTYPE,		COLUMN_NODE_ID_TYPE)
Position		= Column("Position",		COLUMN_POSITION,		COLUMN_POSITION_TYPE)
Ancestral		= Column("Ancestral",		COLUMN_ANCESTRAL,		COLUMN_ANCESTRAL_TYPE)
Derived			= Column("Derived",			COLUMN_DERIVED,			COLUMN_DERIVED_TYPE)
SNPReference	= Column("SNPReference",	COLUMN_REFERENCE,		COLUMN_REFERENCE_TYPE)
Date			= Column("Date",			COLUMN_DATE,			COLUMN_DATE_TYPE)
ChromID			= Column("ChromID",			COLUMN_CHROMOSOME_ID,	COLUMN_CHROMOSOME_ID_TYPE)
Chromosome		= Column("Chromosome",		COLUMN_CHROMOSOME,		COLUMN_CHROMOSOME_TYPE)
GenomeID		= Column("GenomeID",		COLUMN_GENOME_ID,		COLUMN_GENOME_ID_TYPE)
Genome			= Column("Genome",			COLUMN_GENOME,			COLUMN_GENOME_TYPE)
Strain			= Column("Strain",			COLUMN_STRAIN,			COLUMN_STRAIN_TYPE)
GenbankID		= Column("GenbankID",		COLUMN_GENBANK,			COLUMN_GENBANK_TYPE)
RefseqID		= Column("RefseqID",		COLUMN_REFSEQ,			COLUMN_REFSEQ_TYPE)
Assembly		= Column("Assembly",		COLUMN_ASSEMBLY,		COLUMN_ASSEMBLY_TYPE)

"""Tables"""

TreeTable = Table("TreeTable", TABLE_NAME_TREE, (Parent, NodeID, GenoType), (UNIQUE(Parent, Child), PRIMARY - KEY(Child)))
ReferencesTable = Table("ReferencesTable", TABLE_NAME_REFERENCES, (GenomeID, Genome, Strain, GenbankID, RefseqID, Assembly), (PRIMARY - KEY(GenomeID)))
ChromosomesTable = Table("ChromosomesTable", TABLE_NAME_CHROMOSOMES, (ChromID, Chromosome, GenomeID), (PRIMARY - KEY(ChromID), FOREIGN - KEY(GenomeID) - REFERENCES(ReferencesTable)))
SNPsTable = Table("SNPsTable", TABLE_NAME_SNP_ANNOTATION, (NodeID, Position, Ancestral, Derived, SNPReference, Date, ChromID), (PRIMARY - KEY(Position), FOREIGN - KEY(ChromID) - REFERENCES(ChromosomesTable), FOREIGN - KEY(NodeID) - REFERENCES(TreeTable)))

"""Indexes"""

TreeTableByParent = Index("TreeTableByParent", TreeTable, Parent)
SNPsTableByPosition = Index("SNPsTableByPosition", SNPsTable, Position)
SNPsTableByNodeID = Index("SNPsTableByNodeID", SNPsTable, NodeID)
SNPsTableByChromID = Index("SNPsTableByChromID", SNPsTable, ChromID)
ReferencesTableByGenome = Index("ReferencesTableByGenome", ReferencesTable, Genome)
ReferencesTableByAssembly = Index("ReferencesTableByAssembly", ReferencesTable, Assembly)

class CanSNPDatabaseError(DatabaseError): pass
class IsLegacyCanSNPer2(CanSNPDatabaseError): pass
class NoTreeConnectedToRoot(CanSNPDatabaseError): pass

class LegacyObjects:
	
	Parent			= Column("Parent",			"parent")
	Child			= Column("Child",			"child")
	NodeID_Nod		= Column("NodeID",			"id")
	NodeID_SNP		= Column("NodeID",			"node_id")
	Name			= Column("Name",			"name")
	SNPID			= Column("SNPID",			"snp_id")
	Position		= Column("Position",		"position")
	Ancestral		= Column("Ancestral",		"ancestral_base")
	Derived			= Column("Derived",			"derived_base")
	SNPReference	= Column("SNPReference",	"reference")
	Date			= Column("Date",			"date")
	GenomeID_SNP	= Column("GenomeID",		"genome_i")
	GenomeID_Ref	= Column("GenomeID",		"id")
	Genome			= Column("Genome",			"genome")
	Strain			= Column("Strain",			"strain")
	GenbankID		= Column("GenbankID",		"genbank_id")
	RefseqID		= Column("RefseqID",		"refseq_id")
	Assembly		= Column("Assembly",		"assembly_name")

	GenomesTable	= Table("GenomesTable",		"genomes", )
	NodesTable		= Table("Nodes",			"nodes")
	RankTable		= Table("Rank",				"rank")
	ReferencesTable	= Table("ReferencesTable",	"snp_references")
	SNPsTable		= Table("SNPsTable",		"snp_annotation")
	TreeTable		= Table("TreeTable",		"tree")

class NotLegacyCanSNPer2(Assertion):
	legacyObjects = {}
	@classmethod
	def exception(self, database : Database) -> Exception:
		return IsLegacyCanSNPer2("Database is Legacy CanSNPer2 schema.")
	@classmethod
	def condition(self, database : Database) -> bool:
		return database.tablesHash == LEGACY_HASH
	@classmethod
	def rectify(self, database : Database) -> None:
		import ncbi.datasets as datasets # type: ignore
		
		if database.refDir is None:
			refDir = CommonGroups.shared / f"{SOFTWARE_NAME}-Data" / "References" / pName(database.filename)
		else:
			refDir = Path(database.refDir)

		tempTable = TempTable()
		LO = LegacyObjects
		
		# References
		LOGGER.info("Updating 'References'-table")
		database(BEGIN - TRANSACTION)
		database(ALTER - TABLE (LO.ReferencesTable) - RENAME - TO - Table(tempTable))
		database(CREATE - TABLE - sql(ReferencesTable))
		database(INSERT - INTO - ReferencesTable - SELECT * FROM (tempTable))
		database(DROP - TABLE (tempTable))
		database(COMMIT)

		# Chromosomes
		LOGGER.info("Updating 'Chromosomes'-table")
		database(BEGIN - TRANSACTION)
		database(CREATE - TABLE - sql(ChromosomesTable) )
		GenomeAPI = datasets.GenomeApi()
		for i, genbankID, assembly in database(SELECT (LO.GenomeID_Ref, LO.GenbankID, LO.Assembly) - FROM (LO.ReferencesTable)):
			try:
				chromosome = GenomeAPI.assembly_descriptors_by_accessions([genbankID])["assemblies"][0]["assembly"]["biosample"]["sample_ids"][0]["value"]
				assert len(chromosome) != 0, "No genbank entry found"
				database(INSERT - INTO - ChromosomesTable - VALUES (i, chromosome, i))
			except (AssertionError, KeyError) as e:
				LOGGER.exception(e)
				try:
					chromosome = open(refDir.find(f"{assembly}.fna"), "r").readline()[1:].split()[0]
				except FileNotFoundError:
					LOGGER.warning(f"Couldn't find genome with {genbankID=} either online or in {refDir!r}.")
					raise UnableToDefineChromosomes(f"Could not find naming for chromosomes in entry with {i=}, {genbankID=}, and {assembly=}.")
			finally:
				database(INSERT - INTO - ChromosomesTable - VALUES (i, chromosome, i))
					
		database(COMMIT)
		
		# SNPs
		LOGGER.info("Updating 'SNP'-table")
		database(BEGIN - TRANSACTION)
		database(ALTER - TABLE - "snp_annotation" - RENAME - TO - "snp_annotation_old")
		database(CREATE - TABLE - sql(SNPsTable))
		database(INSERT - INTO - SNPsTable - f"({COLUMN_NODE_ID}, {COLUMN_POSITION}, {COLUMN_ANCESTRAL}, {COLUMN_DERIVED}, {COLUMN_REFERENCE}, {COLUMN_DATE}, {COLUMN_CHROMOSOME_ID})" - SELECT ("node_id-1", "position", "ancestral_base", "derived_base", "reference", "date", "genome_i") - FROM - "snp_annotation_old")
		database(DROP - TABLE - "snp_annotation_old")
		database(COMMIT)
		
		# Tree
		LOGGER.info("Updating 'Tree'-table")
		database(BEGIN - TRANSACTION)
		database(ALTER - TABLE - TreeTable - RENAME - TO - "tree_old")
		database(COMMIT)
		database(BEGIN - TRANSACTION)
		database(CREATE - TABLE - sql(TreeTable))
		database(COMMIT)
		database(BEGIN - TRANSACTION)
		database(INSERT - INTO - TreeTable - f"({COLUMN_PARENT}, {COLUMN_NODE_ID}, {COLUMN_GENOTYPE})" - SELECT ("tree_old.parent-1", "null", "nodes.name") - FROM ("tree_old", "nodes") - WHERE (Comparison("tree_old.child", "==", "nodes.id"), Comparison("tree_old.child", ">", 1)) - ORDER - BY - "tree_old.child ASC")
		database(COMMIT)
		database(BEGIN - TRANSACTION)
		database(UPDATE - TreeTable - SET (Parent==0) - WHERE (NodeID == 1))
		database(DROP - TABLE - "nodes")
		database(DROP - TABLE - "tree_old")

		LOGGER.info("Dropping 'genomes'- and 'rank'-tables")

		database(DROP - TABLE - "genomes")
		database(DROP - TABLE - "rank")
		
		for index in database.indexes:
			database.createIndex(index)

		database(PRAGMA (user_version = CURRENT_VERSION))
		database(COMMIT)

class CanSNPNode(Branch):
	table : Table = TreeTable
	parentCol : Column = Parent
	childCol : Column = Child

class MetaCanSNPerDatabase(Database):
	assertions = [NotLegacyCanSNPer2] + Globals.ASSERTIONS
	refDir : str
	def __init__(self, *args, refDir : str=None, **kwargs):
		self.refDir = refDir or os.path.realpath(".")
		super().__init__(*args, **kwargs)
	
	@property
	def tree(self):
		try:
			return CanSNPNode(self, next(self()))
		except StopIteration:
			raise NoTreeConnectedToRoot(f"In database {self.filename!r}")


def loadFromReferenceFile(database : Database, file : TextIO, refDir : str="."):
	file.seek(0)
	if "genome	strain	genbank_id	refseq_id	assembly_name" == file.readline():
		for row in file:
			genome, strain, genbank_id, refseq_id, assembly_name = row.strip().split("\t")
			database(INSERT - INTO - ReferencesTable - VALUES (genome, strain, genbank_id, refseq_id, assembly_name))
			genomeID = next(database(SELECT (GenomeID) - FROM (ReferencesTable) - WHERE (Genome == genome)))
			try:
				chrom = open(os.path.join(refDir, f"{assembly_name}.fna"), "r").readline()[1:].split()[0]
				database(INSERT - INTO - ChromosomesTable - VALUES (None, chrom, genomeID))
			except FileNotFoundError as e:
				raise MissingReferenceFile(f"Could not find reference file {os.path.join(refDir, f'{assembly_name}.fna')!r}. The file {file.__name__!r} does not specify chromosomes, and so the reference fasta file is required. To set the directory in which to look for .fna references, use the flag '--refDir'")

	elif "chromosomes	genome	strain	genbank_id	refseq_id	assembly_name" == file.readline():
		for row in file:
			chromosomes, genome, strain, genbank_id, refseq_id, assembly_name = row.strip().split("\t")
			database(INSERT - INTO - ReferencesTable - VALUES (genome, strain, genbank_id, refseq_id, assembly_name))
			genomeID = next(database(SELECT (GenomeID) - FROM (ReferencesTable) - WHERE (Genome == genome)))
			for chrom in chromosomes.split(";"):
				database(INSERT - INTO - ChromosomesTable - VALUES (None, chrom, genomeID))
	else:
		ValueError("File is not of accepted format.")

def loadFromTreeFile(database : Database, file : TextIO):
	file.seek(0)
	database(INSERT - INTO - TreeTable - VALUES (0, None, file.readline().strip()))
	for row in file:
		*_, parent, child = row.rstrip(file.newlines).rsplit("\t", 2)
		database(INSERT - INTO - TreeTable - VALUES (SELECT (Child) - FROM (TreeTable) - WHERE (GenoType == parent), None, child))

def loadFromSNPFile(database : Database, file : TextIO):
	file.seek(0)
	if "snp_id	strain	reference	genome	position	derived_base	ancestral_base" == file.readline().strip():
		for row in file:
			nodeName, strain, reference, genome, position, ancestral, derived = row.rstrip(file.newlines).split("\t")
			database(INSERT - INTO - SNPsTable - VALUES (SELECT (Child) - FROM (TreeTable) - WHERE (GenoType == nodeName), position, ancestral, derived, reference, None, SELECT (ChromID) - FROM (ChromosomesTable) - WHERE (Genome == genome)))
	else:
		ValueError("File is not of accepted format.")