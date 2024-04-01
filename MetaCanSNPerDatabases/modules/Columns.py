
from MetaCanSNPerDatabases.modules._Constants import *


class ColumnFlag(int): pass

ALL				= ColumnFlag(0)
TreeParent		= ColumnFlag(1)
TreeChild		= ColumnFlag(2)
GenoType		= ColumnFlag(3)
NodeID			= ColumnFlag(4)
Position		= ColumnFlag(5)
Ancestral		= ColumnFlag(6)
Derived			= ColumnFlag(7)
SNPReference	= ColumnFlag(8)
Date			= ColumnFlag(9)
ChromID			= ColumnFlag(10)
Chromosome		= ColumnFlag(11)
GenomeID		= ColumnFlag(12)
Genome			= ColumnFlag(13)
Strain			= ColumnFlag(14)
GenbankID		= ColumnFlag(15)
RefseqID		= ColumnFlag(16)
Assembly		= ColumnFlag(17)

NAMES = [
	ALL,
	TreeParent,
	TreeChild,
	GenoType,
	NodeID,
	Position,
	Ancestral,
	Derived,
	SNPReference,
	Date,
	ChromID,
	Chromosome,
	GenomeID,
	Genome,
	Strain,
	GenbankID,
	RefseqID,
	Assembly
]

NAMES_STRING = [
	"ALL",
	"TreeParent",
	"TreeChild",
	"GenoType",
	"NodeID",
	"Position",
	"Ancestral",
	"Derived",
	"SNPReference",
	"Date",
	"ChromID",
	"Chromosome",
	"GenomeID",
	"Genome",
	"Strain",
	"GenbankID",
	"RefseqID",
	"Assembly"
]

NAMES_DICT = {
	"ALL"			: ALL,
	"TreeParent"	: TreeParent,
	"TreeChild"		: TreeChild,
	"GenoType"		: GenoType,
	"NodeID"		: NodeID,
	"Position"		: Position,
	"Ancestral"		: Ancestral,
	"Derived"		: Derived,
	"SNPReference"	: SNPReference,
	"Date"			: Date,
	"ChromID"		: ChromID,
	"Chromosome"	: Chromosome,
	"GenomeID"		: GenomeID,
	"Genome"		: Genome,
	"Strain"		: Strain,
	"GenbankID"		: GenbankID,
	"RefseqID"		: RefseqID,
	"Assembly"		: Assembly
}

TYPE_LOOKUP = {
    TreeParent		:TREE_COLUMN_PARENT_TYPE,
	TreeChild		:TREE_COLUMN_CHILD_TYPE,
	GenoType		:TREE_COLUMN_NAME_TYPE,
	NodeID			:SNP_COLUMN_NODE_ID_TYPE,
	Position		:SNP_COLUMN_POSITION_TYPE,
	Ancestral		:SNP_COLUMN_ANCESTRAL_TYPE,
	Derived			:SNP_COLUMN_DERIVED_TYPE,
	SNPReference	:SNP_COLUMN_REFERENCE_TYPE,
	Date			:SNP_COLUMN_DATE_TYPE,
	ChromID			:CHROMOSOMES_COLUMN_ID_TYPE,
	Chromosome		:CHROMOSOMES_COLUMN_NAME_TYPE,
	GenomeID		:REFERENCE_COLUMN_GENOME_ID_TYPE,
	Genome			:REFERENCE_COLUMN_GENOME_TYPE,
	Strain			:REFERENCE_COLUMN_STRAIN_TYPE,
	GenbankID		:REFERENCE_COLUMN_GENBANK_TYPE,
	RefseqID		:REFERENCE_COLUMN_REFSEQ_TYPE,
	Assembly		:REFERENCE_COLUMN_ASSEMBLY_TYPE
}

LOOKUP : dict[str,dict[ColumnFlag, str]] = {
    TABLE_NAME_TREE : {
        ALL			: "*",
        TreeParent	: TREE_COLUMN_PARENT,
		TreeChild	: TREE_COLUMN_CHILD,
        NodeID		: TREE_COLUMN_CHILD,
        GenoType	: TREE_COLUMN_NAME
	},
    TABLE_NAME_SNP_ANNOTATION : {
        ALL				: "*",
        NodeID			: SNP_COLUMN_NODE_ID,
		Position		: SNP_COLUMN_POSITION,
		Ancestral		: SNP_COLUMN_ANCESTRAL,
		Derived			: SNP_COLUMN_DERIVED,
		SNPReference	: SNP_COLUMN_REFERENCE,
		Date			: SNP_COLUMN_DATE,
		ChromID			: SNP_COLUMN_CHROMOSOMES_ID
	},
	TABLE_NAME_CHROMOSOMES : {
        ALL			: "*",
        ChromID		: CHROMOSOMES_COLUMN_ID,
        Chromosome	: CHROMOSOMES_COLUMN_NAME,
        GenomeID	: CHROMOSOMES_COLUMN_GENOME_ID
	},
    TABLE_NAME_REFERENCES : {
        ALL			: "*",
        GenomeID	: REFERENCE_COLUMN_GENOME_ID,
		Genome		: REFERENCE_COLUMN_GENOME,
		Strain		: REFERENCE_COLUMN_STRAIN,
		GenbankID	: REFERENCE_COLUMN_GENBANK,
		RefseqID	: REFERENCE_COLUMN_REFSEQ,
		Assembly	: REFERENCE_COLUMN_ASSEMBLY
	}
}

UNIQUES = set(LOOKUP[TABLE_NAME_REFERENCES]).difference(LOOKUP[TABLE_NAME_CHROMOSOMES], LOOKUP[TABLE_NAME_SNP_ANNOTATION], LOOKUP[TABLE_NAME_TREE])
UNIQUELOOKUP = {
    col : [table for table in LOOKUP if col in LOOKUP[table]][0] for col in UNIQUES
}
COMMONS = set(LOOKUP[TABLE_NAME_REFERENCES]).intersection(LOOKUP[TABLE_NAME_CHROMOSOMES], LOOKUP[TABLE_NAME_SNP_ANNOTATION], LOOKUP[TABLE_NAME_TREE])
COMMONLOOKUP = {
    col : {table for table in LOOKUP if col in LOOKUP[table]} for col in COMMONS
}

RELATIONS : dict[tuple[str,str],ColumnFlag] = {
    #	FROM			->			TO
    (TABLE_NAME_TREE, TABLE_NAME_SNP_ANNOTATION) : NodeID,
    (TABLE_NAME_SNP_ANNOTATION, TABLE_NAME_TREE) : NodeID,
    
	(TABLE_NAME_CHROMOSOMES, TABLE_NAME_SNP_ANNOTATION) : ChromID,
    (TABLE_NAME_SNP_ANNOTATION, TABLE_NAME_CHROMOSOMES) : ChromID,
    
	(TABLE_NAME_REFERENCES, TABLE_NAME_CHROMOSOMES) : GenomeID,
    (TABLE_NAME_CHROMOSOMES, TABLE_NAME_REFERENCES) : GenomeID,
    
	(TABLE_NAME_REFERENCES, TABLE_NAME_SNP_ANNOTATION) : GenomeID,
    (TABLE_NAME_SNP_ANNOTATION, TABLE_NAME_REFERENCES) : ChromID,
}

RELATIONSHIPS : dict[str,dict[ColumnFlag, str]]= {
	TABLE_NAME_TREE : {
		Position		: TABLE_NAME_SNP_ANNOTATION,
		Ancestral		: TABLE_NAME_SNP_ANNOTATION,
		Derived			: TABLE_NAME_SNP_ANNOTATION,
		SNPReference	: TABLE_NAME_SNP_ANNOTATION,
		Date			: TABLE_NAME_SNP_ANNOTATION,
		ChromID			: TABLE_NAME_SNP_ANNOTATION,
        
        Chromosome		: TABLE_NAME_SNP_ANNOTATION,
		GenomeID		: TABLE_NAME_SNP_ANNOTATION,

			Genome		: TABLE_NAME_SNP_ANNOTATION,
			Strain		: TABLE_NAME_SNP_ANNOTATION,
			GenbankID	: TABLE_NAME_SNP_ANNOTATION,
			RefseqID	: TABLE_NAME_SNP_ANNOTATION,
			Assembly	: TABLE_NAME_SNP_ANNOTATION
	},
    TABLE_NAME_SNP_ANNOTATION : {
		GenoType		: TABLE_NAME_TREE,
		TreeParent		: TABLE_NAME_TREE,
		TreeChild		: TABLE_NAME_TREE,
        
        Chromosome		: TABLE_NAME_CHROMOSOMES,
		GenomeID		: TABLE_NAME_CHROMOSOMES,
        
		Genome			: TABLE_NAME_CHROMOSOMES,
		Strain			: TABLE_NAME_CHROMOSOMES,
		GenbankID		: TABLE_NAME_CHROMOSOMES,
		RefseqID		: TABLE_NAME_CHROMOSOMES,
		Assembly		: TABLE_NAME_CHROMOSOMES,
	},
    TABLE_NAME_CHROMOSOMES : {
        NodeID			: TABLE_NAME_SNP_ANNOTATION,
		Position		: TABLE_NAME_SNP_ANNOTATION,
		Ancestral		: TABLE_NAME_SNP_ANNOTATION,
		Derived			: TABLE_NAME_SNP_ANNOTATION,
		SNPReference	: TABLE_NAME_SNP_ANNOTATION,
		Date			: TABLE_NAME_SNP_ANNOTATION,
        
		Genome		: TABLE_NAME_REFERENCES,
		Strain		: TABLE_NAME_REFERENCES,
		GenbankID	: TABLE_NAME_REFERENCES,
		RefseqID	: TABLE_NAME_REFERENCES,
		Assembly	: TABLE_NAME_REFERENCES
	},
    TABLE_NAME_REFERENCES : {
        ChromID			: TABLE_NAME_CHROMOSOMES,
		Chromosome		: TABLE_NAME_CHROMOSOMES,
        
        NodeID			: TABLE_NAME_CHROMOSOMES,
		Position		: TABLE_NAME_CHROMOSOMES,
		Ancestral		: TABLE_NAME_CHROMOSOMES,
		Derived			: TABLE_NAME_CHROMOSOMES,
		SNPReference	: TABLE_NAME_CHROMOSOMES,
		Date			: TABLE_NAME_CHROMOSOMES,
		GenoType		: TABLE_NAME_CHROMOSOMES,
		TreeParent		: TABLE_NAME_CHROMOSOMES,
		TreeChild		: TABLE_NAME_CHROMOSOMES,
	}
}