
from MetaCanSNPerDatabases.modules._Constants import *


class ColumnFlag(int): pass

ALL				= ColumnFlag(0)
TreeParent		= ColumnFlag(1)
TreeChild		= ColumnFlag(2)
NodeID			= ColumnFlag(3)
GenoType		= ColumnFlag(4)
SnpID			= ColumnFlag(5)
Position		= ColumnFlag(6)
Ancestral		= ColumnFlag(7)
Derived			= ColumnFlag(8)
SNPReference	= ColumnFlag(9)
Date			= ColumnFlag(10)
ChromID			= ColumnFlag(11)
Chromosome		= ColumnFlag(12)
GenomeID		= ColumnFlag(13)
Genome			= ColumnFlag(14)
Strain			= ColumnFlag(15)
GenbankID		= ColumnFlag(16)
RefseqID		= ColumnFlag(17)
Assembly		= ColumnFlag(18)

NAMES = [
	ALL,
	TreeParent,
	TreeChild,
	NodeID,
	GenoType,
	SnpID,
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

LOOKUP : dict[str,dict[ColumnFlag, str]] = {
    TABLE_NAME_TREE : {
        ALL			: "*",
        TreeParent	: TREE_COLUMN_PARENT,
		TreeChild	: TREE_COLUMN_CHILD
	},
	TABLE_NAME_NODES : {
        ALL			: "*",
        NodeID		: NODE_COLUMN_ID,
        GenoType	: NODE_COLUMN_NAME
	},
    TABLE_NAME_SNP_ANNOTATION : {
        ALL				: "*",
		SnpID			: SNP_COLUMN_SNP_ID,
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

UNIQUES = set(LOOKUP[TABLE_NAME_NODES]).difference(LOOKUP[TABLE_NAME_REFERENCES], LOOKUP[TABLE_NAME_SNP_ANNOTATION], LOOKUP[TABLE_NAME_TREE])
UNIQUELOOKUP = {
    col : [table for table in LOOKUP if col in LOOKUP[table]][0] for col in UNIQUES
}
COMMONS = set(LOOKUP[TABLE_NAME_NODES]).intersection(LOOKUP[TABLE_NAME_REFERENCES], LOOKUP[TABLE_NAME_SNP_ANNOTATION], LOOKUP[TABLE_NAME_TREE])
COMMONLOOKUP = {
    col : {table for table in LOOKUP if col in LOOKUP[table]} for col in COMMONS
}

RELATIONS : dict[tuple[str,str],ColumnFlag] = {
	(TABLE_NAME_NODES, TABLE_NAME_TREE) : NodeID,
    (TABLE_NAME_TREE, TABLE_NAME_NODES) : NodeID,
    
    (TABLE_NAME_NODES, TABLE_NAME_SNP_ANNOTATION) : NodeID,
    (TABLE_NAME_SNP_ANNOTATION, TABLE_NAME_NODES) : NodeID,
    
	(TABLE_NAME_CHROMOSOMES, TABLE_NAME_SNP_ANNOTATION) : ChromID,
    (TABLE_NAME_SNP_ANNOTATION, TABLE_NAME_CHROMOSOMES) : ChromID,
    
	(TABLE_NAME_REFERENCES, TABLE_NAME_CHROMOSOMES) : GenomeID,
    (TABLE_NAME_CHROMOSOMES, TABLE_NAME_REFERENCES) : GenomeID,
    
	(TABLE_NAME_SNP_ANNOTATION, TABLE_NAME_TREE) : NodeID,
    (TABLE_NAME_TREE, TABLE_NAME_SNP_ANNOTATION) : NodeID
}

RELATIONSHIPS : dict[str,dict[ColumnFlag, str]]= {
	TABLE_NAME_NODES : {
		SnpID			: TABLE_NAME_SNP_ANNOTATION,
		Position		: TABLE_NAME_SNP_ANNOTATION,
		Ancestral		: TABLE_NAME_SNP_ANNOTATION,
		Derived			: TABLE_NAME_SNP_ANNOTATION,
		SNPReference	: TABLE_NAME_SNP_ANNOTATION,
		Date			: TABLE_NAME_SNP_ANNOTATION,
		ChromID			: TABLE_NAME_SNP_ANNOTATION,
        
		TreeParent		: TABLE_NAME_TREE,
		TreeChild		: TABLE_NAME_TREE
	},
    TABLE_NAME_SNP_ANNOTATION : {
        Chromosome		: TABLE_NAME_CHROMOSOMES,
		GenomeID		: TABLE_NAME_CHROMOSOMES,
        
		GenoType		: TABLE_NAME_NODES,
        
		TreeParent		: TABLE_NAME_TREE,
		TreeChild		: TABLE_NAME_TREE
	},
    TABLE_NAME_CHROMOSOMES : {
        NodeID			: TABLE_NAME_SNP_ANNOTATION,
        SnpID			: TABLE_NAME_SNP_ANNOTATION,
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
		Chromosome		: TABLE_NAME_CHROMOSOMES
	}
}