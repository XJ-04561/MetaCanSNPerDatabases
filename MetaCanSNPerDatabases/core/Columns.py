
from MetaCanSNPerDatabases.core._Constants import *
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.Structures import Column

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
