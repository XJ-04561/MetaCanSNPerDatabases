
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
import MetaCanSNPerDatabases.core.Columns as Columns
from MetaCanSNPerDatabases.core.Columns import *
from MetaCanSNPerDatabases.core._Constants import *
from MetaCanSNPerDatabases.core.Indexes import Index



TreeTable = Table(name = TABLE_NAME_TREE, column = [Parent, NodeID, GenoType], appendRows = TREE_APPEND)
TreeTable.addIndex(Parent)

SNPsTable = Table(TABLE_NAME_SNP_ANNOTATION, [NodeID, Position, Ancestral, Derived, SNPReference, Date, ChromID], SNP_APPEND)
SNPsTable.addIndex(Position)
SNPsTable.addIndex(ChromID)
SNPsTable.addIndex(NodeID)

ChromosomesTable = Table(TABLE_NAME_CHROMOSOMES, [ChromID, Chromosome, GenomeID], CHROMOSOMES_APPEND)

ReferencesTable = Table(TABLE_NAME_REFERENCES, [GenomeID, Genome, Strain, GenbankID, RefseqID, Assembly], REFERENCE_APPEND)
ReferencesTable.addIndex(Genome)
ReferencesTable.addIndex(Assembly)


class Tables: pass
Tables = Literal[SNPsTable, ReferencesTable, TreeTable, ChromosomesTable] # type: ignore