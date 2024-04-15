
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.Structures import Table
from MetaCanSNPerDatabases.core.Columns import *
from MetaCanSNPerDatabases.core.Words import *


TreeTable = Table("TreeTable", TABLE_NAME_TREE, (Parent, NodeID, GenoType), (UNIQUE(Parent, Child), PRIMARY - KEY(Child)))
ReferencesTable = Table("ReferencesTable", TABLE_NAME_REFERENCES, (GenomeID, Genome, Strain, GenbankID, RefseqID, Assembly), (PRIMARY - KEY(GenomeID)))
ChromosomesTable = Table("ChromosomesTable", TABLE_NAME_CHROMOSOMES, (ChromID, Chromosome, GenomeID), (PRIMARY - KEY(ChromID), FOREIGN - KEY(GenomeID) - REFERENCES(ReferencesTable)))
SNPsTable = Table("SNPsTable", TABLE_NAME_SNP_ANNOTATION, (NodeID, Position, Ancestral, Derived, SNPReference, Date, ChromID), (PRIMARY - KEY(Position), FOREIGN - KEY(ChromID) - REFERENCES(ChromosomesTable), FOREIGN - KEY(NodeID) - REFERENCES(TreeTable)))
