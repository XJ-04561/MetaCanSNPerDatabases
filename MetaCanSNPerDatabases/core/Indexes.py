
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
from MetaCanSNPerDatabases.core.Structures import Index
from MetaCanSNPerDatabases.core.Tables import *
from MetaCanSNPerDatabases.core.Columns import *

TreeTableByParent = Index("TreeTableByParent", TreeTable, Parent)
SNPsTableByPosition = Index("SNPsTableByPosition", SNPsTable, Position)
SNPsTableByNodeID = Index("SNPsTableByNodeID", SNPsTable, NodeID)
SNPsTableByChromID = Index("SNPsTableByChromID", SNPsTable, ChromID)
ReferencesTableByGenome = Index("ReferencesTableByGenome", ReferencesTable, Genome)
ReferencesTableByAssembly = Index("ReferencesTableByAssembly", ReferencesTable, Assembly)