

from typeguard import install_import_hook

install_import_hook("MetaCanSNPerDatabases.modules.Globals")
import MetaCanSNPerDatabases.modules.Globals as Globals

install_import_hook("MetaCanSNPerDatabases.modules.Databases")
from MetaCanSNPerDatabases.modules.Databases import openDatabase, DatabaseReader, DatabaseWriter, IsLegacyCanSNPer2, OutdatedCanSNPerDatabase
from sqlite3 import DatabaseError

install_import_hook("MetaCanSNPerDatabases.modules.Columns")
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
import MetaCanSNPerDatabases.modules.Columns as Columns

install_import_hook("MetaCanSNPerDatabases.modules.Functions")
from MetaCanSNPerDatabases.modules.Functions import downloadDatabase

install_import_hook("MetaCanSNPerDatabases.modules.Tables")
from MetaCanSNPerDatabases.modules.Tables import SNPTable, ReferenceTable, TreeTable

install_import_hook("MetaCanSNPerDatabases.modules.Tree")
from MetaCanSNPerDatabases.modules.Tree import Branch

install_import_hook("MetaCanSNPerDatabases.modules.Test")
import MetaCanSNPerDatabases.modules.Test as Test
