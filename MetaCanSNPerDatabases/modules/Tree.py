
from MetaCanSNPerDatabases.modules.Globals import *
import MetaCanSNPerDatabases.modules.Globals as Globals
import MetaCanSNPerDatabases.modules.Columns as Columns
from MetaCanSNPerDatabases.modules.Columns import ColumnFlag
from MetaCanSNPerDatabases.modules._Constants import *

class Branch:

	_connection : sqlite3.Connection
	nodeID : int
	parameters = [
		TREE_COLUMN_CHILD,
		TABLE_NAME_TREE,
		TREE_COLUMN_PARENT
	]

	def __init__(self, connection : sqlite3.Connection, nodeID : int):
		self._connection = connection
		self.nodeID = nodeID
	
	@property
	def children(self) -> Generator[Self,None,None]:
		for (childID,) in self._connection.execute(f"SELECT {TREE_COLUMN_CHILD} FROM {TABLE_NAME_TREE} WHERE {TREE_COLUMN_PARENT} = ?", [self.nodeID]):
			if childID == self.nodeID:
				continue
			yield Branch(self._connection, childID)