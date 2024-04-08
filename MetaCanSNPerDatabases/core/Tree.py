
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
import MetaCanSNPerDatabases.core.Columns as Columns
from MetaCanSNPerDatabases.core.Columns import Column
from MetaCanSNPerDatabases.core._Constants import *

class Branch:

	_connection : sqlite3.Connection
	nodeID : int
	parameters = [
		COLUMN_NODE_ID,
		TABLE_NAME_TREE,
		COLUMN_PARENT
	]

	def __init__(self, connection : sqlite3.Connection, nodeID : int):
		self._connection = connection
		self.nodeID = nodeID
	
	@property
	def parent(self) -> Self|None:
		try:
			return Branch(self._connection, self._connection.execute(f"SELECT {COLUMN_PARENT} FROM {TABLE_NAME_TREE} WHERE {COLUMN_NODE_ID} = ?", [self.nodeID]).fetchone()[0])
		except TypeError:
			return None

	@property
	def children(self) -> Generator[Self,None,None]:
		for (childID,) in self._connection.execute(f"SELECT {COLUMN_NODE_ID} FROM {TABLE_NAME_TREE} WHERE {COLUMN_PARENT} = ?", [self.nodeID]):
			yield Branch(self._connection, childID)