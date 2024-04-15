
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
from MetaCanSNPerDatabases.core.Columns import *
from MetaCanSNPerDatabases.core.Tables import *
from MetaCanSNPerDatabases.core.Words import *

class Branch:

	_connection : sqlite3.Connection
	nodeID : int

	def __init__(self, connection : sqlite3.Connection, nodeID : int):
		self._connection = connection
		self.nodeID = nodeID
	
	@property
	def parent(self) -> Self|None:
		try:
			return Branch(self._connection, self._connection.execute(*SELECT(Parent) - FROM(TreeTable) - WHERE(Child == self.nodeID)).fetchone()[0])
		except TypeError:
			return None

	@property
	def children(self) -> Generator[Self,None,None]:
		for (childID,) in self._connection.execute(*SELECT(Child) - FROM(TreeTable) - WHERE(Parent == self.nodeID)):
			yield Branch(self._connection, childID)