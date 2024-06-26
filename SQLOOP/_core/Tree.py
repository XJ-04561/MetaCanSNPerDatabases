
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from SQLOOP._core.Structures import *
from SQLOOP._core.Words import *
from SQLOOP._core.Databases import *

class Branch:

	database : "Database"
	table : "Table"
	parentCol : "Column"
	childCol : "Column"
	node : Any

	def __init__(self, database : "Database", node : Any):
		self.database = database
		self.node = node
	
	@property
	def parent(self) -> Union["Branch", None]:
		try:
			return self.__class__(self.database, self.database[self.parentCol, self.table, self.childCol == self.node])
		except StopIteration:
			return None

	@property
	def children(self) -> Generator["Branch",None,None]:
		for childNode in self.database[self.childCol, self.table, self.parentCol == self.node]:
			yield self.__class__(self.database, childNode)