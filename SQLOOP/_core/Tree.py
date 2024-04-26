
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals
from SQLOOP._core.Structures import *
from SQLOOP._core.Words import *
from SQLOOP._core.Databases import *

class Branch:

	database : Database
	table : Table
	parentCol : Column
	childCol : Column
	node : Any

	def __init__(self, database : Database, node : Any):
		self.database = database
		self.node = node
	
	@property
	def parent(self) -> Self|None:
		try:
			return self.__class__(self.database, next(self.database(SELECT(self.parentCol) - FROM(self.table) - WHERE(self.childCol == self.node))))
		except StopIteration:
			return None

	@property
	def children(self) -> Generator[Self,None,None]:
		for childNode in self.database(SELECT(self.childCol) - FROM(self.table) - WHERE(self.parentCol == self.node)):
			yield self.__class__(self.database, childNode)