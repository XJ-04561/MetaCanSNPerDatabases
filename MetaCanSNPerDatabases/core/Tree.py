
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
from MetaCanSNPerDatabases.core.Structures import *
from MetaCanSNPerDatabases.core.Words import *
from MetaCanSNPerDatabases.core.Databases import *

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