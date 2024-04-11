


from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.SQL import *
from MetaCanSNPerDatabases.core.Structures import *
from MetaCanSNPerDatabases.core.Words import *

class Aggregate:
	
	__name__ : str
	X : Column

	def __init__(self, X : Column):
		self.X = X

	def __repr__(self):
		return f"<{pluralize(self.__class__.__base__.__name__)}.{self.__class__.__name__} X={self.X!r}>"
		
	def __str__(self) -> str:
		return f"{self.__name__}({self.X})"
		
	def __sql__(self) -> str:
		return self.__str__()
	
	def __hash__(self):
		return self.__str__().__hash__()
	
	def __format__(self, format_spec : str):
		if format_spec.endswith("!sql"):
			return self.__sql__().__format__(format_spec.rstrip("!sql"))
		elif format_spec.endswith("!r"):
			return self.__repr__().__format__(format_spec.rstrip("!r"))
		elif format_spec.endswith("!s"):
			return self.__str__().__format__(format_spec.rstrip("!s"))
		else:
			return self.__str__().__format__(format_spec)

class AVG:
	__name__ : str = "AVG"
class COUNT:
	__name__ : str = "COUNT"
class MAX:
	__name__ : str = "MAX"
class MIN:
	__name__ : str = "MIN"
class SUM:
	__name__ : str = "SUM"
class TOTAL:
	__name__ : str = "TOTAL"

class GROUP_CONCAT:
	__name__ : str = "GROUP_CONCAT"
	order : ORDER_BY|None = None
	
	@Overload
	def __init__(self, X : Column, order : ORDER_BY, Y : str=","):
		self.X = X
		self.order = order
		self.Y = Y
	
	@__init__.add
	def __init__(self, X : Column, Y : str=","):
		self.X = X
		self.Y = Y
		
	def __str__(self) -> str:
		return f"{self.__name__}({self.X} {self.order}, {self.Y})"
	
class STRING_AGG(GROUP_CONCAT):
	__name__ : str = "STRING_AGG"