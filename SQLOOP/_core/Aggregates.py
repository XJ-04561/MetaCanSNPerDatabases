


from SQLOOP.Globals import *
from SQLOOP._core.Structures import *
from SQLOOP._core.Words import *

class Aggregate:
	
	X : Column

	def __init__(self, X : Column):
		self.X = X

	def __repr__(self):
		from SQLOOP._core.Functions import pluralize
		return f"<{pluralize(self.__class__.__base__.__name__)}.{self.__class__.__name__} {', '.join(map(lambda name: f'{name}={self.__dict__.get(name)}'.format, self.__annotations__))}>"
		
	def __str__(self) -> str:
		return f"{self.__class__.__name__}({self.X})"
		
	def __sql__(self) -> str:
		return self.__str__()
	
	def __hash__(self):
		return self.__str__().__hash__()
	
	# def __format__(self, format_spec : str):
	# 	if format_spec.endswith("!sql"):
	# 		return self.__sql__().__format__(format_spec.rstrip("!sql"))
	# 	elif format_spec.endswith("!r"):
	# 		return self.__repr__().__format__(format_spec.rstrip("!r"))
	# 	elif format_spec.endswith("!s"):
	# 		return self.__str__().__format__(format_spec.rstrip("!s"))
	# 	else:
	# 		return self.__str__().__format__(format_spec)

class AVG(Aggregate): pass
class COUNT(Aggregate): pass
class MAX(Aggregate): pass
class MIN(Aggregate): pass
class SUM(Aggregate): pass
class TOTAL(Aggregate): pass

class GROUP_CONCAT(Aggregate):

	Y : str
	
	def __init__(self, X : Column, Y : str=","):
		self.X = X
		self.Y = Y
		
	def __str__(self) -> str:
		return f"{self.__class__.__name__}({self.X}, {self.Y})"
	
class STRING_AGG(GROUP_CONCAT): pass