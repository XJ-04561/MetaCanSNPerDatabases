
from MetaCanSNPerDatabases.Globals import *


class sql(str):
	def __new__(cls, obj):
		return super().__new__(cls, obj.__sql__())

class Comparison(AutoObject):
	
	left : Any
	operator : str
	right : Any

	def __repr__(self):
		return f"<Comparison {str(self)!r}>"

	def __str__(self):
		f"{self.left} {self.operator} {self.right}"