
from MetaCanSNPerDatabases.Globals import *
import MetaCanSNPerDatabases.Globals as Globals
import MetaCanSNPerDatabases.core.Columns as Columns
from MetaCanSNPerDatabases.core.Columns import Column
from MetaCanSNPerDatabases.core._Constants import *

class Index:

	table : Any
	columns : list[Column]

	def __init__(self, table, *columns : Column):
		self.table = table
		self.columns = columns

	def __format__(self, format_spec : str):
		match format_spec.rsplit("!", 1)[-1]:
			case "!sql":
				return f"{self.table:!sql}By{''.join(map(str.capitalize, self.columns))}"
			case "!sql.CREATE":
				return f"{self.table:!sql}By{''.join(map(str.capitalize, self.columns))} ON {self.table:!sql}({', '.join(self.columns)})"
			case _:
				return self.__class__.__name__.__format__(format_spec)