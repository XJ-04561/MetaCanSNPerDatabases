
from SQLOOP._core.Structures import *

class ALL(Column, name="*"): pass

class SQL(Column): pass
class NAME(Column): pass
class TYPE(Column): pass
class ROW_ID(Column, name="rowid"): pass
class TABLE_NAME(Column, name="tbl_name"): pass
class ROOT_PAGE(Column, name="rootpage"): pass

class SQLITE_MASTER(Table, name="sqlite_master"):
	
	SQL = SQL
	NAME = NAME
	TYPE = TYPE
	ROW_ID = ROW_ID
	TABLE_NAME = TABLE_NAME
	ROOT_PAGE = ROOT_PAGE
