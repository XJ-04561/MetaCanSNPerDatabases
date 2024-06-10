

from SQLOOP._core.Aggregates import Aggregate, AVG, COUNT, MAX, MIN, SUM, TOTAL, GROUP_CONCAT, STRING_AGG
from SQLOOP._core.Databases import Database
from SQLOOP._core.Exceptions import Assertion
from SQLOOP._core.Functions import (ImpossiblePathing, LimitDict, forceHash, CacheMeta, AnyCache, isType, pluralize,
									formatType, Walker, hashQuery, hashSQL, correctDatabase, verifyDatabase,
									getSmallestFootprint, recursiveSubquery, subqueryPaths, createSubqueries)
from SQLOOP._core.Schema import SQLITE_MASTER, ALL
from SQLOOP._core.Structures import Table, Column, Index
from GeekyGadgets.Threads import ThreadConnection
from SQLOOP._core.Tree import Branch
from SQLOOP._core.Types import *
from SQLOOP._core.Words import *
from SQLOOP.Globals import SQLOOP, first, sql, SQLDict

def newColumn(name : str|Column, table : type[Table]=None):
	from SQLOOP._core.Structures import SQLStructure, LinkedColumn
	return SQLStructure(name if isinstance(name, str) else str(name), (LinkedColumn,), {}, table=table)

def createTempTable(**attributes) -> type[Table]:
	from random import randint
	return Table.__class__("TempTable", (Table,), attributes, name=f"TempTable_{randint(0, 1<<32)}")