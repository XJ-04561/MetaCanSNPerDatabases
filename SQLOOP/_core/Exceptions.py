
from sqlite3 import DatabaseError

class DatabaseNotConnected(DatabaseError): pass
class MissingArgument(DatabaseError): pass
class MissingReferenceFile(DatabaseError): pass
class UnableToDefineChromosomes(DatabaseError): pass
class TableDefinitionMissmatch(DatabaseError): pass
class ColumnNotFoundError(DatabaseError): pass
class TablesNotRelated(DatabaseError): pass
class ResultsShorterThanLookup(DatabaseError): pass
class NoResultsFromQuery(DatabaseError): pass

class DatabaseSchemaEmpty(DatabaseError): pass
class SchemaTablesMismatch(DatabaseError): pass
class SchemaIndexesMismatch(DatabaseError): pass
class PRAGMAVersionMismatch(DatabaseError): pass


class Assertion:
	def exception(self, database) -> Exception: raise NotImplementedError("Not implemented in the base class.")
	def condition(self, database) -> bool: raise NotImplementedError("Not implemented in the base class.")
	def rectify(self, database) -> None: raise NotImplementedError("Not implemented in the base class.")

class SchemaNotEmpty(Assertion):
	@classmethod
	def exception(self, database : "Database") -> Exception:
		return DatabaseSchemaEmpty("Database is empty.")
	@classmethod
	def condition(self, database : "Database") -> bool:
		from SQLOOP._core.Words import SELECT, FROM
		from SQLOOP._core.Schema import ALL, SQLITE_MASTER
		from SQLOOP._core.Aggregates import COUNT
		res = database(SELECT - COUNT (ALL) - FROM - SQLITE_MASTER)
		return database(SELECT - COUNT (ALL) - FROM - SQLITE_MASTER) != 0
	@classmethod
	def rectify(self, database : "Database") -> None:
		from SQLOOP._core.Words import BEGIN, TRANSACTION, CREATE, TABLE, PRAGMA, COMMIT
		from SQLOOP.Globals import sql
		database(BEGIN - TRANSACTION)
		for table in database.tables:
			database(CREATE - TABLE - sql(table))
		database(PRAGMA (user_version = database.CURRENT_VERSION))
		database(COMMIT)

class ValidTablesSchema(Assertion):
	@classmethod
	def exception(cls, database : "Database") -> Exception:
		return SchemaTablesMismatch(f"Tables are constructed differently to the current version (Database table schema hash:{database.tablesHash:X} | Current version hash: {database.CURRENT_TABLES_HASH:X}).")
	@classmethod
	def condition(cls, database : "Database") -> bool:
		return database.tablesHash == database.CURRENT_TABLES_HASH
	@classmethod
	def rectify(cls, database : "Database") -> None:
		from SQLOOP._core.Words import BEGIN, TRANSACTION, CREATE, TABLE, PRAGMA, COMMIT, ALTER, RENAME, TO, INSERT, INTO, SELECT, FROM, DROP, INDEX
		from SQLOOP._core.Schema import ALL
		from SQLOOP.Globals import sql
		if not database.clearIndexes():
			raise DatabaseError("Could not clear indexes!")
		database(BEGIN - TRANSACTION)
		for table in database.tables:
			database(ALTER - TABLE - table - RENAME - TO - f"{table}2")
			database(CREATE - TABLE - sql(table))
			database(INSERT - INTO - table - (SELECT (ALL) - FROM(f"{table}2") ))
			database(DROP - TABLE - f"{table}2")
		for index in database.indexes:
			database(CREATE - INDEX - sql(index))
		database(COMMIT)
		database(BEGIN - TRANSACTION)
		for (table,) in database("SELECT name FROM sqlite_master WHERE type='table';"):
			if table not in database.tables:
				database(DROP - TABLE - table)
		database(PRAGMA (user_version = database.CURRENT_VERSION))
		database(COMMIT)

class ValidIndexesSchema(Assertion):
	@classmethod
	def exception(cls, database : "Database") -> Exception:
		return SchemaIndexesMismatch(f"Indexes are constructed differently to the current version (Database index schema hash:{database.indexesHash:X} | Current version hash: {database.CURRENT_INDEXES_HASH:X}).")
	@classmethod
	def condition(cls, database : "Database") -> bool:
		return database.indexesHash == database.CURRENT_INDEXES_HASH
	@classmethod
	def rectify(cls, database : "Database") -> None:
		from SQLOOP._core.Words import BEGIN, TRANSACTION, CREATE, TABLE, PRAGMA, COMMIT, ALTER, RENAME, TO, INSERT, INTO, SELECT, FROM, DROP, INDEX, IF, NOT, EXISTS
		from SQLOOP._core.Schema import ALL
		from SQLOOP.Globals import sql
		if not database.clearIndexes():
			raise DatabaseError("Could not clear indexes!")
		database(BEGIN - TRANSACTION)
		for index in database.indexes:
			database(CREATE - INDEX - IF - NOT - EXISTS - sql(index))
		database(PRAGMA (user_version = database.CURRENT_VERSION))
		database(COMMIT)
try:
	from SQLOOP._core.Databases import Database
except:
	pass