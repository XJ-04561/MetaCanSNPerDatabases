
from sqlite3 import DatabaseError

class DatabaseNotConnected(DatabaseError): pass
class MissingArgument(DatabaseError): pass
class MissingReferenceFile(DatabaseError): pass
class UnableToDefineChromosomes(DatabaseError): pass
class DownloadFailed(DatabaseError): pass
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
	def exception(self, database) -> Exception:
		return DatabaseSchemaEmpty("Database is empty.")
	@classmethod
	def condition(self, database) -> bool:
		return database._connection(f"SELECT COUNT(*) FROM sqlite_master;").fetchone()[0] == 0
	@classmethod
	def rectify(self, database) -> None:
		from MetaCanSNPerDatabases._core.Words import BEGIN, TRANSACTION, CREATE, TABLE, PRAGMA, COMMIT
		from MetaCanSNPerDatabases._core.Structures import sql
		from MetaCanSNPerDatabases.Globals import CURRENT_VERSION
		database(BEGIN - TRANSACTION)
		for table in database.tables:
			database(CREATE - TABLE - sql(table))
		database(PRAGMA (user_version = CURRENT_VERSION))
		database(COMMIT)

class ValidTablesSchema(Assertion):
	@classmethod
	def exception(cls, database) -> Exception:
		from MetaCanSNPerDatabases.Globals import CURRENT_TABLES_HASH
		return SchemaTablesMismatch(f"Tables are constructed differently to the current version (Database table schema hash:{database.tablesSchema!r} | Current version hash: {CURRENT_TABLES_HASH!r}).")
	@classmethod
	def condition(cls, database) -> bool:
		from MetaCanSNPerDatabases.Globals import CURRENT_TABLES_HASH
		return database.tablesSchema != CURRENT_TABLES_HASH
	@classmethod
	def rectify(cls, database) -> None:
		from MetaCanSNPerDatabases._core.Words import BEGIN, TRANSACTION, CREATE, TABLE, PRAGMA, COMMIT, ALTER, RENAME, TO, INSERT, INTO, SELECT, ALL, FROM, DROP, INDEX
		from MetaCanSNPerDatabases._core.Structures import sql
		from MetaCanSNPerDatabases.Globals import CURRENT_VERSION
		database(BEGIN - TRANSACTION)
		database.clearIndexes()
		for table in database.tables:
			database(ALTER - TABLE - table - RENAME - TO - f"{table}2")
			database(CREATE - TABLE - sql(table))
			database(INSERT - INTO - table - (SELECT (ALL) - FROM(f"{table}2") ))
			database(DROP - TABLE - f"{table}2")
		for index in database.indexes:
			database(CREATE - INDEX - sql(index))
		database(COMMIT)
		database(BEGIN - TRANSACTION)
		for (table,) in database._connection.execute("SELECT name FROM sqlite_master WHERE type='table';"):
			if not any(table == validTable.name for validTable in database.tables):
				database(DROP - TABLE - table)
		database(PRAGMA (user_version = CURRENT_VERSION))
		database(COMMIT)

class ValidIndexesSchema(Assertion):
	@classmethod
	def exception(cls, database) -> Exception:
		from MetaCanSNPerDatabases.Globals import CURRENT_INDEXES_HASH
		return SchemaIndexesMismatch(f"Indexes are constructed differently to the current version (Database index schema hash:{database.indexesSchema!r} | Current version hash: {CURRENT_INDEXES_HASH!r}).")
	@classmethod
	def condition(cls, database) -> bool:
		from MetaCanSNPerDatabases.Globals import CURRENT_INDEXES_HASH
		return database.indexesSchema != CURRENT_INDEXES_HASH
	@classmethod
	def rectify(cls, database) -> None:
		from MetaCanSNPerDatabases._core.Words import BEGIN, TRANSACTION, CREATE, TABLE, PRAGMA, COMMIT, ALTER, RENAME, TO, INSERT, INTO, SELECT, ALL, FROM, DROP, INDEX
		from MetaCanSNPerDatabases._core.Structures import sql
		from MetaCanSNPerDatabases.Globals import CURRENT_VERSION
		database(BEGIN - TRANSACTION)
		database.clearIndexes()
		for index in database.indexes:
			database(CREATE - INDEX - sql(index))
		database(PRAGMA (user_version = CURRENT_VERSION))
		database(COMMIT)