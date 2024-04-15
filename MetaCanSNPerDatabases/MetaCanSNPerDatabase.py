
from MetaCanSNPerDatabases.Exceptions import Assertion, baseAssertions
from MetaCanSNPerDatabases.core.Databases import Database
from sqlite3 import DatabaseError
import MetaCanSNPerDatabases.Globals as Globals

class CanSNPDatabaseError(DatabaseError): pass
class IsLegacyCanSNPer2(CanSNPDatabaseError): pass

class NotLegacyCanSNPer2(Assertion):
	@classmethod
	def exception(self, database) -> Exception:
		return IsLegacyCanSNPer2("Database is Legacy CanSNPer2 schema.")
	@classmethod
	def condition(self, database) -> bool:
		from MetaCanSNPerDatabases.Globals import LEGACY_HASH
		return database.tablesHash == LEGACY_HASH
	@classmethod
	def rectify(self, database) -> None:
		from MetaCanSNPerDatabases.core.Functions import updateFromLegacy
		updateFromLegacy(database, refDir=database.refDir)

class MetaCanSNPerDatabase(Database):
	assertions = [NotLegacyCanSNPer2] + baseAssertions
	refDir : str
	def __init__(self, *args, refDir : str=None, **kwargs):
		import os
		self.refDir = refDir or os.path.realpath(".")
		super().__init__(*args, **kwargs)
		