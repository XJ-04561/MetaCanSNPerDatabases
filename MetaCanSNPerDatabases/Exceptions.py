
from sqlite3 import DatabaseError
class CanSNPDatabaseError(DatabaseError): pass
class DatabaseNotConnected(CanSNPDatabaseError): pass
class MissingArgument(CanSNPDatabaseError): pass
class MissingReferenceFile(CanSNPDatabaseError): pass
class UnableToDefineChromosomes(CanSNPDatabaseError): pass
class DownloadFailed(CanSNPDatabaseError): pass
class TableDefinitionMissmatch(CanSNPDatabaseError): pass