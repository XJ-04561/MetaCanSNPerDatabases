
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.Structures import *

class BEGIN: pass
class TRANSACTION: pass
class COMMIT: pass
BEGIN = Prefix("BEGIN", (), {})
TRANSACTION = Prefix("TRANSACTION", (), {})
COMMIT = Prefix("COMMIT", (), {})

class PRAGMA(Word):
	@Overload
	def __init__(self, *columns):
		self.content = columns

	@__init__.add
	def __init__(self, **assignments):
		content = []
		for variable, value in assignments.items():
			content.append(Assignment(variable, value))
		self.content = tuple(content)

class UNIQUE(EnclosedWord): pass
class SELECT(Word): pass
class FROM(Word): pass
class WHERE(Word):
	sep : str = "AND"

class ASC: pass
class DESC: pass
class ORDER: pass

ASC = Prefix("ASC", (), {})
DESC = Prefix("DESC", (), {})
ORDER = Prefix("ORDER", (), {})

class BY(Word): pass

class LIMIT(Word): pass

class CREATE: pass
class ALTER: pass
class RENAME: pass
class DROP: pass

CREATE = Prefix("CREATE", (), {})
ALTER = Prefix("ALTER", (), {})
RENAME = Prefix("RENAME", (Word), {})
DROP = Prefix("DROP", (), {})

class TO(Word): pass

class INDEX: pass
class TABLE: pass
class TRIGGER: pass
class VIEW: pass
INDEX = Prefix("INDEX", (Word), {})
TABLE = Prefix("TABLE", (Word), {})
TRIGGER = Prefix("TRIGGER", (Word), {})
VIEW = Prefix("VIEW", (Word), {})

class IF: pass
class NOT: pass
class PRIMARY: pass
class FOREIGN: pass
IF = Prefix("IF", (), {})
NOT = Prefix("NOT", (), {})
PRIMARY = Prefix("PRIMARY", (), {})
FOREIGN = Prefix("FOREIGN", (), {})
class EXISTS(Word): pass

class INSERT: pass
class INTO: pass
INSERT = Prefix("INSERT", (), {})
INTO = Prefix("INTO", (), {})

class KEY(EnclosedWord): pass
	
class REFERENCES(Word):
	def __str__(self):
		match len(self.content):
			case 1:
				return f"{self.__class__.__name__} {self.content[0]}"
			case 2:
				return f"{self.__class__.__name__} {self.content[0]}({', '.join(map(str, self.content[1]))})"
			case _:
				return f"{self.__class__.__name__} {self.content[0]}({', '.join(map(str, self.content[1:]))})"