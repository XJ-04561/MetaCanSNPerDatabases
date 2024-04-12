
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.Structures import *

class UNIQUE(EnclosedWord):
	name : str = "UNIQUE"

class SELECT(Word):
	name : str = "SELECT"

class FROM(Word):
	name : str = "FROM"

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
class PRIMARY: pass
class FOREIGN: pass
IF = Prefix("IF", (), {})
PRIMARY = Prefix("PRIMARY", (), {})
FOREIGN = Prefix("FOREIGN", (), {})
class EXISTS(Word): pass


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