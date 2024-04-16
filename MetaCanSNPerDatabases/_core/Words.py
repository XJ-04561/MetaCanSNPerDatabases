
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases._core.Structures import *


class BEGIN: pass
class TRANSACTION: pass
class COMMIT: pass
class PRAGMA:
	@Overload
	def __init__(self, *columns):
		self.content = columns

	@__init__.add
	def __init__(self, **assignments):
		content = []
		for variable, value in assignments.items():
			content.append(Assignment(variable, value))
		self.content = tuple(content)
class VALUES:
	def __init__(self, *args):
		from MetaCanSNPerDatabases.core.Structures import SanitizedValue
		self.content = tuple(map(SanitizedValue, args))
class UPDATE: pass
class SET:
	def __init__(self, *comparisons, **assignments):
		content = []
		for comp in comparisons:
			content.append(Assignment(comp.left, comp.right))
		for variable, value in assignments.items():
			content.append(Assignment(variable, value))
		self.content = tuple(content)
class UNIQUE: pass
class SELECT:
	def __mult__(self : Word, right : Column|str):
		return Query(self(ALL), right)
class FROM: pass
class WHERE: sep : str = "AND"
class ASC: pass
class DESC: pass
class ORDER: pass
class BY: pass
class LIMIT: pass
class CREATE: pass
class ALTER: pass
class RENAME: pass
class DROP: pass
class TO: pass
class INDEX: pass
class TABLE: pass
class TRIGGER: pass
class VIEW: pass
class IF: pass
class NOT: pass
class PRIMARY: pass
class FOREIGN: pass
class EXISTS: pass
class INSERT: pass
class INTO: pass
class KEY: pass
class REFERENCES:
	def __str__(self : Word):
		match len(self.content):
			case 1:
				return f"{self.__class__.__name__} {self.content[0]}"
			case 2:
				return f"{self.__class__.__name__} {self.content[0]}({', '.join(map(str, self.content[1]))})"
			case _:
				return f"{self.__class__.__name__} {self.content[0]}({', '.join(map(str, self.content[1:]))})"

BEGIN			= Prefix("BEGIN", (Word), BEGIN.__dict__)
TRANSACTION		= Prefix("TRANSACTION", (Word), TRANSACTION.__dict__)
COMMIT			= Prefix("COMMIT", (Word), COMMIT.__dict__)
PRAGMA			= Prefix("PRAGMA", (Word), PRAGMA.__dict__)
VALUES			= Prefix("VALUES", (EnclosedWord), VALUES.__dict__)
UPDATE			= Prefix("UPDATE", (Word), UPDATE.__dict__)
SET				= Prefix("SET", (Word), SET.__dict__)
UNIQUE			= Prefix("UNIQUE", (EnclosedWord), UNIQUE.__dict__)
SELECT			= Prefix("SELECT", (Word), SELECT.__dict__)
FROM			= Prefix("FROM", (Word), FROM.__dict__)
WHERE			= Prefix("WHERE", (Word), WHERE.__dict__)
ASC				= Prefix("ASC", (Word), ASC.__dict__)
DESC			= Prefix("DESC", (Word), DESC.__dict__)
ORDER			= Prefix("ORDER", (Word), ORDER.__dict__)
BY				= Prefix("BY", (Word), BY.__dict__)
LIMIT			= Prefix("LIMIT", (Word), LIMIT.__dict__)
CREATE			= Prefix("CREATE", (Word), CREATE.__dict__)
ALTER			= Prefix("ALTER", (Word), ALTER.__dict__)
RENAME			= Prefix("RENAME", (Word), RENAME.__dict__)
DROP			= Prefix("DROP", (Word), DROP.__dict__)
TO				= Prefix("TO", (Word), TO.__dict__)
INDEX			= Prefix("INDEX", (Word), INDEX.__dict__)
TABLE			= Prefix("TABLE", (Word), TABLE.__dict__)
TRIGGER			= Prefix("TRIGGER", (Word), TRIGGER.__dict__)
VIEW			= Prefix("VIEW", (Word), VIEW.__dict__)
IF				= Prefix("IF", (Word), IF.__dict__)
NOT				= Prefix("NOT", (Word), NOT.__dict__)
PRIMARY			= Prefix("PRIMARY", (Word), PRIMARY.__dict__)
FOREIGN			= Prefix("FOREIGN", (Word), FOREIGN.__dict__)
EXISTS			= Prefix("EXISTS", (Word), EXISTS.__dict__)
INSERT			= Prefix("INSERT", (Word), INSERT.__dict__)
INTO			= Prefix("INTO", (Word), INTO.__dict__)
KEY				= Prefix("KEY", (EnclosedWord), KEY.__dict__)
REFERENCES		= Prefix("REFERENCES", (Word), REFERENCES.__dict__)
