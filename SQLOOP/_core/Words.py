
from SQLOOP._core.Structures import *


class BEGIN: pass
class TRANSACTION: pass
class COMMIT: pass
class PRAGMA:
	@overload
	def __init__(self, *columns : tuple[Column]):
		...
	@overload
	def __init__(self, **assignments : dict[str,Any]):
		...
	@final
	def __init__(self, *columns, **assignments):
		if len(columns) > 0:
			assert all(isinstance(col, Column) for col in columns), f"Only columns are allowed for a PRAGMA-statement not: {set(col for col in columns if not isinstance(col, Column))}"
			self.content = columns
		elif len(assignments) > 0:
			content = []
			for variable, value in assignments.items():
				content.append(Assignment(variable, value))
			self.content = tuple(content)
		else:
			raise ValueError("PRAGMA what? Nothing provided to PRAGMA.")
class VALUES:
	def __init__(self, *args):
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

BEGIN			= Prefix("BEGIN", (Word,), {})
TRANSACTION		= Prefix("TRANSACTION", (Word,), {})
COMMIT			= Prefix("COMMIT", (Word,), {})
PRAGMA			= Prefix("PRAGMA", (Word,), {"PRAGMA" : PRAGMA.__init__})
VALUES			= Prefix("VALUES", (EnclosedWord,), {"VALUES" : VALUES.__init__})
UPDATE			= Prefix("UPDATE", (Word,), {})
SET				= Prefix("SET", (Word,), {"SET" : SET.__init__})
UNIQUE			= Prefix("UNIQUE", (EnclosedWord,), {})
SELECT			= Prefix("SELECT", (Word,), {"SELECT" : SELECT.__mult__})
FROM			= Prefix("FROM", (Word,), {})
WHERE			= Prefix("WHERE", (Word,), {})
ASC				= Prefix("ASC", (Word,), {})
DESC			= Prefix("DESC", (Word,), {})
ORDER			= Prefix("ORDER", (Word,), {})
BY				= Prefix("BY", (Word,), {})
LIMIT			= Prefix("LIMIT", (Word,), {})
CREATE			= Prefix("CREATE", (Word,), {})
ALTER			= Prefix("ALTER", (Word,), {})
RENAME			= Prefix("RENAME", (Word,), {})
DROP			= Prefix("DROP", (Word,), {})
TO				= Prefix("TO", (Word,), {})
INDEX			= Prefix("INDEX", (Word,), {})
TABLE			= Prefix("TABLE", (Word,), {})
TRIGGER			= Prefix("TRIGGER", (Word,), {})
VIEW			= Prefix("VIEW", (Word,), {})
IF				= Prefix("IF", (Word,), {})
NOT				= Prefix("NOT", (Word,), {})
PRIMARY			= Prefix("PRIMARY", (Word,), {})
FOREIGN			= Prefix("FOREIGN", (Word,), {})
EXISTS			= Prefix("EXISTS", (Word,), {})
INSERT			= Prefix("INSERT", (Word,), {})
INTO			= Prefix("INTO", (Word,), {})
KEY				= Prefix("KEY", (EnclosedWord,), {})
REFERENCES		= Prefix("REFERENCES", (Word,), {"REFERENCES" : REFERENCES.__str__})
