
from SQLOOP._core.Structures import *
from typing import overload, final, Any


class BEGIN(Word): pass
class ROLLBACK(Word): pass
class IN(EnclosedWord):
	def __rsub__(self, left):
		return Comparison(left, "IN", self.content[0] if len(self.content) == 1 else SQLTuple(self.content))
class TRANSACTION(Word): pass
class COMMIT(Word): pass
class CONSTRAINT(Word): pass
class CHECK(Word): pass
class PRAGMA(Word, metaclass=PragmaMeta):
	@overload
	def __init__(self, *columns : tuple[Column]):
		...
	@overload
	def __init__(self, **assignments : dict[str,Any]):
		...
	@final
	def __init__(self, *columns, **assignments):
		from SQLOOP.Globals import isRelated
		if len(columns) > 0:
			assert all(isRelated(col, Column) for col in columns), f"Only columns are allowed for a PRAGMA-statement not: {set(col for col in columns if not isRelated(col, Column))}"
			self.content = columns
		elif len(assignments) > 0:
			content = []
			for variable, value in assignments.items():
				content.append(Assignment(variable, value, hardcode=True))
			self.content = tuple(content)
		else:
			raise ValueError("PRAGMA what? Nothing provided to PRAGMA.")
class VALUES(EnclosedWord):
	def __init__(self, *args):
		self.content = tuple(map(SanitizedValue, args))
class UPDATE(Word): pass
class SET(Word):
	def __init__(self, *comparisons, **assignments):
		content = []
		for comp in comparisons:
			content.append(Assignment(comp.left, comp.right))
		for variable, value in assignments.items():
			content.append(Assignment(variable, value))
		self.content = tuple(content)
class UNIQUE(EnclosedWord): pass
class SELECT(Word):
	def __mult__(self : Word, right : Column|str):
		from SQLOOP._core.Schema import ALL
		return Query(self(ALL), right)
class FROM(Word): pass
class WHERE(Word): sep : str = " AND "
class ASC(Word): pass
class DESC(Word): pass
class ORDER(Word): pass
class BY(Word): sep : str = " AND "
class LIMIT(Word): pass
class CREATE(Word): pass
class ALTER(Word): pass
class AND(Word): pass
class RENAME(Word): pass
class DROP(Word): pass
class TO(Word): pass
class INDEX(Word): pass
class TABLE(Word): pass
class TRIGGER(Word): pass
class VIEW(Word): pass
class IF(Word): pass
class NOT(Word): pass
class PRIMARY(Word): pass
class FOREIGN(Word): pass
class EXISTS(Word): pass
class INSERT(Word): pass
class INTO(Word): pass
class NULL(EnclosedWord): pass
class KEY(EnclosedWord): pass
class REFERENCES(Word):
	def __str__(self : Word):
		match len(self.content):
			case 1:
				return f"{self.__class__.__name__} {self.content[0]}"
			case _:
				return f"{self.__class__.__name__} {self.content[0]}({', '.join(map(str, self.content[1:]))})"

