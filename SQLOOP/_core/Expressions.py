

from SQLOOP._core.Structures import *
from SQLOOP._core.Structures import Word
from SQLOOP._core.Words import *
from SQLOOP._core.Words import Word

class Expression(Query):

	startWords : set[type[Word]]
	startWord : type[Word]

	def __init__(self, startWord, *words: tuple[Word], sep: str = None):
		super().__init__(startWord, *words, sep=sep)
		self.startWord = type(self.words[0]) if isinstance(self.words[0], Word) else self.words[0]
	
	def __init_subclass__(cls, *args, name: str | None = None, **kwargs) -> None:
		super().__init_subclass__(*args, name=name, **kwargs)
		for word in cls.startWords:
			word.startWord = word
			words = cached_property(lambda self:SQLTuple([self]))
			words.__set_name__(word, "words")
			word.words = words
			for attrName, value in vars(cls).items():
				if not hasattr(word, attrName):
					setattr(word, attrName, value)
			

class SelectStatement(Expression):

	startWords = {SELECT}
	startWord = SELECT
	
	@property
	def cols(self):

		from SQLOOP._core.Schema import ALL
		n = 0
		for column in self.columns:
			if column is ALL:
				n += sum(map(len, self.tables))
			else:
				n += 1
		return n

	@property
	def columns(self):
		"""The columns of a SELECT-statement must be either the content given in the instance creation or if the SELECT
		is the type object itself, then the only column selected is the second word in the statement."""
		try:
			return self.words[0].content if isinstance(self.words[0], SELECT) else (self.words[1], )
		except IndexError:
			return ()

	@columns.setter
	def columns(self, values):
		if not isinstance(values, tuple):
			values = (values,)
		for i, word in enumerate(self.words):
			if word is SELECT:
				self.words = (self.words[:i], SELECT(*values), self.words[i+2:])
				break
			elif isinstance(word, FROM):
				self.words = (self.words[:i], SELECT(*values), self.words[i+1:])
				break
		else:
			self.words = self.words + (SELECT(*values),)

	@property
	def tables(self):
		for i, word in enumerate(self.words):
			if isinstance(word, FROM):
				return word.content
			elif word is FROM:
				try:
					return (self.words[i+1], )
				except IndexError:
					return ()
	
	@tables.setter
	def tables(self, values):
		if not isinstance(values, tuple):
			values = (values,)
		for i, word in enumerate(self.words):
			if word is FROM:
				self.words = (*self.words[:i], FROM(*values), *self.words[i+2:])
				break
			elif isinstance(word, FROM):
				self.words = (*self.words[:i], FROM(*values), *self.words[i+1:])
				break
		else:
			self.words = self.words + (FROM(*values),)
	
	@property
	def wheres(self):
		_iter = iter(self.words)
		for word in _iter:
			if isinstance(word, WHERE):
				return word.content
			elif word is WHERE:
				ret = []
				for comp in itertools.takewhile(lambda x:isinstance(x, Comparison) or x is AND, _iter):
					if comp is AND:
						continue
					ret.append(comp)
				return tuple(ret)
		return ()
	
	@wheres.setter
	def wheres(self, values):
		if not isinstance(values, tuple):
			values = (values,)
		for i, word in enumerate(self.words):
			if word is WHERE:
				for j, word2 in enumerate(self.words[i:]):
					if word2 is not AND and not isinstance(word2, Comparison):
						break
				self.words = (*self.words[:i], WHERE(*values), *self.words[i+j:])
				break
			elif isinstance(word, WHERE):
				self.words = (*self.words[:i], WHERE(values), *self.words[i+1:])
				break
		else:
			self.words = self.words + (WHERE(*values),)
	
	@property
	def singlet(self):
		from SQLOOP._core.Aggregates import Aggregate
		for col in self.columns:
			if not isinstance(col, Aggregate):
				break
		else:
			return True
		conditionalColumns = set(map(*this.left, filter(lambda x:isRelated(x.left, Column) and x.operator != "IN" and isinstance(x.right, SanitizedValue), self.wheres)))
		for constraint in self.constraints:
			if not constraint.unique:
				continue
			if conditionalColumns.issuperset(constraint.columns):
				return True
		else:
			return False

	@property
	def constraints(self) -> set["TableConstraint"]:
		return set(constraint for t in self.tables for constraint in t.constraints)

class TableConstraint(Expression):

	startWords = {CONSTRAINT, PRIMARY, UNIQUE, CHECK, FOREIGN, NOT}

	def __init__(self, *words : tuple[Word], sep : str=" "):
		super().__init__(*words, sep=sep)
	
	@property
	def unique(self):
		return self.startWord == PRIMARY or self.startWord == UNIQUE
	
	@property
	def columns(self):
		for word in self.words:
			if isinstance(word, SQLTuple):
				return word
			elif isinstance(word, Word):
				return word.content
	
	expression = columns
