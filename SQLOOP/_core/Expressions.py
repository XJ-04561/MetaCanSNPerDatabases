

from SQLOOP._core.Structures import *
from SQLOOP._core.Structures import Word
from SQLOOP._core.Words import *
from SQLOOP._core.Words import Word

class Expression(Query):

	startWords : set[type[Word]]
	startWord : type[Word]

	def __init__(self, startWord, *words: tuple[Word], sep: str = " "):
		super().__init__(startWord, *words, sep=sep)
		self.startWord = type(self.words[0]) if isinstance(self.words[0], Word) else self.words[0] 

class SelectStatement(Expression):

	startWords = {SELECT}
	startWord = SELECT
	
	@property
	def columns(self):
		return self.words[0].content

	@cached_property
	def tables(self):
		for i, word in enumerate(self.words):
			if isinstance(word, FROM):
				return word.content
			elif word is FROM:
				return tuple(filter(lambda x:not isRelated(x, Word), itertools.takewhile(lambda x:x is not WHERE and not isinstance(x, WHERE),self.words[i:])))
	
	@cached_property
	def wheres(self):
		tables = self.tables
		_iter = iter(self.words)
		for word in _iter:
			if isinstance(word, WHERE):
				return [col for comp in word.content for col in comp if any(col in t for t in tables)]
			elif word is WHERE:
				ret = []
				for comp in itertools.takewhile(lambda x:isinstance(x, Comparison) or x is AND, _iter):
					if comp is AND:
						continue
					for col in comp:
						if any(col in t for t in tables):
							ret.append(col)
				return ret
		return []
	
	@cached_property
	def singlet(self):
		from SQLOOP._core.Aggregates import Aggregate
		for col in self.columns:
			if not isinstance(col, Aggregate):
				break
		else:
			return True
		for constraint in self.constraints:
			if not constraint.unique:
				continue
			if all(col not in self.wheres for col in constraint.columns):
				break
		else:
			return True
		return False

	@cached_property
	def constraints(self):
		return set(itertools.chain(*(t.constraints for t in self.tables)))

class TableConstraint(Expression):

	startWords = {CONSTRAINT, PRIMARY, UNIQUE, CHECK, FOREIGN}

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

# class Expression:

# 	word : Word|Query
# 	contents : tuple["SyntaxNode"]
# 	def __init__(self, word, contents):
# 		self.word = word
# 		self.contents = contents
# 	def __eq__(self, other):
# 		return isinstance(other, self.word)
# 	def __getitem__(self, word):
# 		if isinstance(word, Query):
# 			word = word.words[0]


# 		for node in self.contents:
# 			if word == node.word:
# 				return node

# class SyntaxNode:

# 	word : Word|Query
# 	nexts = list[Self]

# 	def __init__(self, word):
# 		self.word = word
# 		self.nexts = []
	
# 	def __rshift__(self, other):
# 		ret = SyntaxNode(other)
# 		self.nexts.append(ret)
# 		return ret

# 	def __getitem__(self, word):
# 		for node in self.nexts:
# 			if word == node.word:
# 				return node

# class SyntaxDict(dict):

# 	def __init__(self, iterable):
# 		for key, value in iterable:
# 			if isinstance(key, Union):
# 				for subKey in key:
# 					self[subKey] = value
# 			else:
# 				self[key] = value
# 	def __getitem__(self, name):
	