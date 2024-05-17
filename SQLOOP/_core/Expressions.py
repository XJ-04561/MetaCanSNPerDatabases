

from SQLOOP._core.Structures import *
from SQLOOP._core.Structures import Word
from SQLOOP._core.Words import *

class Expression(Query):

	startWords : set[type[Word]]
	startWord : type[Word]

	def __init__(self, startWord, *words: tuple[Word], sep: str = " "):
		super().__init__(*words, sep=sep)
		self.startWord = startWord

class SelectStatement(Expression):

	startWords = {SELECT}
	startWord = SELECT
	
	@property
	def unique(self):
		return self.startWord == PRIMARY or self.startWord == UNIQUE
	
	@property
	def columns(self):
		for word in self.words:
			if isinstance(word, SQLTuple):
				return word
	
	expression = columns

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
	