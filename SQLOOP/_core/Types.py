

class SQL_TYPE_META(type):
	def __str__(self):
		return self.__name__

class SQL_TYPE(metaclass=SQL_TYPE_META):
	args : tuple
	def __init__(self, *args):
		self.args = args
	def __str__(self):
		if self.args:
			return f"{type(self).__name__.replace('_', ' ')}({', '.join(map(str, self.args))})"
		else:
			return f"{type(self).__name__}"
class VARCHAR(SQL_TYPE): pass
class CHAR(SQL_TYPE): pass
class INT(SQL_TYPE): pass
class INTEGER(SQL_TYPE): pass
class TINYINT(SQL_TYPE): pass
class SMALLINT(SQL_TYPE): pass
class MEDIUMINT(SQL_TYPE): pass
class BIGINT(SQL_TYPE): pass
class UNSIGNED_BIG_INT(SQL_TYPE): pass
class INT2(SQL_TYPE): pass
class INT8(SQL_TYPE): pass
class CHARACTER(SQL_TYPE): pass
class VARYING_CHARACTER(SQL_TYPE): pass
class NCHAR(SQL_TYPE): pass
class NATIVE_CHARACTER(SQL_TYPE): pass
class NVARCHAR(SQL_TYPE): pass
class TEXT(SQL_TYPE): pass
class CLOB(SQL_TYPE): pass
class BLOB(SQL_TYPE): pass
class REAL(SQL_TYPE): pass
class DOUBLE(SQL_TYPE): pass
class DOUBLE_PRECISION(SQL_TYPE): pass
class FLOAT(SQL_TYPE): pass
class NUMERIC(SQL_TYPE): pass
class DECIMAL(SQL_TYPE): pass
class BOOLEAN(SQL_TYPE): pass
class DATE(SQL_TYPE): pass
class DATETIME (SQL_TYPE): pass
