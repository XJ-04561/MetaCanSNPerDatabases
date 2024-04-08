
from typing import overload
from MetaCanSNPerDatabases.Globals import *
from MetaCanSNPerDatabases.core.Columns import *

class Table:
	@overload
	def createIndex(self : Self):
		"""Create the default indexes configured for this table."""
		...
	@overload
	def createIndex(self : Self, *cols : Column, name : str=None):
		"""Create an index on the table with the columns that are given in the method call. If no columns are specified, will create the default indexes defined in the `self._indexes` attribute.

		Args:
			*cols			(ColumnFlarg): Columns to be used in the index.
			name			(str): Name of index to be created, a value of None will generate a name based on the table and the columns involved. Defaults to None.
		"""

		...

	def get(self,
			*columnsToGet : tuple[Column],
			orderBy : Column|tuple[Column] = ...,
			# WHERE-statements
			Parent : int = ...,
			NodeID : int = ...,
			Genotype : str = ...,
			Position : int = ...,
			Ancestral : Nucleotides = ...,
			Derived : Nucleotides = ...,
			SNPReference : str = ...,
			Date : str = ...,
			ChromID : int = ...,
			Chromosome : str = ...,
			GenomeID : int = ...,
			Genome : str = ...,
			Strain : str = ...,
			GenbankID : str = ...,
			RefseqID : str = ...,
			Assembly : str = ...
			) -> Generator[tuple[Any],None,None]:
		"""All positional arguments should be `Column` objects and they are used to
		determine what information to be gathered from the database.
		
		All keyword arguments (except `orderBy`) are the conditions by which each row
		is selected. For example, if you inted to get the row for a specific genbankID
		then you would use the keyword argument as such: `genbankID="GCA_123123123.1"`.
		
		`orderBy` is used to sort the selected data according to `Column`.
		Direction is indicated by negating the flag. A positive flag is the default
		of "DESC" and negative flags indicate "ASC".

		Args:
			# `SELECT`-statements

			*columnsToGet : tuple[Column]

			# `ORDER BY`-statements
			
			orderBy 		(Column | tuple[Column], optional):
				The default direction is `DESC`, to use `ASC` simply negate the Column by using the negation operator `-`. Example:
					```python
						Table().get( orderBy = (Columns.NodeID, -Columns.Position))
						# Will sort selection in descending order by NodeID, and in ascending order by Position.
					```

			# `WHERE`-statements

			Parent			(int)
			NodeID 			(int)
			Genotype		(str)
			Position		(int)
			Ancestral		(Nucleotides)
			Derived			(Nucleotides)
			SNPReference	(str)
			Date			(str)
			ChromID			(int)
			Chromosome		(str)
			GenomeID		(int)
			Genome			(str)
			Strain			(str)
			GenbankID		(str)
			RefseqID		(str)
			Assembly		(str)

		Yields:
			tuple[Any]
		"""
	...

	def first(self,
			*columnsToGet : tuple[Column],
			orderBy : Column|tuple[Column] = ...,
			Parent : int = ...,
			NodeID : int = ...,
			Genotype : str = ...,
			Position : int = ...,
			Ancestral : Nucleotides = ...,
			Derived : Nucleotides = ...,
			SNPReference : str = ...,
			Date : str = ...,
			ChromID : int = ...,
			Chromosome : str = ...,
			GenomeID : int = ...,
			Genome : str = ...,
			Strain : str = ...,
			GenbankID : str = ...,
			RefseqID : str = ...,
			Assembly : str = ...
			) -> tuple[Any]:
		"""All positional arguments should be `Column` objects and they are used to
		determine what information to be gathered from the database.
		
		All keyword arguments (except `orderBy`) are the conditions by which each row
		is selected. For example, if you inted to get the row for a specific genbankID
		then you would use the keyword argument as such: `genbankID="GCA_123123123.1"`.
		
		`orderBy` is used to sort the selected data according to `Column`.
		Direction is indicated by negating the flag. A positive flag is the default
		of "DESC" and negative flags indicate "ASC".

		Args:
			# `SELECT`-statements

			*columnsToGet : tuple[Column]

			# `ORDER BY`-statements
			
			orderBy 		(Column | tuple[Column], optional):
				The default direction is `DESC`, to use `ASC` simply negate the Column by using the negation operator `-`. Example:
					```python
						Table().get( orderBy = (Columns.NodeID, -Columns.Position))
						# Will sort selection in descending order by NodeID, and in ascending order by Position.
					```

			# `WHERE`-statements

			Parent			(int)
			NodeID 			(int)
			Genotype		(str)
			Position		(int)
			Ancestral		(Nucleotides)
			Derived			(Nucleotides)
			SNPReference	(str)
			Date			(str)
			ChromID			(int)
			Chromosome		(str)
			GenomeID		(int)
			Genome			(str)
			Strain			(str)
			GenbankID		(str)
			RefseqID		(str)
			Assembly		(str)

		Returns:
			tuple[Any]
		"""
		...
	
	def all(self,
			*columnsToGet : tuple[Column],
			orderBy : Column|tuple[Column] = ...,
			Parent : int = ...,
			NodeID : int = ...,
			Genotype : str = ...,
			Position : int = ...,
			Ancestral : Nucleotides = ...,
			Derived : Nucleotides = ...,
			SNPReference : str = ...,
			Date : str = ...,
			ChromID : int = ...,
			Chromosome : str = ...,
			GenomeID : int = ...,
			Genome : str = ...,
			Strain : str = ...,
			GenbankID : str = ...,
			RefseqID : str = ...,
			Assembly : str = ...
			) -> list[tuple[Any]]:
		"""All positional arguments should be `Column` objects and they are used to
		determine what information to be gathered from the database.
		
		All keyword arguments (except `orderBy`) are the conditions by which each row
		is selected. For example, if you inted to get the row for a specific genbankID
		then you would use the keyword argument as such: `genbankID="GCA_123123123.1"`.
		
		`orderBy` is used to sort the selected data according to `Column`.
		Direction is indicated by negating the flag. A positive flag is the default
		of "DESC" and negative flags indicate "ASC".

		Args:
			# `SELECT`-statements

			*columnsToGet : tuple[Column]

			# `ORDER BY`-statements
			
			orderBy 		(Column | tuple[Column], optional):
				The default direction is `DESC`, to use `ASC` simply negate the Column by using the negation operator `-`. Example:
					```python
						Table().get( orderBy = (Columns.NodeID, -Columns.Position))
						# Will sort selection in descending order by NodeID, and in ascending order by Position.
					```

			# `WHERE`-statements

			Parent			(int)
			NodeID 			(int)
			Genotype		(str)
			Position		(int)
			Ancestral		(Nucleotides)
			Derived			(Nucleotides)
			SNPReference	(str)
			Date			(str)
			ChromID			(int)
			Chromosome		(str)
			GenomeID		(int)
			Genome			(str)
			Strain			(str)
			GenbankID		(str)
			RefseqID		(str)
			Assembly		(str)

		Returns:
			list[tuple[Any]]
		"""
		...