from SQLOOP import *
from SQLOOP.core import *

def test_new():
	from pprint import pprint

	from SQLOOP._core.Structures import Column, Table

	class MyTable(Table, name="my_table"):
		
		class IDColumn(Column, name="id"): pass
		class NameColumn(Column, name="name"): pass
		class SpecialColumn(Column, name="special_column"): pass

		columns : tuple[Column]
		columnLookup : dict[str,Column]
		options = ()

	assert MyTable.__name__ == "MyTable", f'{MyTable.__name__=} == {"MyTable"=}'
	assert MyTable.__sql_name__ == "my_table", f'{MyTable.__sql_name__=} == {"my_table"=}'
	assert hash(MyTable.columns) == hash((MyTable.IDColumn, MyTable.NameColumn, MyTable.SpecialColumn)), f'{MyTable.columns=} == {(MyTable.IDColumn, MyTable.NameColumn, MyTable.SpecialColumn)=}'
	assert hash(MyTable.columnLookup["IDColumn"]) == hash(MyTable.IDColumn), f'{MyTable.columnLookup["IDColumn"]=} == {MyTable.IDColumn=}'
	assert hash(MyTable.columnLookup["NameColumn"]) == hash(MyTable.NameColumn), f'{MyTable.columnLookup["NameColumn"]=} == {MyTable.NameColumn=}'
	assert hash(MyTable.columnLookup["SpecialColumn"]) == hash(MyTable.SpecialColumn), f'{MyTable.columnLookup["SpecialColumn"]=} == {MyTable.SpecialColumn=}'

	class MySecondTable(Table, name="my_table2"):
		
		class IDColumn(Column, name="id"): pass
		class NameColumn(Column, name="name"): pass
		class SpecialColumn(Column, name="special_column"): pass

		options = ()
	
	assert MySecondTable.__name__ == "MySecondTable", f'{MySecondTable.__name__=} == {"MySecondTable"=}'
	assert MySecondTable.__sql_name__ == "my_table2", f'{MySecondTable.__sql_name__=} == {"my_table2"=}'
	assert hash(MySecondTable.columns) == hash((MySecondTable.IDColumn, MySecondTable.NameColumn, MySecondTable.SpecialColumn)), f'{MySecondTable.columns=} == {(MySecondTable.IDColumn, MySecondTable.NameColumn, MySecondTable.SpecialColumn)=}'
	assert hash(MySecondTable.columnLookup["IDColumn"]) == hash(MySecondTable.IDColumn), f'{MySecondTable.columnLookup["IDColumn"]=} == {MySecondTable.IDColumn=}'
	assert hash(MySecondTable.columnLookup["NameColumn"]) == hash(MySecondTable.NameColumn), f'{MySecondTable.columnLookup["NameColumn"]=} == {MySecondTable.NameColumn=}'
	assert hash(MySecondTable.columnLookup["SpecialColumn"]) == hash(MySecondTable.SpecialColumn), f'{MySecondTable.columnLookup["SpecialColumn"]=} == {MySecondTable.SpecialColumn=}'

	assert hash(MyTable.columns) == hash(MySecondTable.columns), f'{MyTable.columns=} == {MySecondTable.columns=}'

def test_words():
	
	from SQLOOP._core.Structures import Column, Table
	
	class ID(Column, name="id"): pass
	class Name(Column, name="name"): pass

	class MyTable(Table, name="my_table"):
		class ID(Column, name="id"): pass
		class Name(Column, name="name"): pass

	query1 = SELECT (ID, Name) - FROM ( MyTable ) - WHERE (ID < 10, Name == "parent")
	query2 = SELECT (ID, Name) - FROM - MyTable - WHERE - (ID < 10) - AND - (Name == "parent")

	assert str(query1) == "SELECT id, name FROM my_table WHERE id < ? AND name == ?", f'{str(query1)=} == {"SELECT (id, name) FROM my_table WHERE id < ? AND name == ?"=}'
	assert str(query2) == "SELECT id, name FROM my_table WHERE id < ? AND name == ?", f'{str(query2)=} == {"SELECT (id, name) FROM my_table WHERE id < ? AND name == ?"=}'

