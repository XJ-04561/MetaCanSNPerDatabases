from SQLOOP import *
from SQLOOP.core import *

def test_new():
	from SQLOOP._core.Structures import NewMeta
	from pprint import pprint
	class MyTable(NewMeta):
		
		columns : tuple[Column]

		id = Column("id")
		name = Column("name")
		specialColumn = Column("special_column")
		options = ()
	
	table1 = Table("MyTable", "MyTable", columns=(Column("id"), Column("name"), Column("special_column")))
	table2 = MyTable
	assert table1.__name__ == table2.__name__, f"{table1.__name__=} == {table2.__name__=}"
	assert table1.name == table2.name, f"{table1.name=} == {table2.name=}"
	assert table1.columns == table2.columns, f"{table1.columns=} == {table2.columns=}"
	assert table1.options == table2.options, f"{table1.options=} == {table2.options=}"

def test_words():

	query1 = SELECT (Column("id"), Column("name")) - FROM ( Table("myTable", "myTable") ) - WHERE (Column("id") < 10, Column("name") == "parent")
	query2 = SELECT - Column("id") - Column("name") - FROM - Table("myTable", "myTable") - WHERE  - (Column("id") < 10) - (Column("name") == "parent")

	assert str(query1) == "SELECT (id, name) FROM myTable WHERE id < 10 AND name == \"parent\"", f'{str(query1)=} == {"SELECT (id, name) FROM myTable WHERE id < 10 AND name == \"parent\""=}'
	assert str(query2) == "SELECT (id, name) FROM myTable WHERE id < 10 AND name == \"parent\"", f'{str(query2)=} == {"SELECT (id, name) FROM myTable WHERE id < 10 AND name == \"parent\""=}'

