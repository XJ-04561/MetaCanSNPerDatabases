

from SQLOOP import *
from SQLOOP.core import *

def test_new():

	from SQLOOP.core import Column, Table

	class IDColumn(Column, name="id"): pass
	class NameColumn(Column, name="name"): pass
	class SpecialColumn(Column, name="special_column"): pass

	class MyTable(Table, name="my_table"):
		
		id = IDColumn
		name = NameColumn
		special = SpecialColumn
	
	assert MyTable.__name__ == "MyTable", f'{MyTable.__name__=} == {"MyTable"=}'
	assert MyTable.__sql_name__ == "my_table", f'{MyTable.__sql_name__=} == {"my_table"=}'
	assert MyTable.columns["id"] == MyTable.id, f'{MyTable.columns["id"]=} == {MyTable.id=}'
	assert MyTable.columns["name"] == MyTable.name, f'{MyTable.columns["name"]=} == {MyTable.name=}'
	assert MyTable.columns["special_column"] == MyTable.special, f'{MyTable.columns["special_column"]=} == {MyTable.special=}'
	assert MyTable.columns == (MyTable.id, MyTable.name, MyTable.special), f'{MyTable.columns=} == {(MyTable.id, MyTable.name, MyTable.special)=}'

	class MySecondTable(Table, name="my_table2"):
		
		id = IDColumn
		name = NameColumn
		special = SpecialColumn
	
	assert MySecondTable.__name__ == "MySecondTable", f'{MySecondTable.__name__=} == {"MySecondTable"=}'
	assert MySecondTable.__sql_name__ == "my_table2", f'{MySecondTable.__sql_name__=} == {"my_table2"=}'
	assert MySecondTable["id"] == MySecondTable.id, f'{MySecondTable["id"]=} == {MySecondTable.id=}'
	assert MySecondTable["name"] == MySecondTable.name, f'{MySecondTable["name"]=} == {MySecondTable.name=}'
	assert MySecondTable["special_column"] == MySecondTable.special, f'{MySecondTable["special_column"]=} == {MySecondTable.special=}'
	assert MySecondTable.columns == (MySecondTable.id, MySecondTable.name, MySecondTable.special), f'{MySecondTable.columns=} == {(MySecondTable.id, MySecondTable.name, MySecondTable.special)=}'

	assert MyTable.columns == MySecondTable.columns, f'{MyTable.columns=} == {MySecondTable.columns=}'

def test_words():
	
	from SQLOOP._core.Structures import Column, Table
	
	class ID(Column, name="id"): pass
	class Name(Column, name="name"): pass

	class MyTable(Table, name="my_table"):
		class ID(Column): pass
		class Name(Column): pass

	query1 = SELECT (ID, Name) - FROM ( MyTable ) - WHERE (ID < 10, Name == "parent")
	query2 = SELECT (ID, Name) - FROM - MyTable - WHERE - (ID < 10) - AND - (Name == "parent")

	assert str(query1) == "SELECT id, name FROM my_table WHERE id < ? AND name == ?", f'{str(query1)=} == {"SELECT (id, name) FROM my_table WHERE id < ? AND name == ?"=}'
	assert str(query2) == "SELECT id, name FROM my_table WHERE id < ? AND name == ?", f'{str(query2)=} == {"SELECT (id, name) FROM my_table WHERE id < ? AND name == ?"=}'

def test_database():

	from SQLOOP.core import Column, Table, VARCHAR, CHAR

	class ID(Column, name="pn", type=int): pass
	class Name(Column, type=VARCHAR(100)): pass
	class Age(Column, type=int): pass

	class PN(Column, type=int): pass
	class PhoneNumber(Column, type=CHAR(20)): pass
	class Adress(Column, type=VARCHAR(200)): pass

	class NamesTable(Table):
		A = ID
		B = Name
		C = Age

		constraints = (
			PRIMARY - KEY (ID),
		)
	
	class PhoneBookTable(Table):
		A = PN
		B = PhoneNumber
		C = Adress
	
	class NameIndex(Index):
		table = NamesTable
		A = Name

	class MyDatabase(Database):
		A = NamesTable
		B = PhoneBookTable

		C = NameIndex

	assert NamesTable in MyDatabase.tables
	assert NamesTable in MyDatabase
	assert PhoneBookTable in MyDatabase.tables
	assert PhoneBookTable in MyDatabase
	assert NameIndex not in MyDatabase.tables
	assert NameIndex in MyDatabase.indexes
	assert NameIndex in MyDatabase
	
	assert ID in NamesTable
	assert PN in PhoneBookTable
	assert ID in PhoneBookTable
	assert PN in NamesTable
	assert ID in MyDatabase
	assert PN in MyDatabase

	assert NamesTable.A in NamesTable
	assert PhoneBookTable.A in PhoneBookTable
	assert PhoneBookTable.A in PhoneBookTable
	assert NamesTable.A in NamesTable
	
	assert sql(NamesTable) == "names_table (\n\tpn INTEGER,\n\tname VARCHAR(100),\n\tage INTEGER,\n\tPRIMARY KEY (pn)\n)"
	assert sql(PhoneBookTable) == "phone_book_table (\n\tpn INTEGER,\n\tphone_number CHAR(20),\n\tadress VARCHAR(200)\n)"

	###############################################################

	database = MyDatabase(":memory:", "w")

	assert not database.valid
	# Is True if all assertions for a good database holds

	try:
		raise database.exception
	except Exception as e:
		assert isinstance(e, DatabaseSchemaEmpty)
	# Is the exception that the first broken assertion wants to raise.
	# Use is:
	# raise database.exception

	database.fix()
	# Will attempt to fix all problems which cause assertions to not hold
	
	assert database.valid

	assert list(database(SELECT * FROM - PhoneBookTable)) == []
	database(INSERT - INTO - NamesTable - (PN, Name, Age) - VALUES - (1, "Eddrik Reensen", 69))
	database(INSERT - INTO - NamesTable - (PN, Name, Age) - VALUES - (2, "Brunhilda Brunson", 68))
	
	database(INSERT - INTO - PhoneBookTable - (PN, PhoneNumber, Adress) - VALUES - (1, "+46731234567", "Råttgränd 90"))
	database(INSERT - INTO - PhoneBookTable - (PN, PhoneNumber, Adress) - VALUES - (1, "+46733025383", "Klintvägen 69"))

	assert list(database[PN][NamesTable]) == [2,1]
	assert list(database[Name][NamesTable]) == ["Brunhilda Brunson", "Eddrik Reensen"]
	assert list(database[ALL][PhoneBookTable][Name == "Eddrik Reensen"]) == [(1, "+46731234567", "Råttgränd 90"), (1, "+46733025383", "Klintvägen 69")]
	assert list(database[Name][NamesTable][PN == 1]) == "Eddrik Reensen"