import pytest
from etl_db_tools.base.schema import Column, BaseTable



@pytest.fixture(scope='function')
def create_column():
    c = Column(name='provincie', type='nvarchar', nullable=False, length=255,
               precission= 4, scale = 'scaledef', default='onbekend')
    yield c    

@pytest.fixture(scope='function')
def create_table():
    t = BaseTable(name = 'tablename')

    yield t

def test_can_create_column(create_column):

    c = create_column    

    assert c.name == 'provincie'
    assert c.type == 'nvarchar'
    assert c.nullable == False
    assert c.length == 255
    assert c.precission == 4
    assert c.scale == 'scaledef'
    assert c.default == 'onbekend'  


def test_column_can_print(create_column):
    
    c = create_column   
    assert c.__str__() == 'column: name: provincie, type: nvarchar, length: 255, precission: 4, scale: scaledef, default: onbekend'


def test_can_create_BaseTable():

    t = BaseTable(name = 'tablename')

    assert isinstance(t, BaseTable)
    assert t.name == 'tablename'
    assert len(t.columns) == 0


def test_can_add_column(create_column):
    c = create_column
    t = BaseTable(name = 'big_table', columns=[c,])

    assert isinstance(t, BaseTable)
    assert len(t.columns) == 1
    assert t.columns[0].name == 'provincie'

def test_can_add_column_method():
    c = create_column
    t = BaseTable(name = 'big_table')
    t.add_column(c)
    assert len(t.columns) == 1



def test_prints_correctly(create_column):
    c2 = create_column
    c = Column(name='stad', type = 'nvarchar', nullable=True, length= 255) 
    t = BaseTable(name = 'big_table', columns=[c, c2])

    assert t.__str__() == 'table: big_table, columns: stad, provincie'    

def test_can_only_add_column_instance():

    c = 'something_else'
    with pytest.raises(TypeError, match='columns must be instance of class column'):
        t = BaseTable(name='error_table', columns=[c,])


# Tests to check type to sql
# Integers
def test_int_to_sql():
    c = Column(name='integer', type = 'int', nullable=True, default=0)
    assert c.to_sql() == 'integer int default 0'

def test_int_not_nullable_to_sql():
    c = Column(name='integer', type = 'int', nullable=False, default=0)
    assert c.to_sql() == 'integer int not null default 0'

def test_int_without_default_to_sql():
    c = Column(name='integer', type = 'int', nullable=True, length=0)
    assert c.to_sql() == 'integer int'

def test_tinyint_to_sql():
    c = Column(name='integer', type = 'tinyint', nullable=True,  default=0)
    assert c.to_sql() == 'integer tinyint default 0'

def test_bigint_to_sql():
    c = Column(name='integer', type = 'bigint', nullable=True,  default=0)
    assert c.to_sql() == 'integer bigint default 0'

# date family
def test_date_to_sql():
    c = Column(name='datum', type = 'date', nullable=True,  default='1900-01-01')
    assert c.to_sql() == "datum date default '1900-01-01'" 

def test_date_not_nullable_to_sql():
    c = Column(name='datum', type = 'date', nullable=False,  default='1900-01-01')
    assert c.to_sql() == "datum date not null default '1900-01-01'" 


def test_datetime_to_sql():
    c = Column(name='datumtijd', type = 'datetime', nullable=True,  default='1900-01-01 10:00:00')
    assert c.to_sql() == "datumtijd datetime default '1900-01-01 10:00:00'" 

def test_date_without_default_nullable_to_sql():
    c = Column(name='datum', type = 'date', nullable=True, length=0)
    assert c.to_sql() == 'datum date'

def test_tinyint_to_sql():
    c = Column(name='datumtijd_twee', type = 'datetime2', nullable=True,  default='1900-01-01 10:00:00')
    assert c.to_sql() == "datumtijd_twee datetime2 default '1900-01-01 10:00:00'" 

def test_bigint_to_sql():
    c = Column(name='datumtijd_klein', type = 'smalldatetime', nullable=True, length=0)
    assert c.to_sql() == 'datumtijd_klein smalldatetime'

def test_decimal_five_two_to_sql():
    c = Column(name='breuk', type = 'decimal', nullable=True,  precission=5, scale=2)
    assert c.to_sql() == 'breuk decimal(5,2)'   

def test_decimal_five_two_to_not_nullable_sql():
    c = Column(name='breuk', type = 'decimal', nullable=False,  precission=5, scale=2)
    assert c.to_sql() == 'breuk decimal(5,2) not null'    

def test_decimal_five_two_default_to_sql():
    c = Column(name='breuk', type = 'decimal', nullable=True,  precission=5, scale=2, default=0.0)
    assert c.to_sql() == 'breuk decimal(5,2) default 0.0'


# Float
def test_decimal_float_with_precission_to_sql():
    c = Column(name='Floatingpoint', type = 'float', nullable=True,  precission=5, default=0.0)

    assert c.to_sql() == 'Floatingpoint float(5) default 0.0'

def test_decimal_float_without_precission_to_sql():
    c = Column(name='Floatingpoint', type = 'float', nullable=True, default=0.0)

    assert c.to_sql() == 'Floatingpoint float default 0.0'


# Character 
def test_nvarchar_with_length_to_sql():
    c = Column(name='Nvarchar', type = 'nvarchar', nullable=True, length=255)

    assert c.to_sql() == 'Nvarchar nvarchar(255)'

def test_nvarchar_should_be_max_to_sql():
    c = Column(name='Nvarchar', type = 'nvarchar', nullable=True, length=-1)

    assert c.to_sql() == 'Nvarchar nvarchar(max)'

def test_nvarchar_should_be_max_too_to_sql():
    c = Column(name='Nvarchar', type = 'nvarchar', nullable=True, length=4001)

    assert c.to_sql() == 'Nvarchar nvarchar(max)'


def test_Nchar_with_length_to_sql():
    c = Column(name='Nchar', type = 'nchar', nullable=True, length=255)

    assert c.to_sql() == 'Nchar nchar(255)'

def test_Nchar_should_be_max_to_sql():
    c = Column(name='Nchar', type = 'nchar', nullable=True, length=-1)

    assert c.to_sql() == 'Nchar nchar(max)'

def test_Nchar_should_be_max_too_to_sql():
    c = Column(name='Nchar', type = 'nchar', nullable=False, length=4001)

    assert c.to_sql() == 'Nchar nchar(max) not null'


def test_unknown_type_raises_exception():
    c = Column(name='Unknown', type = 'string', nullable=False, length=4001)
    with pytest.raises(ValueError, match='Data type not implemented'):
        c.to_sql()



def test_can_create_table():
    
    c = Column(name='tabel_id', type = 'int', nullable=False)
    t = BaseTable(name='myFirstTable', columns=[c,])

    statement_expected = """create table myFirstTable (
    tabel_id int not null
    );"""

    assert t.create_table_statement() == statement_expected

def test_can_create_table_two_columns():
    
    c = Column(name='tabel_id', type = 'int', nullable=False)
    c2 = Column(name='nummer', type = 'int', nullable=False)
    t = BaseTable(name='myFirstTable', columns=[c,c2])

    statement_expected = """create table myFirstTable (
    tabel_id int not null,
    nummer int not null
    );"""

    assert t.create_table_statement() == statement_expected


def test_can_remove_column_from_table():
        
    c = Column(name='tabel_id', type = 'int', nullable=False)
    c2 = Column(name='nummer', type = 'int', nullable=False)
    t = BaseTable(name='myFirstTable', columns=[c,c2])

    t.drop_column('nummer')

    assert len(t.columns) == 1
    assert t.column_names() == ['tabel_id']