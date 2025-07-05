import pytest
import pyodbc
import tomli
import datetime
from etl_db_tools.sqlservertools.sqlservertools import SQLserverconnection
from etl_db_tools.sqlservertools.sqlservertools import Table, Column, copy_table


with open('secrets.toml', 'rb') as t:
    config =tomli.load(t)

UID= config.get('DATABASE_ADMIN')
PWD = config.get('DATABASE_ADMIN_PW')
cs = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost;DATABASE=master;UID={UID};PWD={PWD};TrustServerCertificate=yes"

testuser = config.get('DATABASE_TESTUSER')
testuser_pw = config.get('DATABASE_TESTUSER_PW')

@pytest.fixture(scope='function')
def clean_up_schema():

    find = """
    select concat(sc.name, '.', s.[name]) as table_name
    from sys.tables as s
    inner join sys.schemas as sc
    on s.schema_id = sc.schema_id
    where 1=1
    and sc.name = 'testing'
    and s.[type] = 'U'
    order by s.object_id"""

    with pyodbc.connect(cs) as cnxn:
        cursor = cnxn.cursor()
        cursor.execute('use TestDB')
        cursor.commit()
        cursor.execute(find)
        res = cursor.fetchall()
        for r in res:
            cursor.execute(f'drop table if exists {r[0]}')
            cursor.commit()
        cursor.close()
    cnxn.close()


@pytest.fixture(scope='function')
def create_test_data(clean_up_schema):
    clean_up_schema

    with pyodbc.connect(cs) as cnxn:
        cursor = cnxn.cursor()
        cursor.execute("""
            use TestDB;
            drop table if exists testing.plaatsen;
            create table testing.plaatsen (plaats_id int, plaatsnaam nvarchar(255), provincie nvarchar(255));
            """
        )
        cursor.commit()

        cursor.execute(
            """ 
            insert into testing.plaatsen (plaats_id, plaatsnaam, provincie)
            values
            (1, 'Amsterdam', 'Noord-Holland'),
            (2, 'Den Haag', 'Zuid-Holland'), 
            (3, 'Groningen', 'Groningen'), 
            (4, 'Leeuwarden', 'Friesland')"""
            )

    cnxn.close()

@pytest.fixture(scope='function')
def create_test_data_datatypes(clean_up_schema):
    clean_up_schema
    with pyodbc.connect(cs) as cnxn:
        cursor = cnxn.cursor()
        cursor.execute("""
            use TestDB;
            drop table if exists dbo.SecondTable; 
            create table SecondTable (
            tabel_id int not null,
            nummer int default(1),
            [float] float(25) default((1.2)),
            [decimal] decimal(5,2) default((1.5)), 
            lange_nvarchar nvarchar(max), 
            normale_nvarchar nvarchar(255) default ('onbekend'),
            datum date default (getdate()), 
            datumtijd datetime default CURRENT_TIMESTAMP, 
            datumtijd_twee datetime2)""")
        cursor.commit()

    cnxn.close()


@pytest.fixture(scope='function')
def create_connection(clean_up_schema):
        clean_up_schema
        cnxn = SQLserverconnection(
             driver = 'ODBC Driver 18 for SQL Server',
             server = 'localhost',
             database='TestDB',
             UID = testuser,
             PWD = testuser_pw,
             TrustServerCertificate = 'yes'
        )

        with cnxn.connect() as active_cnxn:
            yield active_cnxn

@pytest.fixture(scope='function')
def create_connection_testuser(clean_up_schema):
        clean_up_schema
        cnxn = SQLserverconnection(
             driver = 'ODBC Driver 18 for SQL Server',
             server = 'localhost',
             database='TestDB',
             UID = testuser,
             PWD = testuser_pw,
             TrustServerCertificate = 'yes'
        )

        with cnxn.connect() as active_cnxn:
            yield active_cnxn


def test_select_returns_all_rows(create_connection, create_test_data):  
    connection = create_connection
    create_test_data

    results = connection.select_data('select * from testing.plaatsen')

    assert len(list(results)) == 4

def test_select_returns_generator(create_connection, create_test_data):
    connection = create_connection
    create_test_data
    results = connection.select_data('select top 1 * from testing.plaatsen')
    
    assert type(results).__name__ == "generator"


def test_select_row_is_dictionary(create_connection, create_test_data):
    connection = create_connection
    create_test_data
    results = connection.select_data('select top 1 * from testing.plaatsen')
    row = list(results)[0]

    assert type(row).__name__ == "dict"
    

def test_select_returns_all_columns(create_connection, create_test_data):
    connection = create_connection
    create_test_data
    results = connection.select_data('select top 1 * from testing.plaatsen')
    row = list(results)[0]

    assert list(row.keys()) == ['plaats_id', 'plaatsnaam', 'provincie']

def test_select_returns_correct_values(create_connection, create_test_data):
    connection = create_connection
    create_test_data
    results = connection.select_data('select top 1 * from testing.plaatsen')
    row = list(results)[0]

    assert list(row.values()) == [1, 'Amsterdam', 'Noord-Holland']


def test_can_create_from_query(create_test_data_datatypes, create_connection):
    create_test_data_datatypes
    connection = create_connection
    table_object = Table.from_connection(connection=connection, table_name='dbo.SecondTable')

    assert table_object.name == 'dbo.SecondTable'
    assert len(table_object.columns) == 9
    assert table_object.columns[0].name == 'tabel_id'
    assert table_object.columns[1].name == 'nummer'
    assert table_object.columns[2].name == 'float'
    assert table_object.columns[3].name == 'decimal'
    assert table_object.columns[4].name == 'lange_nvarchar'        
    assert table_object.columns[5].name == 'normale_nvarchar'
    assert table_object.columns[6].name == 'datum'
    assert table_object.columns[7].name == 'datumtijd'
    assert table_object.columns[8].name == 'datumtijd_twee'

def test_columns_contain_all(create_test_data_datatypes, create_connection, clean_up_schema):

    create_test_data_datatypes
    connection = create_connection
    table_object = Table.from_connection(connection=connection, table_name='dbo.SecondTable')

    assert table_object.columns[5].name == 'normale_nvarchar'
    assert table_object.columns[5].type == 'nvarchar'
    assert table_object.columns[5].length == 255
    assert table_object.columns[5].default == "('onbekend')"

    clean_up_schema

def test_can_check_if_table_exists(create_test_data, create_connection, clean_up_schema):
    create_test_data
    cnxn = create_connection

    assert cnxn.if_exists('testing.plaats') is False
    assert cnxn.if_exists('testing.plaatsen') is True


def test_cant_create_table_in_database_if_exists(create_connection, create_test_data):
    cnxn = create_connection
    create_test_data

    c1 = Column(name='id', type = 'int', nullable=False)
    c2 = Column(name='naam', type = 'nvarchar', nullable=True, length=255, default= 'onbekend')
    t = Table('testing.plaatsen', columns=[c1, c2])

    with pytest.warns(UserWarning ,match = "can't create table because it allready"):
        cnxn.create_table(table=t, drop_if_exists=False)

def test_can_drop_table(create_test_data, create_connection, clean_up_schema):
    
    create_test_data
    cnxn = create_connection

    assert cnxn.if_exists('testing.plaatsen') is True
    cnxn.drop_table('testing.plaatsen')
    assert cnxn.if_exists('testing.plaatsen') is False

def test_can_create_table_in_database(create_connection):
    cnxn = create_connection

    c1 = Column(name='id', type = 'int', nullable=False)
    c2 = Column(name='naam', type = 'nvarchar', nullable=True, length=255, default= 'onbekend')
    t = Table('testing.Test_table', columns=[c1, c2])

    if cnxn.if_exists('testing.Test_table') is not False:
        cnxn.drop_table('testing.Test_table')

    assert cnxn.if_exists('testing.Test_table') is False
    cnxn.create_table(table=t, drop_if_exists=True)
    assert cnxn.if_exists('testing.Test_table') is True
    

def test_can_insert_dict_into_table(create_test_data, create_connection, clean_up_schema):
    
    create_test_data
    cnxn = create_connection    

    data = [{'plaats_id': 5, 'plaatsnaam': 'Vlissingen', 'provincie': 'Zeeland'},
            {'plaats_id': 6, 'plaatsnaam': 'Eindhoven', 'provincie': 'Noord-Brabant'},]
    cnxn.sql_insert_dictionary(table = 'testing.plaatsen', data=data)

    rijen = cnxn.select_data('select * from testing.plaatsen')
    assert len(list(rijen)) == 6
    clean_up_schema

def test_can_insert_list_into_table(create_test_data, create_connection, clean_up_schema):
    
    create_test_data
    cnxn = create_connection    

    data = [[ 5, 'Vlissingen', 'Zeeland'],[ 6, 'Arnhem', 'Gelderland'],]    
    cnxn.sql_insert_list(table = 'testing.plaatsen', data=data)

    rijen = cnxn.select_data('select * from testing.plaatsen')
    assert len(list(rijen)) == 6
    clean_up_schema

def test_insert_list_only_works_if_all_columns_are_supplied(create_test_data, create_connection, clean_up_schema):
    create_test_data
    cnxn = create_connection    

    data = [[ 5, 'Vlissingen', 'Zeeland'],[ 6, 'Arnhem'],]

    with pytest.raises(Exception, match = 'expected a row with'):    
        cnxn.sql_insert_list(table = 'testing.plaatsen', data=data)

    rijen = cnxn.select_data('select * from testing.plaatsen')
    assert len(list(rijen)) == 4
    clean_up_schema


def test_can_copy_table(create_connection, create_connection_testuser):

    cnxn = create_connection
    cnxn2 = create_connection_testuser

    id = Column('id', 'int', False)
    datum = Column('datum', 'date', True)
    amount = Column(name='amount', type='decimal', nullable=True, scale=2, precission=10)

    original = Table('testing.original', columns = [id, datum, amount,])

    cnxn.create_table(original, True)

    start = datetime.datetime(2020, 1, 1)

    ls = []
    for i in range(5, 2005):
        datum = start + datetime.timedelta(days=i)
        d = {'id': i, 'datum': datum.strftime('%Y-%m-%d'), 'amount': (i*100) + (i * 0.35) }
        ls.append(d)

    cnxn.sql_insert_dictionary(table = 'testing.original', data=ls)

    copy_table(cnxn, 'testing.original', cnxn2, into= 'testing.copy')

    data_in_copy = list(cnxn.select_data('select count(1) as N, sum(amount) as total from testing.copy'))
    data_in_original = list(cnxn.select_data('select count(1) as N, sum(amount) as total from testing.original'))

    assert data_in_copy[0].get('N') == data_in_original[0].get('N')
    assert data_in_copy[0].get('total') == data_in_original[0].get('total')    



def test_list_tables_finds_tables(clean_up_schema, create_connection):
    
    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing')
    assert lijst == ['testing.tabel_een',
                    'testing.tabel_twee',
                    'testing.pinda_noot',
                    'testing.noot_pinda']
    
def test_list_tables_finds_tables_with_startswith( create_connection):
    
    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing', startswith='pinda')
    assert lijst == ['testing.pinda_noot',]

def test_list_tables_finds_tables_with_contains( create_connection):
    
    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing', contains='pinda')
    assert 'testing.pinda_noot' in lijst
    assert 'testing.noot_pinda' in lijst
    assert len(lijst) == 2


def test_list_tables_finds_tables_with_contains_and_startswith(create_connection):
    
    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing', contains='pinda', startswith='pin')
    assert 'testing.pinda_noot' in lijst
    assert 'testing.noot_pinda' not in lijst
    assert len(lijst) == 1


def test_list_tables_finds_tables_with_contains_no_match(create_connection):
    
    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing', contains='table')
    assert len(lijst) == 0

def test_list_tables_finds_tables_with_startswith_no_match(create_connection):
    
    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing', startswith='aap')
    assert len(lijst) == 0


def test_list_tables_finds_tables_with_contains_match_startswith_no_match(create_connection):
    
    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing', startswith='aap', contains='tabel')
    assert len(lijst) == 0

def test_list_tables_finds_tables_with_contains_no_match_startswith_match(create_connection):

    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('testing', startswith='tabel', contains='aap')
    assert len(lijst) == 0   


def test_list_tables_finds_tables_with_no_match_schema(create_connection):

    cnxn = create_connection

    cnxn.execute_sql('drop table if exists testing.tabel_een; create table testing.tabel_een (id int)')
    cnxn.execute_sql('drop table if exists testing.tabel_twee; create table testing.tabel_twee (id int)')
    cnxn.execute_sql('drop table if exists testing.pinda_noot; create table testing.pinda_noot (id int)')
    cnxn.execute_sql('drop table if exists testing.noot_pinda; create table testing.noot_pinda (id int)')


    lijst = cnxn.list_tables('barking')
    assert len(lijst) == 0   
