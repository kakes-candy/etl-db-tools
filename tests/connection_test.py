
from etl_db_tools.sqlservertools.sqlservertools import SQLserverconnection


def test_generate_simple_string():
    cnxn = SQLserverconnection(driver='SQL Server 18 for MS', 
                               server='SQLMACHINE_01', 
                               database='Databasename')
    
    assert cnxn.to_string() == 'DRIVER={SQL Server 18 for MS};SERVER=SQLMACHINE_01;DATABASE=Databasename'


def test_can_add_random_parameter(): 
    cnxn = SQLserverconnection(driver='SQL Server 18 for MS', 
                               server='SQLMACHINE_01', 
                               database='Databasename', 
                               rando = "I'm nobody")
    
    assert cnxn.other_params.get('rando') == "I'm nobody"


def test_other_parameters_added_to_string(): 
    cnxn = SQLserverconnection(driver='SQL Server 18 for MS', 
                               server='SQLMACHINE_01', 
                               database='Databasename', 
                               rando = "nobody")
    
    assert cnxn.to_string() == 'DRIVER={SQL Server 18 for MS};SERVER=SQLMACHINE_01;DATABASE=Databasename;rando=nobody'


