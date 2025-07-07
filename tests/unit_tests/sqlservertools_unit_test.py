import pytest
from etl_db_tools.sqlservertools.sqlservertools import (
    SQLserverconnection,
    Table,
    Column,
)


def test_generate_simple_string():
    cnxn = SQLserverconnection(
        driver="SQL Server 18 for MS", server="SQLMACHINE_01", database="Databasename"
    )

    assert (
        cnxn.to_string()
        == "DRIVER={SQL Server 18 for MS};SERVER=SQLMACHINE_01;DATABASE=Databasename"
    )


def test_can_add_random_parameter():
    cnxn = SQLserverconnection(
        driver="SQL Server 18 for MS",
        server="SQLMACHINE_01",
        database="Databasename",
        rando="I'm nobody",
    )

    assert cnxn.other_params.get("rando") == "I'm nobody"


def test_other_parameters_added_to_string():
    cnxn = SQLserverconnection(
        driver="SQL Server 18 for MS",
        server="SQLMACHINE_01",
        database="Databasename",
        rando="nobody",
    )

    assert (
        cnxn.to_string()
        == "DRIVER={SQL Server 18 for MS};SERVER=SQLMACHINE_01;DATABASE=Databasename;rando=nobody"
    )


def test_insert_dictionary_checks_columns():
    cnxn = SQLserverconnection(
        driver="SQL Server 18 for MS",
        server="SQLMACHINE_01",
        database="Databasename",
        rando="nobody",
    )

    c1 = Column(name="id", type="int", nullable=False)
    c2 = Column(name="place", type="nvarchar", nullable=False, length=255)
    t = Table(name="test", columns=[c1, c2])

    data = [{"id": 1, "placemat": "New York"}]

    with pytest.raises(KeyError):
        cnxn.sql_insert_dictionary(table=t, data=data)
