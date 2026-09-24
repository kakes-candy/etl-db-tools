import pytest
from etl_db_tools.sqlservertools.sqlservertools import (
    SQLserverconnection,
    Table,
    Column,
    copy_table,
    _parse_default,
)
from etl_db_tools.base.schema import SqlExpression


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


@pytest.mark.parametrize("table_name", ["foo", "db.schema.foo", ".foo", "schema."])
def test_copy_table_rejects_table_name_without_schema(table_name):
    with pytest.raises(ValueError, match="must be 'schema.table'"):
        copy_table(None, table_name, None)


def test_copy_table_rejects_into_without_schema():
    with pytest.raises(ValueError, match="must be 'schema.table'"):
        copy_table(None, "testing.original", None, into="foo")


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, None),
        ("((0))", "0"),
        ("((-1))", "-1"),
        ("((1.5))", "1.5"),
        ("('abc')", "abc"),
        ("(N'abc')", "abc"),
        ("('')", ""),
        ("('it''s')", "it's"),
        ("('2020-01-01')", "2020-01-01"),
    ],
)
def test_parse_default_returns_value(raw, expected):
    parsed = _parse_default(raw)
    assert parsed == expected
    assert not isinstance(parsed, SqlExpression)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("(getdate())", "getdate()"),
        ("(newid())", "newid()"),
        ("(sysdatetime())", "sysdatetime()"),
        ("(getdate()+(1))", "getdate()+(1)"),
        ("(CONVERT([bit],(0)))", "CONVERT([bit],(0))"),
    ],
)
def test_parse_default_returns_expression(raw, expected):
    parsed = _parse_default(raw)
    assert parsed == expected
    assert isinstance(parsed, SqlExpression)


def test_expression_default_is_not_quoted():
    c = Column(name="id", type="uniqueidentifier", nullable=False, default=SqlExpression("newid()"))
    assert c.to_sql() == "id uniqueidentifier not null default newid()"


def test_quote_in_string_default_is_escaped():
    c = Column(name="naam", type="nvarchar", nullable=True, length=50, default="it's")
    assert c.to_sql() == "naam nvarchar(50) default ('it''s')"


class EmptyConnection:
    def select_data(self, query):
        return iter([])


def test_from_connection_raises_if_table_not_found():
    with pytest.raises(ValueError, match="table 'testing.x' not found"):
        Table.from_connection(EmptyConnection(), "testing.x")
