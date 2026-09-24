from etl_db_tools.base.connection import Connection
from etl_db_tools.base.schema import BaseTable, Column, sql_render
from collections.abc import Iterator
from contextlib import contextmanager
import pyodbc
import warnings
import logging
from azure.identity import AzureCliCredential
import struct

logger = logging.getLogger(__name__)

class Column(Column):
    def __init__(
        self,
        name: str,
        type: str,
        nullable: bool,
        length: int = None,
        precission: int = None,
        scale=None,
        default=None,
    ) -> None:
        super().__init__(name, type, nullable, length, precission, scale, default)

    def quoted_name(self):
        return f"[{self.name}]"


class Table(BaseTable):
    def __init__(self, name, columns: list[Column] = None) -> None:
        super().__init__(name, columns)

    @classmethod
    def from_connection(cls, connection: Connection, table_name: str):
        querystring = f"""
                select 
                c.TABLE_NAME
                ,c.COLUMN_NAME
                ,c.DATA_TYPE
                ,convert(bit, iif(c.IS_NULLABLE = 'YES' , 1, 0)) as IS_NULLABLE
                ,c.CHARACTER_MAXIMUM_LENGTH
                ,c.NUMERIC_PRECISION
                ,c.NUMERIC_SCALE
                ,case 
                    when c.COLUMN_DEFAULT = '(getdate())' then 'getdate()'
                    when left(c.COLUMN_DEFAULT, 3) = '(N''' then substring(c.COLUMN_DEFAULT, 4, len(c.COLUMN_DEFAULT)-5) 
                    else substring(c.COLUMN_DEFAULT, 3, len(c.COLUMN_DEFAULT)-4) 
                    end as COLUMN_DEFAULT
                from information_schema.columns as c 
                where CONCAT(c.TABLE_SCHEMA, '.', c.table_name) = '{table_name}'
                order by c.ORDINAL_POSITION"""

        logger.debug("query is: %s", querystring)

        res_gen = connection.select_data(querystring)

        columns = []
        for column in res_gen:
            c = Column(
                name=column.get("COLUMN_NAME"),
                type=column.get("DATA_TYPE"),
                nullable=column.get("IS_NULLABLE"),
                length=column.get("CHARACTER_MAXIMUM_LENGTH"),
                precission=column.get("NUMERIC_PRECISION"),
                scale=column.get("NUMERIC_SCALE"),
                default=column.get("COLUMN_DEFAULT"),
            )
            logger.debug('Column def: %s', c)
            columns.append(c)

        # make instance from output column definition
        return cls(name=table_name, columns=columns)


class SQLserverconnection(Connection):
    def __init__(self, driver: str, server: str, database: str, **kwargs) -> None:
        self.driver = driver
        self.server = server
        self.database = database
        self.other_params = kwargs

    def to_string(self):
        basic_cnxn = (
            f"DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database}"
        )
        # make a list and add further elements
        cnxn_ls = [
            basic_cnxn,
        ]
        for k, v in self.other_params.items():
            cnxn_ls.append(f"{k}={v}")

        # make the final string
        final_cnxn = ";".join(cnxn_ls)
        return final_cnxn

    @contextmanager
    def connect(self):
        self.connection = pyodbc.connect(self.to_string())
        self.cursor = self.connection.cursor()
        try:
            yield (self)
        finally:
            if self.cursor is not None:
                self.cursor.close()
                self.cursor = None
            if self.connection is not None:
                self.connection.close()
                self.connection = None

    def select_data(self, query: str) -> Iterator[list[dict]]:
        self.cursor.execute(query)
        columns = [column[0] for column in self.cursor.description]
        while True:
            results = self.cursor.fetchmany(5000)
            if not results:
                break
            for result in results:
                yield dict(zip(columns, result))

    def execute_sql(self, query):
        self.cursor.execute(query)
        self.cursor.commit()

    def if_exists(self, table_name):
        query = f"""
        select s.[name], object_id, sc.name
        from sys.tables as s
        inner join sys.schemas as sc
        on s.schema_id = sc.schema_id
        where concat(sc.name, '.', s.name) = '{table_name}'
        and s.[type] = 'U'"""

        res = self.select_data(query)
        if len(list(res)) > 0:
            return True
        elif len(list(res)) == 0:
            return False

    def drop_table(self, table_name: str) -> None:
        q = f"drop table if exists {table_name} ;"
        self.execute_sql(q)

    def create_table(self, table: Table, drop_if_exists: bool):
        if self.if_exists(table_name=table.name) and drop_if_exists is False:
            warnings.warn(
                "can't create table because it allready exists and drop_if_exists is False"
            )
            return
        elif self.if_exists(table_name=table.name) and drop_if_exists is True:
            self.drop_table(table_name=table.name)

        q = table.create_table_statement()
        self.execute_sql(q)

    def sql_insert_dictionary(self, table: str | Table, data: list[dict]):
        if isinstance(table, str):
            table_obj = Table.from_connection(self, table)
        elif isinstance(table, Table):
            table_obj = table

        # All keys must be a column name, if not raise error
        data_columns = data[0].keys()
        for c in data_columns:
            if c not in table_obj.column_names():
                raise KeyError(
                    f"data column {c} does not match any column in the table object"
                )

        # only keep columns in the table object that are in the dataset
        table_obj.columns = [c for c in table_obj.columns if c.name in data_columns]

        # render insert statement
        insert_sql = sql_render(template="insert.sql", data=table_obj)

        cleaned_data = []
        for row in data:
            rowlist = [row.get(x) for x in table_obj.column_names()]
            cleaned_data.append(rowlist)

        self.cursor.fast_executemany = True
        self.cursor.executemany(insert_sql, cleaned_data)
        self.cursor.commit()

    def sql_insert_list(self, table: str | Table, data: list[list]):
        if isinstance(table, str):
            table_obj = Table.from_connection(self, table)
        elif isinstance(table, Table):
            table_obj = table

        # lists must have the same length as n columns
        N = len(table_obj.columns)
        for li in data:
            len_li = len(li)
            if len_li != N:
                raise Exception("expected a row with {N} values, got {len_li}.")

        # render insert statement
        insert_sql = sql_render(template="insert.sql", data=table_obj)

        self.cursor.fast_executemany = True
        self.cursor.executemany(insert_sql, data)
        self.cursor.commit()

    def list_tables(
        self, schema: str, startswith: str = None, contains: str = None
    ) -> list[str]:
        data = {"schema": schema, "startswith": startswith, "contains": contains}

        q = sql_render(template="find_tables.sql", data=data)
        ls = self.select_data(q)
        ls_flat = [x.get("table_name") for x in ls]
        return ls_flat


class AzureSqlServerConnection(SQLserverconnection):


    @staticmethod
    def process_az_login_token():

        # attribute id
        SQL_COPT_SS_ACCESS_TOKEN = 1256 

        # Use the cli credential to get a token after the user has signed in via the Azure CLI 'az login' command.
        credential = AzureCliCredential()
        databaseToken = credential.get_token('https://database.windows.net/')

        # get bytes from token obtained
        tokenb = bytes(databaseToken[0], "UTF-8")
        exptoken = b''
        for i in tokenb:
            exptoken += bytes({i})
            exptoken += bytes(1)
            tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

        # return encoded token as attribute dict
        return({SQL_COPT_SS_ACCESS_TOKEN:tokenstruct})
        

    @contextmanager
    def connect(self):
        self.connection = pyodbc.connect(self.to_string(), attrs_before = self.process_az_login_token())
        self.cursor = self.connection.cursor()
        try:
            yield (self)
        finally:
            if self.cursor is not None:
                self.cursor.close()
                self.cursor = None
            if self.connection is not None:
                self.connection.close()
                self.connection = None


def _check_table_name(name: str) -> None:
    parts = name.split(".")
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"table name must be 'schema.table', got '{name}'")


# All steps involved in copying a table
def copy_table(
    source_connection: Connection,
    table_name: str,
    target_connection: Connection,
    into: str = None,
) -> None:
    # override name if asked
    if into is not None:
        target_name = into
    else:
        target_name = table_name

    _check_table_name(table_name)
    _check_table_name(target_name)

    temp_name = f"{target_name}_temporary"

    # Get the table definition
    table = Table.from_connection(source_connection, table_name)

    # set the name to temporary name
    table.name = temp_name

    # create the table in the target database, drop if exists
    target_connection.create_table(table, drop_if_exists=True)

    try:
        if source_connection is target_connection:
            # one connection can't read and insert at the same time, so copy
            # the data inside the database instead
            cols = ", ".join(c.quoted_name() for c in table.columns)
            target_connection.execute_sql(
                f"insert into {temp_name} ({cols}) select {cols} from {table_name}"
            )
        else:
            # get the data
            generator = source_connection.select_data(f"select * from {table_name}")

            # write data from the generator in chunks to the temp table
            chunk = []
            chunk_length = 1000
            i = 0
            for row in generator:
                chunk.append(row)
                i += 1
                if i == chunk_length:
                    target_connection.sql_insert_dictionary(table, chunk)
                    chunk.clear()
                    i = 0
            if i > 0 and i < chunk_length:
                target_connection.sql_insert_dictionary(table, chunk)

        # now switch by dropping the old table and renaming the temp table
        # note: we must remove the schema from the name as it will mess up the sp_rename call
        data = {"target_name": target_name.split(".")[-1], "target_schema": target_name.split(".")[0],   "temp_table_name": temp_name}
        q = sql_render("finalize_copy.sql", data=data)

        target_connection.execute_sql(q)
    except Exception:
        # roll back first, so a half-finished finalize (target dropped, rename failed)
        # is undone instead of committed by the cleanup below
        try:
            target_connection.cursor.rollback()
            target_connection.drop_table(temp_name)
        except Exception:
            logger.exception("could not drop temporary table %s", temp_name)
        raise
