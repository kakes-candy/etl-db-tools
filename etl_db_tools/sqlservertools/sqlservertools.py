from etl_db_tools.base.connection import Connection
from collections.abc import Iterator
import pyodbc


class SQLserverconnection(Connection):
    def __init__(self, driver: str, server: str, database:str, **kwargs) -> None:
        self.driver = driver
        self.server = server
        self.database = database
        self.other_params = kwargs

    def to_string(self):

        basic_cnxn = f'DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database}'
        # make a list and add further elements
        cnxn_ls = [basic_cnxn, ]
        for k, v in self.other_params.items():
            cnxn_ls.append(f'{k}={v}')

        # make the final string
        final_cnxn = ';'.join(cnxn_ls)
        return final_cnxn
    
    def select_data(self, query: str) -> Iterator[list[dict]]:

        with pyodbc.connect(self.to_string()) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            while True:
                result = cursor.fetchmany(5000)
                columns = [column[0] for column in cursor.description]
                if result:
                    yield([dict(zip(columns, x)) for x in result])
                else:
                    break
        conn.close()    
