
from abc import ABC
from collections.abc import Iterator
from etl_db_tools.base.schema import BaseTable

class Connection(ABC):

    def __init__(self) -> None:
        super().__init__()


    def to_string(self):
        pass

    def select_data(self, query: str) -> Iterator[list[dict]]:
        pass    
    
    def execute_sql(self, query):
        pass

    def if_exists(self, table_name):
        pass    

    def create_table(self, table: BaseTable, drop_if_exists:bool):
        pass

    def sql_insert_dictionary(self, table: str | BaseTable, data:list[dict]):
        pass

    def sql_insert_list(self, table: str | BaseTable, data: list[list]):
        pass

    def __print__(self):
        pass