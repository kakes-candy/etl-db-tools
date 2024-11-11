
from etl_db_tools.base.connection import Connection

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
    

