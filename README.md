# Etl-db-Tools

description: This package provides some convenience functions for interacting with databases. For instance executing queries, copying a table etc. It uses pyodbc as a the backend. 

### install
```
pip install etl-db-tools
```

## basic usage

#### Create a connection with a sql server database

```
from etl_db_tools.sqlservertools import sqlservertools as sql

cnxn = sql.SQLserverconnection(driver='ODBC Driver 18 for SQL Server', 
                            server='localhost_or_else', 
                            database='TestDB', 
                            uid = 'your_username',
                            pwd = 'your_password', 
                            TrustServerCertificate = 'yes')
```

#### Select data from the connection

```
query = """select id, name from dbo.myTable """
data = cnxn.select_data(query)
```

The select_data method yields a generator. This will return list of dictionaries.

#### use the data

```
for row in data:
    print(f"id = {row.get('id')}, name = {row.get('name')} ")
```

