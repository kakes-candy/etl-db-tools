# Etl-db-Tools

This package provides some convenience functions for interacting with databases. For instance executing queries, copying a table etc. It uses pyodbc as a the backend. 

### install
```
pip install etl-db-tools
```

## Getting started

#### Set up a connection with a sql server database

``` python
from etl_db_tools.sqlservertools import sqlservertools as sql

cnxn = sql.SQLserverconnection(driver='ODBC Driver 18 for SQL Server', 
                            server='localhost_or_else', 
                            database='TestDB', 
                            uid = 'your_username',
                            pwd = 'your_password', 
                            TrustServerCertificate = 'yes')

```
#### Select data from an active connection
You can use the connect method as a context manager.

``` python
# create an active connection as context manager
with cnxn.connect() as active_cnxn:
    query = """select id, name from dbo.myTable """
    data = cnxn.select_data(query)
```

Select_data() yields a generator. This will return list of dictionaries containing the query results.

#### use the data

``` python
for row in data:
    print(f"id = {row['id']}")
```

