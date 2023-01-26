# This script contains connection data of my local MS SQL Server 19 DBMS sql_server_test@localhost and its databases
# https://docs.sqlalchemy.org/en/14/dialects/index.html
# https://docs.sqlalchemy.org/en/14/dialects/mssql.html
from enum import Enum

dbdriver = "mssql+pyodbc"  # https://docs.sqlalchemy.org/en/14/core/engines.html#microsoft-sql-server,
# https://docs.sqlalchemy.org/en/14/dialects/mssql.html
dbpath = "localhost\\SQLEXPRESSTEST:1433"
username = "SA"
password = "ilzen92!"


# sql_server_test@localhost databases
class SQLServerTestDBs(Enum):
    MASTER_DB = "master"
    TEMP_DB = "tempdb"

