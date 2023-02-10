from loguru import logger

from db_engines.db_sources_data.sql_server_test_localhost import dbdriver, dbpath, username, password, SQLServerTestDBs
from phillip.db_connection import make_engine, build_full_url

# For establishing connections see https://docs.sqlalchemy.org/en/14/tutorial/engine.html#tutorial-engine
# For configuring the engine see https://docs.sqlalchemy.org/en/14/core/engines.html#microsoft-sql-server
# For working with engines see https://docs.sqlalchemy.org/en/14/core/connections.html

# See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#dialect-mssql-pyodbc-connect
url_SQLServerTestDBMS = f"{dbdriver}://{username}:{password}@{dbpath}/"

url_hostname = build_full_url(
    url=url_SQLServerTestDBMS + SQLServerTestDBs.MASTER_DB.value,
    odbc_driver="ODBC Driver 17 for SQL Server"
)

engine_sqlservertest_main = make_engine(url_hostname, timeout=1000, verbose=True)

