from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from db_engines.db_sources_data.sql_server_test_localhost import dbdriver, dbpath, username, password, SQLServerTestDBs

Base = declarative_base()
# For establishing connections see https://docs.sqlalchemy.org/en/14/tutorial/engine.html#tutorial-engine
# For configuring the engine see https://docs.sqlalchemy.org/en/14/core/engines.html
# For working with engines see https://docs.sqlalchemy.org/en/14/core/connections.html

# See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#dialect-mssql-pyodbc-connect
url_SQLServerTestDBMS = f"{dbdriver}://{username}:{password}@{dbpath}/"
print(url_SQLServerTestDBMS + f"{SQLServerTestDBs.MASTER_DB.value}?driver=ODBC+Driver+17+for+SQL+Server")

# Hostname connection. See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc
engine_SQLServerTest_MainDB = create_engine(
    # When using a hostname connection str, the driver name must also be specified in the query parameters of the URL:
    url_SQLServerTestDBMS +
    f"{SQLServerTestDBs.MASTER_DB.value}?driver=ODBC+Driver+17+for+SQL+Server",
    # TODO: Where to store hostname DB-drivers?
    echo=True, future=True)

# connection_url_SQLServer_obj = URL.create(
#     "mssql+pyodbc",
#     username="SA",
#     password="ilzen92!",
#     host="localhost/SQLEXPRESSTEST",
#     port=1433,
#     database="master/Cracs_preventer_test",
#     query={
#         "driver": "ODBC Driver 19 for SQL Server",
#         "TrustServerCertificate": "yes"
#     },
# )

