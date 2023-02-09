from sqlalchemy import create_engine
from loguru import logger

from db_engines.db_sources_data.sqlite_test import dbdriver, dbpath

# For establishing connections see https://docs.sqlalchemy.org/en/14/tutorial/engine.html#tutorial-engine
# For configuring the engine see https://docs.sqlalchemy.org/en/14/core/engines.html#sqlite
# For working with engines see https://docs.sqlalchemy.org/en/14/core/connections.html

url_SQLite_TestDB = f"{dbdriver}:/{dbpath}"  # TODO: always check, if the sqlite-file is available at "./sqlite_db/"
test_mssg = url_SQLite_TestDB
logger.debug(f"sql_server_engine script uses url: {test_mssg}")

# Hostname connection. See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc
engine_SQLite_TestDB = create_engine(
    url_SQLite_TestDB,
    # Following args concern DB loguru_logging: https://docs.sqlalchemy.org/en/14/core/engines.html#configuring-logging,
    echo=True,  # https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine.params.echo
    echo_pool=True,  # https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine.params.echo_pool
    logging_name="SQLite test engine",
    future=True)

