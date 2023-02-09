# This script contains connection data of my test SQLite DB testdn
# https://docs.sqlalchemy.org/en/14/dialects/index.html
# https://docs.sqlalchemy.org/en/14/dialects/sqlite.html

# TODO: always check, if the sqlite-file is available at "../sqlite_db/"
from config.definitions import ROOT_DIR  # Found @ https://towardsdatascience.com/simple-trick-to-work-with-relative-paths-in-python-c072cdc9acb9

dbdriver = "sqlite+pysqlite"  # https://docs.sqlalchemy.org/en/14/core/engines.html#sqlite
# pysqlite dialect: https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#pysqlite
# It uses Python module sqlite3 (by default -> "pysqlite" is opt): https://docs.python.org/3/library/sqlite3.html
dbpath = f"//{ROOT_DIR}\\db_engines\\sqlite_db\\testdb.sqlite"

