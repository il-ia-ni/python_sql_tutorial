from phillip.db_connection import get_session
from db_engines.sql_server_engine import engine_sqlservertest_main as sqlserver_engine

from loguru import logger
from loguru_logging.debug_formatter import debug_format

debug_format()  # adds a custom format for debug-level of loguru (saving local .log-files)
global_session = get_session(sqlserver_engine)

