from phillip.db_connection import get_session
from db_engines.sql_server_engine import engine_sqlservertest_main as sqlserver_engine

from loguru import logger
from loguru_logging.debug_formatter import debug_format

import dql_scripts
import dql_scripts.simple_select as smpl_sel
import dql_scripts.select_joins as jnt_sel

debug_format()  # adds a custom format for debug-level of loguru (saving local .log-files)
global_session = get_session(sqlserver_engine)

# TODO: Move following lines to corresponding __name__="__main__" scripts!
# smpl_sel.select_core_signalmeta_all(global_engine)  # Selection from the table using Core API
# smpl_sel.select_orm_signalmeta_all(global_session)  # Selection from the table using ORM API

# smpl_sel.select_core_signalmeta_3cols(global_engine)  # selects 3 cols from a Core API Table obj

# smpl_sel.select_orm_signalmeta_testobjs(global_session)  # Returns a _engine.Result obj with Row objs
# smpl_sel.select_orm_signalmeta_testobjs_scalar_result(global_session)  # Returns selection result as a Scalar obj



