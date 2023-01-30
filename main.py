from db_connection import build_full_url, make_engine, get_session
from db_engines.sql_server_engine import url_SQLServerTestDBMS
from db_engines.db_sources_data.sql_server_test_localhost import dbdriver, dbpath, username, password, SQLServerTestDBs

from loguru import logger
from loguru_logging.debug_formatter import debug_format

import dql_scripts.simple_select as smpl_sel
import dql_scripts.select_joins as jnt_sel

debug_format()
url = build_full_url(url_SQLServerTestDBMS + SQLServerTestDBs.MASTER_DB.value + "?driver=ODBC+Driver+17+for+SQL+Server")
global_engine = make_engine(url, 1000)
global_session = get_session(global_engine)

# smpl_sel.select_core_signalmeta_all(global_engine)  # Selection from the table using Core API
# smpl_sel.select_orm_signalmeta_all(global_session)  # Selection from the table using ORM API

# smpl_sel.select_core_signalmeta_3cols(global_engine)  # selects 3 cols from a Core API Table obj

# smpl_sel.select_orm_signalmeta_testobjs(global_session)  # Returns a _engine.Result obj with Row objs
# smpl_sel.select_orm_signalmeta_testobjs_scalar_result(global_session)  # Returns selection result as a Scalar obj

# jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt1)  # Join with select.join_from()
# jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt2)  # Join with select.join()
jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt3)  # Join with join() and explicit ON

