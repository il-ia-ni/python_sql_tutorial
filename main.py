from sqlalchemy import inspect

from db_connection import build_full_url, make_engine, get_session
from db_engines.sql_server_engine import url_SQLServerTestDBMS
from db_engines.db_sources_data.sql_server_test_localhost import dbdriver, dbpath, username, password, SQLServerTestDBs

from loguru import logger
from loguru_logging.debug_formatter import debug_format

import pandas as pa

import dql_scripts.simple_select as smpl_sel
import dql_scripts.select_joins as jnt_sel
from ddl_scripts.creating_tables import SignalMeta
from ddl_scripts.adding_data import add_new_objs, test_dataobj1, test_dataobj2

debug_format()
url = build_full_url(url_SQLServerTestDBMS + SQLServerTestDBs.MASTER_DB.value + "?driver=ODBC+Driver+17+for+SQL+Server")
global_engine = make_engine(url, 1000)
global_session = get_session(global_engine)
global_session.expire_on_commit = False  # To avoid DetachedInstanceError when loading els from a DataFrame
# https://docs.sqlalchemy.org/en/14/errors.html#error-bhk3

# smpl_sel.select_core_signalmeta_all(global_engine)  # Selection from the table using Core API
# smpl_sel.select_orm_signalmeta_all(global_session)  # Selection from the table using ORM API

# smpl_sel.select_core_signalmeta_3cols(global_engine)  # selects 3 cols from a Core API Table obj

# smpl_sel.select_orm_signalmeta_testobjs(global_session)  # Returns a _engine.Result obj with Row objs
# smpl_sel.select_orm_signalmeta_testobjs_scalar_result(global_session)  # Returns selection result as a Scalar obj

joins_scalar = jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt1)  # Join with select.join_from()
joins_rows = jnt_sel.get_select_join_rowslist_result(global_session, jnt_sel.select_join_orm_stmt1)  # Join with select.join_from()
# joins_scalar2 = jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt2)  # Join with select.join()
# joins_scalar3 = jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt3)  # Join with join() and explicit ON

joins_df1 = pa.DataFrame(joins_scalar)
logger.info(f"Instances of scalar_joins list are inspected for detached-status... The result is: {inspect(joins_scalar[0]).detached}")  # State Inspection of ORM Instances is possible
# For State Management see @ https://docs.sqlalchemy.org/en/14/orm/session_state_management.html#session-object-states
joins_df1.info()

joins_df2 = pa.DataFrame(joins_rows)
logger.info(f"Instances of rows_joins list are inspected for detached-status... The result is: {inspect(joins_rows[0]).detached}")   # State Inspection of Engine.Row Instances is not possible
joins_df2.info()

# """ Test adding and deleting new instances of signalMeta ORM cls"""
# add_new_objs(global_session, test_dataobj1)
# add_new_objs(global_session, test_dataobj2)
#
# test_obj1 = global_session.get(SignalMeta, "test_obj_1")
# logger.info(f"Fetching the following test obj: {test_obj1}")  # TODO: trace severity requires a custom formatter
#
# test_obj2 = global_session.get(SignalMeta, "test_obj_2")
# logger.info(f"Fetching the following test obj: {test_obj2}")  # TODO: trace severity requires a custom formatter
#
# global_session.delete(test_obj1)
# logger.debug(f"Deleting test obj2: {test_obj1}")
# global_session.delete(test_obj2)
# logger.debug(f"Deleting test obj2: {test_obj2}")
#
# global_session.commit()

