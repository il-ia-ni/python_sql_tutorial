from sqlalchemy import inspect

import dql_scripts.select_joins
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

# smpl_sel.select_core_signalmeta_all(global_engine)  # Selection from the table using Core API
# smpl_sel.select_orm_signalmeta_all(global_session)  # Selection from the table using ORM API

# smpl_sel.select_core_signalmeta_3cols(global_engine)  # selects 3 cols from a Core API Table obj

# smpl_sel.select_orm_signalmeta_testobjs(global_session)  # Returns a _engine.Result obj with Row objs
# smpl_sel.select_orm_signalmeta_testobjs_scalar_result(global_session)  # Returns selection result as a Scalar obj

joins_scalar = jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt1)  # Join with select.join_from()
joins_rows = jnt_sel.get_select_join_rowslist_result(global_session, jnt_sel.select_join_orm_stmt1)  # Join with select.join_from()
# joins_scalar2 = jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt2)  # Join with select.join()
# joins_scalar3 = jnt_sel.get_select_join_orm_result(global_session, jnt_sel.select_join_orm_stmt3)  # Join with join() and explicit ON

scalars_df = pa.DataFrame(joins_scalar)  # This receives a list of ORM instances in form of __repr__(self) print
logger.info("A DataFrame with following parameters was created from the scalars list: \n", scalars_df.info())
logger.info(scalars_df.head(5))

rows_df = pa.DataFrame(joins_scalar)  # This receives a list of Core.Row instances
logger.info("A DataFrame with following parameters was created from the rows list: \n", rows_df.info())
logger.info(rows_df.head(5))

pa_query_df = pa.read_sql_query(sql=dql_scripts.select_joins.select_join_orm_stmt1, con=global_engine)
logger.info("A DataFrame with following parameters was created from the SQL query: \n", pa_query_df.info())

logger.info("Showing first 5 rows of the DF: \n{0}"
            .format(pa_query_df[:5])
            )
logger.info("Showing 4 columns of the first {0} rows of the DF: \n{1}"
            .format(10, pa_query_df.loc[:, ["update_date", "event_id", "signal_id", "importance"]].head(10))
            )
logger.info("Showing 4 columns of the rows {0}-{1} of the DF: \n{2}"
            .format(2, 5, pa_query_df.loc[2:5, "update_date":"importance"])
            )

logger.info("Selecting data rows with signal_id {0}: \n{1}"
            .format(2406981, pa_query_df.loc[pa_query_df.event_id == 2406981, "update_date":"importance"])
            )
logger.info("Selecting first {0} data rows with update_date after {1}: \n{2}"
            .format(10, "2022-12-23",
                    pa_query_df.loc[lambda df: df['update_date'] > "2022-12-23", :].head(10))
            )  # selection by callable https://pandas.pydata.org/docs/user_guide/indexing.html#indexing-callable

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

