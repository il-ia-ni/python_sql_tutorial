import pandas as pa

from phillip.db_connection import build_full_url, make_engine, get_session
from db_engines.sql_server_engine import engine_sqlservertest_main as sqlserver_engine
from data_analysis.dataframes import create_df_from_sql_request
from data_analysis import defect_event_root_cause, defect_root_cause_pairs

from loguru import logger
from loguru_logging.debug_formatter import debug_format

import dql_scripts
import dql_scripts.simple_select as smpl_sel
import dql_scripts.select_joins as jnt_sel

debug_format()
global_session = get_session(sqlserver_engine)

# TODO: Move following lines to corresponding __name__="__main__" scripts!
# smpl_sel.select_core_signalmeta_all(global_engine)  # Selection from the table using Core API
# smpl_sel.select_orm_signalmeta_all(global_session)  # Selection from the table using ORM API

# smpl_sel.select_core_signalmeta_3cols(global_engine)  # selects 3 cols from a Core API Table obj

# smpl_sel.select_orm_signalmeta_testobjs(global_session)  # Returns a _engine.Result obj with Row objs
# smpl_sel.select_orm_signalmeta_testobjs_scalar_result(global_session)  # Returns selection result as a Scalar obj

pa_query_df = create_df_from_sql_request(
    dql_scripts.select_joins.select_join_orm_stmt1,
    sqlserver_engine,
    {"signal_id": "string", "name": "string", "model_name": "string"}
)
pa_query_df.rename_axis("SQL Query DF", axis="columns")

logger.info("A DataFrame with following parameters was created from the SQL query: \n", pa_query_df.info())

logger.info("Showing first 5 rows of the DF: \n{0}"
            .format(pa_query_df[:5])
            )
logger.info("Showing 4 columns of the first {0} rows of the DF: \n{1}"
            .format(10, pa_query_df.loc[:,
                        ["update_date", "event_id", "signal_id", "importance", "name", "model_name"]].head(10))
            )
logger.info("Showing 4 columns of the rows {0}-{1} of the DF: \n{2}"
            .format(2, 5, pa_query_df.loc[2:5, "update_date":"model_name"])
            )

logger.info("Selecting data rows with signal_id {0}: \n{1}"
            .format(2406981, pa_query_df.loc[pa_query_df.event_id == 2406981, "update_date":"model_name"])
            )
logger.info("Selecting first {0} data rows with update_date after {1}: \n{2}"
            .format(10, "2022-12-23",
                    pa_query_df.loc[lambda df: df['update_date'] > "2022-12-23", "update_date":"model_name"].head(10))
            )  # selection by callable https://pandas.pydata.org/docs/user_guide/indexing.html#indexing-callable
