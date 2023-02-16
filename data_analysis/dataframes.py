"""
This script contains methods for creating pandas DataFrames from relations of a database using sql-statements
"""

import pandas as pd
import sqlalchemy
from loguru import logger

from db_engines.db_sources_data.sql_server_test_localhost import SQLServerTestDBs
from db_engines.sql_server_engine import url_SQLServerTestDBMS
from phillip.db_connection import create_session_from_url
from dql_scripts import select_joins as jnt_sel
from db_engines.sql_server_engine import engine_sqlservertest_main as sqlserver_engine


def create_df_from_list(rows_data_list: list, columns_list: list) -> pd.DataFrame:
    """
    Creates a pandas DataFrame based on a list of selection result Rows instances that were already created using f.e.
    SQL Alchemy Core API or ORM API
    :param rows_data_list: a list of Rows instances of a select result
    :param columns_list: an iterable with the strings of attributes names of the select result
    :return: a pandas dataframe with the data of the list with the selection result
    """
    result_df = pd.DataFrame(rows_data_list, columns=columns_list)
    logger.debug("A DataFrame with following parameters was created from the list: \n", result_df.info())

    return result_df


def create_df_from_sql_request(select_statement, engine: sqlalchemy.engine, dtype_dict: dict = {}) -> pd.DataFrame:
    """
    Creates a pandas DataFrame by performing a pandas sql query request based on the received select request and engine
    :param select_statement: SQL query to be executed on a DB
    :param engine: connection instance to a Database of the query, like SQLAlchemy engine
    :param dtype_dict: an optional dictionary of column names that are to be be parsed as strings instead of as objs
    :return: a pandas dataframe with the data of the query result
    """

    return pd.read_sql_query(
        # See https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html#pandas.read_sql_query
        sql=select_statement,
        con=engine,
        dtype=dtype_dict,  # otherwise strings are parsed as dtype 'object'
        # parse_dates={"update_date": "%c"},
        # parse_dates={"update_date": {"utc": True, "format": "%c"}}
    )


# For direct script execution without calling its methods in main.py:
if __name__ == "__main__":
    # More to dunder name variable __name__: https://www.pythontutorial.net/python-basics/python-__name__/
    app_odbc_driver = "ODBC Driver 17 for SQL Server"
    URL = url_SQLServerTestDBMS + SQLServerTestDBs.MASTER_DB.value
    session = create_session_from_url(url=URL, odbc_driver=app_odbc_driver)

    joins_scalar, cols1 = jnt_sel.get_select_join_orm_result(session,
                                                             jnt_sel.select_join_orm_stmt1)
    joins_rows, cols2 = jnt_sel.get_select_join_rowslist_result(sqlserver_engine,
                                                                jnt_sel.select_join_core_stmt1)
    joins_scalar2 = jnt_sel.get_select_join_orm_result(session, jnt_sel.select_join_orm_stmt2)  # Join with select.join()
    joins_scalar3 = jnt_sel.get_select_join_orm_result(session, jnt_sel.select_join_orm_stmt3)  # Join with join() and explicit ON

    # Create a DataFrame from the list of the query result Rows created with ORM API
    scalars_df = create_df_from_list(joins_scalar, cols1)
    scalars_df.rename_axis("ORM_DF", axis="columns")
    logger.info("A DataFrame with following parameters was created from the scalars list: \n", scalars_df.info())
    logger.info(scalars_df.head(5))

    # Create a DataFrame from the list of the query result Rows created with Core API
    rows_df = create_df_from_list(joins_rows, cols2)
    rows_df.rename_axis("CORE_DF", axis="columns")
    logger.info("A DataFrame with following parameters was created from the rows list: \n", rows_df.info())
    logger.info(rows_df.head(5))