"""
This script contains methods for creating pandas DataFrames from relations of a database using sql-statements
"""
from __future__ import annotations
import json

import pandas as pd
import sqlalchemy
from sqlalchemy.engine.row import Row, LegacyRow
from loguru import logger

from phillip.db_connection import create_session_from_url
import dql_scripts.select_joins
from db_engines.db_sources_data.sql_server_test_localhost import SQLServerTestDBs
from db_engines.sql_server_engine import url_SQLServerTestDBMS
from dql_scripts import select_joins as jnt_sel
from db_engines.sql_server_engine import engine_sqlservertest_main as sqlserver_engine


def create_df_from_sqlrows(rows_data_list: list[Row | LegacyRow], columns_list: list) -> pd.DataFrame:
    """
    Creates a pandas DataFrame based on a list of selection result Rows instances that were already created using f.e.
    SQL Alchemy Core API or ORM API
    :param rows_data_list: a list of Rows instances of a result of a selection from DB. Union type Annotation is written
    with the new Union Type Operator T1 | T2 (introduced in Python 3.10 as alternative to the old Union[T1, T2] syntax.
    See https://docs.python.org/3.10/whatsnew/3.10.html#pep-604-new-type-union-operator
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


# TODO: Write a unit test with pytest!
# TODO: Implement the rest of the logic from Phillip's Jupiter Notebook for assigning group names to the signals
# This method is tested in script plots.py!
def df_from_group(group: pd.DataFrame):
    """ Creates DataFrames from corresponding signals json-data of each event entry from the DB
    See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#sqlalchemy.dialects.mssql.JSON
    :param group: A DataFrame of events and root cause data
    :return: a formatted DataFrame of signals data for a single event
    """
    df_list = []
    for index, row in group.iterrows():
        signal_data = row['signal_data']
        # https://arctype.com/blog/json-database-when-use/
        signal_data_json = json.loads(signal_data)
        print(signal_data_json)
        signal_data_df = pd.DataFrame(signal_data_json['data'], columns=['time', signal_data_json['signal_id']])
        signal_data_df['time'] = pd.to_datetime(signal_data_df['time'])
        signal_data_df = signal_data_df.set_index('time')
        df_list.append(signal_data_df)
    df_concate = pd.concat(df_list, axis=1)
    return df_concate


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
    scalars_df = create_df_from_sqlrows(joins_scalar, cols1)
    scalars_df.rename_axis("ORM_DF", axis="columns")
    logger.info("A DataFrame with following parameters was created from the scalars list: \n", scalars_df.info())
    logger.info(scalars_df.head(5))

    # Create a DataFrame from the list of the query result Rows created with Core API
    rows_df = create_df_from_sqlrows(joins_rows, cols2)
    rows_df.rename_axis("CORE_DF", axis="columns")
    logger.info("A DataFrame with following parameters was created from the rows list: \n", rows_df.info())
    logger.info(rows_df.head(5))

    # Create a DataFrame with pandas sql_query() + display infos of the DF
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
                        pa_query_df.loc[lambda df: df['update_date'] > "2022-12-23", "update_date":"model_name"].head(
                            10))
                )  # selection by callable https://pandas.pydata.org/docs/user_guide/indexing.html#indexing-callable

