"""
This script contains methods for creating pandas DataFrames from relations of a database using sql-statements
"""

import pandas as pd
import sqlalchemy
from loguru import logger


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
