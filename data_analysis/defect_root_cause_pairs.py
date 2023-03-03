""" This script contains methods to analyse the defect root cause based on prepared signal patterns """
from __future__ import annotations

from enum import Enum

from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session
import pandas as pd
from loguru import logger

from phillip.db_connection import create_session_from_url
from db_engines.db_sources_data.sql_server_test_localhost import SQLServerTestDBs
from db_engines.sql_server_engine import url_SQLServerTestDBMS as sql_server_url
from db_engines.sqlite_engine import url_hostname as sqlite_url


class Casters(str, Enum):
    """Defines allowed casters' names"""
    CASTER_1 = "1"
    CASTER_2 = "2c"


# non-scalar variables (Sets, Tuples, Lists can be added to text queries can be added using expanding bind parameters:
# https://stackoverflow.com/a/56382828
# https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.text
create_root_cause_groups_text_clause = text(
    "WITH A AS ("
    "   SELECT A.create_date, A.event_id, B.signal_id, A.behaviour_pattern_id, A.strand_id, A.slab_id, B.importance "
    "   FROM main.defect_event A INNER JOIN main.defect_root_cause B "
    "       on A.event_id=B.event_id "
    "   WHERE A.caster_id IN :casters AND A.event_id IN ("
    "       SELECT event_id "
    "       FROM main.defect_root_cause "
    "       WHERE signal_id NOT in :signals "
    "           AND slab_id IS NOT null "
    "           AND create_date BETWEEN CAST(:start_date AS DATE) AND GETDATE() "
    "       GROUP BY event_id HAVING COUNT(event_id) > 1 AND COUNT(event_id) < 3)"
    "   )"
    "SELECT "
    "   create_date, "
    "   event_id, "
    "   behaviour_pattern_id, "
    "   strand_id, "
    "   slab_id, "
    "   importance,"
    "   STUFF((SELECT ',' + CAST(signal_id AS varchar(1000)) "
    "       FROM A AS innerTable "
    "       WHERE innerTable.event_id = p.event_id "
    "       FOR XML PATH('')),1,1,'') "
    "AS signal_pattern "
    "FROM A AS p "
    "GROUP BY p.event_id, create_date, behaviour_pattern_id, strand_id, slab_id, importance")


def root_cause_pairs_query(date: str, casters: tuple[str, ...] | str, signals: tuple[str, ...] | str) -> str:
    # tuple[str] vs tuple[str, ...] see: https://stackoverflow.com/questions/72001132/python-typing-tuplestr-vs-tuplestr

    # f"" is formatted string literal. An overview of Python String formatting: https://realpython.com/python-f-strings/
    # f""" """ allows writing a multiline string with no escape character \ before each new line
    root_cause_groups = f"""
    WITH A AS (
        SELECT A.create_date, A.event_id, B.signal_id, A.behaviour_pattern_id, A.strand_id, A.slab_id, B.importance
        FROM main.defect_event A INNER JOIN main.defect_root_cause B 
            on A.event_id=B.event_id 
        WHERE A.caster_id in {casters} AND A.event_id IN (
            SELECT event_id 
            FROM main.defect_root_cause
            WHERE signal_id NOT IN {signals}
                AND slab_id IS NOT null
                AND create_date BETWEEN CAST(\'{date}' AS DATE) and GETDATE()
            GROUP BY event_id 
            HAVING COUNT(event_id) > 1 AND COUNT(event_id) < 3
        )
    )
    SELECT p.create_date, p.event_id, p.behaviour_pattern_id, p.strand_id, p.slab_id, p.importance, STUFF(
        (SELECT ',' + CAST(innerTable.signal_id AS varchar(1000)) FROM A AS innerTable 
        WHERE innerTable.event_id = p.event_id FOR XML PATH('')),1,1,'') 
    AS signal_pattern 
    FROM A AS p 
    GROUP BY p.event_id, p.create_date, p.behaviour_pattern_id, p.strand_id, p.slab_id, p.importance
    """
    return root_cause_groups


def extract_root_cause_pairs(
        session: Session,
        date: str,
        casters_to_filter: tuple[Casters, ...] | Casters,
        signals_to_filter: tuple[str, ...] | str
) -> pd.DataFrame:
    # tuple[str] vs tuple[str, ...] see: https://stackoverflow.com/questions/72001132/python-typing-tuplestr-vs-tuplestr

    # Option 1:
    # Solution for binding non-scalar vars as param of string queries: https://stackoverflow.com/a/56382828
    # text_statement = create_root_cause_groups_text_clause.bindparams(
    #     bindparam('casters', expanding=True),
    #     bindparam('signals', expanding=True)
    # )

    # Option 2:
    if isinstance(casters_to_filter, tuple):
        query = root_cause_pairs_query(
            date=date, signals=signals_to_filter,
            casters=tuple(map(lambda x: x.value, casters_to_filter))  # https://realpython.com/python-lambda/#classic-functional-constructs
        )
    elif isinstance(casters_to_filter, str):  # TODO: for f-String statements there has to be another statement with no "with in"!
        query = root_cause_pairs_query(date=date, signals=signals_to_filter, casters=tuple(casters_to_filter))
    with session as s:
        # result = s.execute(text_statement, params={
        #     'start_date': date,
        #     'casters': casters_to_filter,
        #     'signals': signals_to_filter
        # })
        result = s.execute(query)

        column_names = list(result.keys())
        return pd.DataFrame(data=result, columns=column_names)


# For direct script execution without calling its methods in main.py:
if __name__ == "__main__":
    # More to dunder name variable __name__: https://www.pythontutorial.net/python-basics/python-__name__/
    app_odbc_driver = "ODBC Driver 17 for SQL Server"
    URL = sqlite_url
    session = create_session_from_url(url=URL, odbc_driver=app_odbc_driver)
    data = extract_root_cause_pairs(
        session=session,
        date='2022-12-23',
        casters_to_filter=(Casters.CASTER_1, Casters.CASTER_1),  # TODO: for f-String statements there needs to be another implementation for single values
        signals_to_filter=("casting_length_1", "casting_length_C")  # TODO: for f-String statements there needs to be another implementation for single values
    )
    logger.info(data.info())
    logger.info(data)
