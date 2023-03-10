"""
file provides queries to extract data
"""
from typing import Set
from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd

from phillip.db_connection import create_session_from_url

create_root_cause_groups = text(
    "WITH A AS (SELECT A.create_date, A.event_id, B.signal_id, A.behaviour_pattern_id, A.strand_id, A.slab_id, B.importance "
    "FROM defect_event A INNER JOIN defect_root_cause B on A.event_id=B.event_id WHERE A.event_id IN (SELECT event_id FROM defect_root_cause "
    "WHERE signal_id NOT IN: signals_to_filter "
    "AND slab_id IS NOT null "
    "AND create_date BETWEEN: date AND GETDATE() "
    "GROUP BY event_id HAVING COUNT(event_id) > 1 AND COUNT(event_id) < 3))"
    "SELECT create_date, event_id, behaviour_pattern_id, strand_id, slab_id, importance,"
    "STUFF((SELECT ',' + CAST(signal_id AS varchar(1000)) FROM A AS innerTable "
    "WHERE innerTable.event_id = p.event_id FOR XML PATH('')),1,1,'') AS signal_pattern "
    "FROM A AS p "
    "GROUP BY p.event_id, create_date, behaviour_pattern_id, strand_id, slab_id, importance")


def root_cause_pairs_query(date: str, signals_to_filter: Set[str]) -> str:
    root_cause_groups = f"""
                                WITH A AS (SELECT A.create_date, A.event_id, B.signal_id, A.behaviour_pattern_id, A.strand_id, A.slab_id, B.importance
                                    FROM defect_event A INNER JOIN defect_root_cause B on A.event_id=B.event_id WHERE A.event_id IN (SELECT event_id FROM defect_root_cause
                                    WHERE signal_id NOT IN {signals_to_filter}
                                    AND slab_id IS NOT null
                                    AND create_date BETWEEN CAST({date} AS DATETIME) and GETDATE()
                                    GROUP BY event_id HAVING COUNT(event_id) > 1 AND COUNT(event_id) < 3))
                                    SELECT create_date, event_id, behaviour_pattern_id, strand_id, slab_id, importance,
                                    STUFF((SELECT ',' + CAST(signal_id AS varchar(1000)) FROM A AS innerTable 
                                    WHERE innerTable.event_id = p.event_id FOR XML PATH('')),1,1,'') AS signal_pattern 
                                    FROM A AS p 
                                    GROUP BY p.event_id, create_date, behaviour_pattern_id, strand_id, slab_id, importance
                                """
    return root_cause_groups


def extract_root_cause_pairs(session: Session, date: str, signals_to_filter: Set[str]) -> pd.DataFrame:
    query = root_cause_pairs_query(date=date, signals_to_filter=signals_to_filter)
    with session as sess:
        result = sess.execute(query)
        column_names = list(result.keys())
        data = pd.DataFrame(data=result, columns=column_names)
    return data


if __name__ == "__main__":
    app_odbc_driver = "ODBC Driver 17 for SQL Server"
    URL = "mssql+pyodbc://Admin:Very(!)Secure-Password_123@127.0.0.1:1433/ilia_test_db"
    session = create_session_from_url(url=URL, odbc_driver=app_odbc_driver)
    values = extract_root_cause_pairs(session=session, date='2022-01-01', signals_to_filter=(
        'casting_length_1', 'casting_length_C'))
