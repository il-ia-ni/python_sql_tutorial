""" This script contains methods to analyse the defect root cause based on defect event """
from enum import Enum
from typing import List
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.types import DateTime
from sqlalchemy.orm import Session
import pandas as pd
from loguru import logger

from db_engines.db_sources_data.sql_server_test_localhost import SQLServerTestDBs
from db_engines.sql_server_engine import url_SQLServerTestDBMS
from ddl_scripts.creating_tables import DefectEvent, DefectRootCause
from phillip.db_connection import create_session_from_url


class BehaviourPattern(str, Enum):
    """Defines known behavior patterns"""

    BULGING = "bulging"
    CLOGGING = "clogging"
    TUNDISH_VORTEX = "tundish_vortex"


def join_defect_event_root_cause() -> pd.DataFrame:
    join_query = select(DefectEvent.event_type,
                        DefectEvent.event_id,
                        DefectEvent.caster_id,
                        DefectEvent.strand_id,
                        DefectEvent.grade_id,
                        DefectEvent.behaviour_pattern_id,
                        DefectEvent.detection_probability,
                        DefectRootCause.event_id,
                        DefectRootCause.signal_id,
                        DefectRootCause.importance
                        )\
        .join(DefectEvent, DefectRootCause.event_id == DefectEvent.event_id)

    result = session.execute(join_query).fetchall()
    column_names = join_query.columns.keys()
    return pd.DataFrame(data=result, columns=column_names)


def join_defect_event_root_cause_filter_event_id(*event_id_to_filter) -> pd.DataFrame:
    # *event_id_to_filter is a Python splat operator. See @ https://realpython.com/python-kwargs-and-args/
    join_query = select(DefectEvent.event_type,
                        DefectEvent.event_id,
                        DefectEvent.caster_id,
                        DefectEvent.strand_id,
                        DefectEvent.grade_id,
                        DefectEvent.behaviour_pattern_id,
                        DefectEvent.detection_probability,
                        DefectRootCause.signal_id,
                        DefectRootCause.importance
                        )\
        .join(DefectEvent, DefectRootCause.event_id == DefectEvent.event_id)\
        .filter(DefectEvent.event_id.in_(event_id_to_filter))

    result = session.execute(join_query).fetchall()
    column_names = join_query.columns.keys()
    return pd.DataFrame(data=result, columns=column_names)


def join_defect_event_root_cause_filter_behaviour_id(session: Session,
                                                     defects: List[BehaviourPattern],
                                                     strand_ids: List[str],
                                                     start: DateTime(),
                                                     end: DateTime()) -> pd.DataFrame:
    join_query = select(DefectEvent.create_date,
                        DefectEvent.event_type,
                        DefectEvent.event_id,
                        DefectEvent.caster_id,
                        DefectEvent.strand_id,
                        DefectEvent.grade_id,
                        DefectEvent.behaviour_pattern_id,
                        DefectEvent.detection_probability,
                        DefectRootCause.signal_id,
                        DefectRootCause.importance,
                        )\
        .join(DefectEvent, DefectRootCause.event_id == DefectEvent.event_id) \
        .filter(DefectEvent.behaviour_pattern_id.in_(defects)) \
        .filter(DefectEvent.strand_id.in_(strand_ids)) \
        .filter(DefectEvent.create_date.between(start, end))

    result = session.execute(join_query).fetchall()
    column_names = join_query.columns.keys()
    return pd.DataFrame(data=result, columns=column_names)


# For direct script execution without calling its methods in main.py:
if __name__ == "__main__":
    # More to dunder name variable __name__: https://www.pythontutorial.net/python-basics/python-__name__/
    app_odbc_driver = "ODBC Driver 17 for SQL Server"
    URL = url_SQLServerTestDBMS + SQLServerTestDBs.MASTER_DB.value
    session = create_session_from_url(url=URL, odbc_driver=app_odbc_driver)
    data = join_defect_event_root_cause()
    data_1 = join_defect_event_root_cause_filter_event_id(19826, 2452843, 2406977, 2452847)
    data_2 = join_defect_event_root_cause_filter_behaviour_id(session=session,
                                                              defects=[BehaviourPattern.BULGING],
                                                              strand_ids=['1_1'],
                                                              start=datetime.strptime('2022-12-23 01:58:36',
                                                                                      '%Y-%m-%d %H:%M:%S'),
                                                              end=datetime.strptime('2022-12-23 02:58:36',
                                                                                    '%Y-%m-%d %H:%M:%S'))

    logger.info(data)
    logger.info(data_1)
    logger.info(data_2)

