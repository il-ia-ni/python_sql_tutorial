import datetime
from typing import List
import pandas as pd
from enum import Enum
from db_models.defect_root_cause import DefectRootCause
from db_models.defect_event import DefectEvent
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.types import DateTime, String
from db_connection import create_session_from_url


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
                        ).join(DefectEvent, DefectRootCause.event_id == DefectEvent.event_id)
    result = session.execute(join_query).fetchall()
    column_names = join_query.columns.keys()
    data = pd.DataFrame(data=result, columns=column_names)
    return data


def join_defect_event_root_cause_filter_event_id(event_id_to_filter: int) -> pd.DataFrame:
    join_query = select(DefectEvent.event_type,
                        DefectEvent.event_id,
                        DefectEvent.caster_id,
                        DefectEvent.strand_id,
                        DefectEvent.grade_id,
                        DefectEvent.behaviour_pattern_id,
                        DefectEvent.detection_probability,
                        DefectRootCause.signal_id,
                        DefectRootCause.importance
                        ).join(DefectEvent,
                               DefectRootCause.event_id == DefectEvent.event_id).filter(
        DefectEvent.event_id == event_id_to_filter)
    result = session.execute(join_query).fetchall()
    column_names = join_query.columns.keys()
    data = pd.DataFrame(data=result, columns=column_names)
    return data


def join_defect_event_root_cause_filter_behaviour_id(session: Session,
                                                     defects: List[BehaviourPattern],
                                                     strand_ids: List[String],
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
                        DefectRootCause.signal_data
                        ).join(DefectEvent,
                               DefectRootCause.event_id == DefectEvent.event_id)\
        .filter(DefectEvent.behaviour_pattern_id.in_(defects))\
        .filter(DefectEvent.strand_id.in_(strand_ids))\
        .filter(DefectEvent.create_date.between(start, end))
    result = session.execute(join_query).fetchall()
    column_names = join_query.columns.keys()
    data = pd.DataFrame(data=result, columns=column_names)
    return data


if __name__ == "__main__":
    app_odbc_driver = "ODBC Driver 17 for SQL Server"
    URL = "mssql+pyodbc://Admin:Very(!)Secure-Password_123@127.0.0.1:1433/ilia_test_db"
    session = create_session_from_url(url=URL, odbc_driver=app_odbc_driver)
    data = join_defect_event_root_cause()
    data_1 = join_defect_event_root_cause_filter_event_id(event_id_to_filter=19826)
    data_2 = join_defect_event_root_cause_filter_behaviour_id(session=session,
                                                              defects=[BehaviourPattern.BULGING],
                                                              strand_ids=['1_1'],
                                                              start='2022-12-20 14:58:36',
                                                              end='2022-12-20 15:58:36')
    print('')
