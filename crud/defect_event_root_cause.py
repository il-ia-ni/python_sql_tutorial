import pandas as pd
from enum import Enum
from db_models.defect_root_cause import DefectRootCause
from db_models.defect_event import DefectEvent
from sqlalchemy import select
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


def join_defect_event_root_cause_filter_behaviour_id(defect: BehaviourPattern) -> pd.DataFrame:
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
        DefectEvent.behaviour_pattern_id == defect)
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
    data_2 = join_defect_event_root_cause_filter_behaviour_id(BehaviourPattern.BULGING)
    print('')
