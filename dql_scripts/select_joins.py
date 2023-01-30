import sqlalchemy
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB
from ddl_scripts.creating_tables import signal_meta, SignalMeta, DefectEvent, DefectRootCause

from loguru import logger

SessionTestSQLServer = sessionmaker(bind=engine_SQLServerTest_MainDB)  # TODO: Is this supposed to be stored centrally?
session2 = SessionTestSQLServer.begin()
session1 = Session(engine_SQLServerTest_MainDB)

select_join_orm_stmt1 = (
    # JOIN with left and right explicit sides + automatic ON-clause
    # See https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#explicit-from-clauses-and-joins
    select(DefectRootCause)
    .join_from(DefectRootCause, DefectEvent)
    .join_from(DefectRootCause, SignalMeta)
    .where(DefectRootCause.event_id == 2407113)
)



select_join_orm_stmt2 = (
    # JOIN with only right-hand explicit side + automatic ON-clause
    select(DefectRootCause)
    .join(DefectEvent)
    .join(SignalMeta)
    .where(DefectRootCause.event_id == 2407026)
)


select_join_orm_stmt3 = (
    # JOIN with only right-hand explicit side + explicit ON-clause
    # See https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#setting-the-on-clause
    select(DefectRootCause)
    .join(DefectEvent, DefectRootCause.event_id == DefectEvent.event_id)
    .join(SignalMeta, DefectRootCause.signal_id == SignalMeta.id)
    .where(DefectRootCause.event_id == 2407022)
)


def get_select_join_orm_result(session: sqlalchemy.orm.session, select_stmt):
    with SessionTestSQLServer.begin() as session3:
        logger.trace(f"Using following select-statement: {select_stmt}")

        result = session3.scalars(select_stmt).all()
        for ormObj in result:
            print(ormObj)

