import sqlalchemy
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ddl_scripts.creating_tables import signal_meta, defect_event, defect_root_cause, SignalMeta, DefectEvent, DefectRootCause

from loguru import logger

""" AREA of JOIN-Statements CORE API """


select_join_core_stmt1 = (
    select(defect_root_cause.c.update_date, defect_root_cause.c.event_id, defect_root_cause.c.signal_id,
           defect_root_cause.c.importance, signal_meta.c.name, defect_event.c.model_name)
    .join_from(defect_root_cause, signal_meta)  # https://docs.sqlalchemy.org/en/14/core/selectable.html#sqlalchemy.sql.expression.Select.join_from
    .join_from(defect_root_cause, defect_event)
)

def get_select_join_rowslist_result(engine: sqlalchemy.engine, select_stmt):
    with engine.connect() as connection:
        logger.debug(f"Choosing joined data with following select-statement: {select_stmt}")

        result = connection.execute(select_stmt).all()  # Return all rows in a list
        logger.info("Session.execute() creates a list of instances of type {0}. An example of the first instance:\n{1}"
                    .format(type(result[0]), result[0])
                    )
        return result


""" AREA JOIN-Statements with ORM API"""


select_join_orm_stmt1 = (
    # JOIN with left and right explicit sides + automatic ON-clause
    # See https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#explicit-from-clauses-and-joins
    select(DefectRootCause.update_date, DefectRootCause.event_id, DefectRootCause.signal_id, DefectRootCause.importance,
           SignalMeta.name, DefectEvent.model_name)
    .join_from(DefectRootCause, DefectEvent)  # https://docs.sqlalchemy.org/en/14/core/selectable.html#sqlalchemy.sql.expression.Select.join_from
    .join_from(DefectRootCause, SignalMeta)
    # .where(DefectRootCause.event_id == 2407113)
)


select_join_orm_stmt2 = (
    # JOIN with only right-hand explicit side + automatic ON-clause
    select(DefectRootCause)
    .join(DefectEvent)
    .join(SignalMeta)
    # .where(DefectRootCause.event_id == 2407026)
)


select_join_orm_stmt3 = (
    # JOIN with only right-hand explicit side + explicit ON-clause
    # See https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#setting-the-on-clause
    select(DefectRootCause)
    .join(DefectEvent, DefectRootCause.event_id == DefectEvent.event_id)
    .join(SignalMeta, DefectRootCause.signal_id == SignalMeta.id)
    # .where(DefectRootCause.event_id == 2407022)
)


def get_select_join_orm_result(session: sqlalchemy.orm.session, select_stmt):
    with session:
        logger.debug(f"Choosing joined data with following select-statement: {select_stmt}")

        # See https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#selecting-orm-entities-and-columns
        result = session.execute(select_stmt).all()  # Return Rows of attributes / ORM instances in a list
        # result = session.scalars(select_stmt).all()  # Return Rows of ORM instances in a list
        # result = session.scalars(select_stmt).fetchall()  # A synonym for the _engine.ScalarResult.all method.
        logger.info("Session.execute() creates a list of instances of type {0}. An example of the first instance:\n{1}"
                    .format(type(result[0]), result[0])
                    )

        return result

