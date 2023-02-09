from datetime import datetime

from sqlalchemy.orm import Session
from loguru import logger

from ddl_scripts.creating_tables import Base, SignalMeta


@logger.catch
def add_new_objs(session: Session, orm_obj: Base):
    # See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#tutorial-executing-orm-session
    # https://docs.sqlalchemy.org/en/14/orm/session_basics.html#id1
    with session:
        # It’s recommended that the fundamental transactional / database interactive ORM object "Session" is used in
        # context manager style using the Python "with:" statement. The Session object represents active database resources,
        # and it’s good to make sure it’s closed when operations are completed
        session.add_all([orm_obj])
        logger.info(f"Successfully added the following object: {orm_obj}")
        session.commit()


test_dataobj1 = SignalMeta(
    id="test_obj_1",
    create_date=datetime.utcnow(),
    update_date=datetime.utcnow(),
    name="Test signal meta",
    caster_id="testCaster",
    category="test_category",
    external_id="TEST.testCaster",
    units="TestUnit",
    description="This is a test object",
)

test_dataobj2 = SignalMeta(
    id="test_obj_2",
    create_date=datetime.utcnow(),
    update_date=datetime.utcnow(),
    name="Test signal meta 2",
    caster_id="testCaster2",
    category="test_category2",
    external_id="TEST.testCaster2",
    units="TestUnit2",
    description="This is a test object 2",
)


