from sqlalchemy.orm import Session

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB
from ddl_scripts.creating_tables import SignalMeta

# See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#tutorial-executing-orm-session
with Session(engine_SQLServerTest_MainDB) as session:
    testDataObj1 = SignalMeta(
        id="test_obj_1",
        create_date="2023-19-01 00:00:00.000",  # TODO: Try out with Python datetimes?
        update_date="2023-19-01 00:00:00.000",
        name="Test signal meta",
        caster_id="testCaster",
        category="test_category",
        external_id="TEST.testCaster",
        units="TestUnit",
        description="This is a test object",
    )

session.add_all([testDataObj1])
session.commit()
