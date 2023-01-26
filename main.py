from datetime import datetime

from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB
from ddl_scripts.creating_tables import SignalMeta

select1_stmt = text("SELECT id, category, name FROM Cracs_preventer_test.signal_meta ORDER BY id, category")
select2_stmt = text("SELECT id, category, name FROM Cracs_preventer_test.signal_meta WHERE id > :id ORDER BY id, category")
select3_stmt = text("SELECT id, name, description, update_date FROM Cracs_preventer_test.signal_meta WHERE id = :id ORDER BY id")
update1_stmt = text("UPDATE Cracs_preventer_test.signal_meta SET update_date=:upd_date, description=:descr WHERE id=:id")

# # It’s recommended that the object "Connection" is used in context manager style using the Python "with:" statement. It
# # represents active database resources and it’s good to make sure it’s closed when operations are completed
# with engine_SQLServerTest_MainDB.connect() as connection:
#     result = connection.execute(select1_stmt)
#     counter = 1
#     for row in result:
#         print(f"id{counter}:", row["id"], f" has a category", row["category"], "with name:", row["name"])
#         counter += 1
#
# # It’s recommended that the fundamental transactional / database interactive ORM object "Session" is used in
# # context manager style using the Python "with:" statement. It represents active database resources, and it’s good to
# # make sure it’s closed when operations are completed
# # See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#tutorial-executing-orm-session
# with Session(engine_SQLServerTest_MainDB) as session:
#     result = session.execute(select1_stmt)  # Session.execute() is used the same way as Connection.execute()
#     for row in result:
#         print(f"id{counter}:", row["id"], f" has a category", row["category"], "with name:", row["name"])
#         counter += 1

# See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#tutorial-executing-orm-session
with Session(engine_SQLServerTest_MainDB) as session:
    ''' "Release" ans connection pools
    When the Connection is closed at the end of the with: block, the referenced DBAPI connection is released to the 
    connection pool. From the perspective of the database itself, the connection pool will not actually “close” the 
    connection assuming the pool has room to store this connection for the next use. When the connection is returned 
    to the pool for re-use, the pooling mechanism issues a rollback() call on the DBAPI connection so that any 
    transactional state or locks are removed, and the connection is ready for its next use.
    See: https://docs.sqlalchemy.org/en/14/glossary.html#term-released
    A connection pool is a standard technique used to maintain long running connections in memory for efficient re-use, 
    as well as to provide management for the total number of connections an application might use simultaneously.
    https://docs.sqlalchemy.org/en/14/core/pooling.html
    '''
    result1 = session.execute(update1_stmt, {"upd_date": "2023-19-01 00:00:00.000", "descr": "This is a test object", "id": "test_obj_1"})

    result2 = session.execute(select3_stmt, {"id": "test_obj_1"})
    for row in result2:
        print(row["id"], row["name"], "last updated on:", row["update_date"], "with description:", row["description"])

    result3 = session.execute(update1_stmt, {"upd_date": datetime.utcnow(), "descr": "New test description 1", "id": "test_obj_1"})

    result4 = session.execute(select3_stmt, {"id": "test_obj_1"})
    for row in result4:
        print(row["id"], row["name"], "last updated on:", row["update_date"], "with description:", row["description"])

    session.commit()  # "commit-as-you-go" approach vs "begin once". See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#committing-changes


with Session(engine_SQLServerTest_MainDB) as session2:
    local_select_stmt = select(SignalMeta).where(SignalMeta.id.in_(["test_obj_1", "test_obj_2"]))

    print("Scalar select output:", session2.scalar(local_select_stmt))
    # for signal in session2.scalar(select_stmt):
    #     print("Scalar select output:", signal)

