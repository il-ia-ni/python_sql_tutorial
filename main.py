from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB

select1_stmt = text("SELECT id, category, name FROM Cracs_preventer_test.signal_meta ORDER BY id, category")
select2_stmt = text("SELECT id, category, name FROM Cracs_preventer_test.signal_meta WHERE id > :id ORDER BY id, category")

# It’s recommended that the object "Connection" is used in context manager style using the Python "with:" statement. It
# represents active database resources and it’s good to make sure it’s closed when operations are completed
with engine_SQLServerTest_MainDB.connect() as connection:
    result = connection.execute(select1_stmt)
    counter = 1
    for row in result:
        print(f"id{counter}:", row["id"], f" has a category", row["category"], "with name:", row["name"])
        counter += 1

# It’s recommended that the fundamental transactional / database interactive ORM object "Session" is used in
# context manager style using the Python "with:" statement. It represents active database resources, and it’s good to
# make sure it’s closed when operations are completed
with Session(engine_SQLServerTest_MainDB) as session:
    result = session.execute(select1_stmt)
    for row in result:
        print(f"id{counter}:", row["id"], f" has a category", row["category"], "with name:", row["name"])
        counter += 1


