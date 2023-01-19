from sqlalchemy import create_engine, text

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB

# It’s recommended that the connect() is used in context manager style using the Python "with:" statement. The Engine
# object represents active database resources and it’s good to make sure it’s closed when operations are completed
with engine_SQLServerTest_MainDB.connect() as connection:
    result = connection.execute(text("select id, category, name from Cracs_preventer_test.signal_meta"))
    counter = 1
    for row in result:
        print(f"id{counter}:", row["id"], f" has a category", row["category"], "with name:", row["name"])
        counter += 1


