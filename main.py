from sqlalchemy import create_engine, text

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB

with engine_SQLServerTest_MainDB.connect() as connection:
    result = connection.execute(text("select id, category, name from Cracs_preventer_test.signal_meta"))
    counter = 1
    for row in result:
        print(f"id{counter}:", row["id"], f" has a category", row["category"], "with name:", row["name"])
        counter += 1


