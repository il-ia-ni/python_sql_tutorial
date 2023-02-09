from datetime import datetime

import sqlalchemy
from sqlalchemy import text, select

from ddl_scripts.creating_tables import signal_meta, SignalMeta

select1_core_stmt = select(signal_meta)  # uses Core API Table instance with MetaData. See creating_tables.py
select1_orm_stmt = select(SignalMeta)  # uses class mapped from the ORM API's Base Class. See creating_tables.py
select2_core_stmt = select(signal_meta.c.id, signal_meta.c.category, signal_meta.c.name)
select2_orm_stmt = text("SELECT id, category, name FROM Cracs_preventer_test.signal_meta WHERE id > :id ORDER BY id, category")
select3_stmt = text("SELECT id, name, description, update_date FROM Cracs_preventer_test.signal_meta WHERE id = :id ORDER BY id")
update1_stmt = text("UPDATE Cracs_preventer_test.signal_meta SET update_date=:upd_date, description=:descr WHERE id=:id")

""" Connection.execute() in Core API
Selecting Rows with Core API or ORM API: https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#tutorial-selecting-data
See more to SQLAlchemy Core, ORM APIs @ https://docs.sqlalchemy.org/en/14/tutorial/index.html 
"""


def select_core_signalmeta_all(engine: sqlalchemy.engine):
    # It’s recommended that the object "Connection" is used in context manager style using the Python "with:" statement.
    # It represents active database resources and it’s good to make sure it’s closed when operations are completed
    counter = 1
    with engine.connect() as connection:
        result = connection.execute(select1_core_stmt)
        for coreRowObj in result:  # Returns Row objs which are as close to Python tuples as possible
            print(f"id{counter}:", coreRowObj["id"],
                  f" has a category", coreRowObj["category"],
                  "with name:", coreRowObj["name"])
            counter += 1


def select_core_signalmeta_3cols(engine: sqlalchemy.engine):
    counter = 1
    with engine.connect() as connection:
        result = connection.execute(select2_core_stmt)
        for coreRowObj in result:
            print(coreRowObj)
            counter += 1


""" Session.execute() in ORM API 
https://docs.sqlalchemy.org/en/14/orm/queryguide.html and https://docs.sqlalchemy.org/en/14/orm/quickstart.html#simple-select
Selecting Rows with Core API or ORM API: https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#tutorial-selecting-data
See more to SQLAlchemy Core, ORM APIs @ https://docs.sqlalchemy.org/en/14/tutorial/index.html 
"""


def select_orm_signalmeta_all(session: sqlalchemy.orm.session):
    # It’s recommended that the fundamental transactional / database interactive ORM object "Session" is used in
    # context manager style using the Python "with:" statement. It represents active database resources, and it’s good
    # to make sure it’s closed when operations are completed using the with statement
    # See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#tutorial-executing-orm-session
    counter = 1
    with session:
        result = session.execute(select1_orm_stmt)  # Session.execute() is used the same way as Connection.execute().
        # Using this approach, we continue to get Row objects from the result, however these rows are now capable of
        # including complete entities, such as instances of the SignalMeta class as individual elements within each row
        for ormRowObj in result:
            # SignalID (id='water_pressure_LF_Z05_C' of category water_pressure with name: 'Water pressure LF Z05')
            print(ormRowObj)
            counter += 1


# noinspection SpellCheckingInspection
def select_orm_signalmeta_testobjs(session: sqlalchemy.orm.session):
    # See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#tutorial-executing-orm-session
    with session:
        """ "Release" ans connection pools
        When the Connection is closed at the end of the with: block, the referenced DBAPI connection is released to the 
        connection pool. From the perspective of the database itself, the connection pool will not actually “close” the 
        connection assuming the pool has room to store this connection for the next use. When the connection is returned 
        to the pool for re-use, the pooling mechanism issues a rollback() call on the DBAPI connection so that any 
        transactional state or locks are removed, and the connection is ready for its next use.
        See: https://docs.sqlalchemy.org/en/14/glossary.html#term-released
        A connection pool is a standard technique used to maintain long running connections in memory for efficient re-use, 
        as well as to provide management for the total number of connections an application might use simultaneously.
        https://docs.sqlalchemy.org/en/14/core/pooling.html
        """
        result1 = session.execute(update1_stmt,
                                  {"upd_date": "2023-19-01 00:00:00.000", "descr": "This is a test object",
                                   "id": "test_obj_1"})

        result2 = session.execute(select3_stmt, {"id": "test_obj_1"})
        for row in result2:
            print(row["id"], row["name"], "last updated on:", row["update_date"], "with description:",
                  row["description"])

        result3 = session.execute(update1_stmt, {"upd_date": datetime.utcnow(), "descr": "New test description 1",
                                                 "id": "test_obj_1"})

        result4 = session.execute(select3_stmt, {"id": "test_obj_1"})
        for row in result4:
            print(row["id"], row["name"], "last updated on:", row["update_date"], "with description:",
                  row["description"])

        session.commit()  # "commit-as-you-go" approach vs "begin once". See https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html#committing-changes


# noinspection SpellCheckingInspection
def select_orm_signalmeta_testobjs_scalar_result(session: sqlalchemy.orm.session):
    """
    The method that is often useful when querying for ORM objects is the Session.scalars() method, which will return
    a ScalarResult filtering object (A wrapper for a Result that returns scalar values rather than Row values) which
    will return single elements rather than Row object. See https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session.scalars
    https://docs.sqlalchemy.org/en/14/orm/quickstart.html#simple-select
    """
    with session:
        local_select_stmt = select(SignalMeta).where(SignalMeta.id.in_(["test_obj_1", "test_obj_2"]))

        print("Scalar select output:", session.scalar(local_select_stmt))
        # for signal in session2.scalar(select_stmt):
        #     print("Scalar select output:", signal)

