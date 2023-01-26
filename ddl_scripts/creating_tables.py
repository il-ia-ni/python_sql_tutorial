from sqlalchemy import Table, MetaData, Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, relationship

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB

"""
While the SQL looks the same whether we invoke select(signal_meta) or select(SignalMeta(Base)) (See select1_stmts in 
simple_select.py), in the more general case they do not necessarily render the same thing, as an ORM-mapped class may be
 mapped to other kinds of “selectables” besides tables. The select() that’s against an ORM entity also indicates that 
 ORM-mapped instances should be returned in a result, which is not the case when SELECTing from a Table object.
"""

"""
Describing Databases with CORE API's MetaData and Table objs
See @ https://docs.sqlalchemy.org/en/14/core/metadata.html
Table objs can also be created using table reflection from the DB itself: https://docs.sqlalchemy.org/en/14/tutorial/metadata.html#table-reflection
"""


metadata_obj = MetaData()  # https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData
signal_meta = Table(
    # https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.Table
    "signal_meta",
    metadata_obj,
    Column("id", String(128), primary_key=True, nullable=False),
    Column("create_date", DateTime, nullable=False),
    Column("update_date", DateTime, nullable=False),
    Column("name", String(128), nullable=False),
    Column("caster_id", String(256), nullable=False),
    Column("category", String(22), nullable=True),
    Column("external_id", String(256), nullable=False),
    Column("units", String(256), nullable=False),
    Column("description", String(1024), nullable=True),
    Column("y_lim_min", Float, nullable=True),
    Column("y_lim_max", Float, nullable=True),
    schema="Cracs_preventer_test"
)

"""
Defining Table Metadata with the ORM API
See @ https://docs.sqlalchemy.org/en/14/tutorial/metadata.html#tutorial-orm-table-metadata
"""


Base = declarative_base()
# Base.metadata.create_all(engine_SQLServerTest_MainDB)  # Adds all ORM table classes to the specified DB by emitting
# CREATE TABLE DDL


class SignalMeta(Base):  # ORM class
    # __table_name__ + Column-props form a SQLAlchemy table metadata with Declarative Table configuration using both
    # Core and ORM approaches See @ https://docs.sqlalchemy.org/en/14/glossary.html#term-table-metadata
    # and https://docs.sqlalchemy.org/en/14/orm/declarative_tables.html#orm-declarative-table
    __tablename__ = "signal_meta"  # Must correspond to Table name in the DB!
    __table_args__ = {"schema": "Cracs_preventer_test"}  # Found @ https://stackoverflow.com/questions/47077649/how-do-i-set-the-schema-in-sqlalchemy-for-mssql

    id = Column(String(128), primary_key=True, nullable=False)
    create_date = Column(DateTime, nullable=False)
    update_date = Column(DateTime, nullable=False)
    name = Column(String(128), nullable=False)
    caster_id = Column(String(256), nullable=False)
    category = Column(String(22), nullable=True)
    external_id = Column(String(256), nullable=False)
    units = Column(String(256), nullable=False)
    description = Column(String(1024), nullable=True)
    y_lim_min = Column(Float, nullable=True)
    y_lim_max = Column(Float, nullable=True)

    def __repr__(self):
        # method is not required but is useful for debugging
        return f"SignalID (id={self.id!r} of category {self.category} with name: {self.name!r})"


class DefectEvent(Base):
    __tablename__ = "defect_event"
    __table_args__ = {"schema": "Cracs_preventer_test"}

    create_date = Column(DateTime, nullable=False)
    update_date = Column(DateTime, nullable=False)
    id = Column(Integer, primary_key=True, nullable=False)
    created_at = Column(DateTime, nullable=False)
    pdw_product_id = Column(Integer, nullable=True)
    caster_id = Column(String(256), nullable=False)
    heat_id = Column(String(128), nullable=True)
    slab_id = Column(String(128), nullable=True)
    model_name = Column(String(128), nullable=False)
    model_number = Column(Integer, nullable=False)
    data_start_time = Column(DateTime, nullable=True)
    data_end_time = Column(DateTime, nullable=True)
    cast_length_start = Column(Float, nullable=True)
    cast_length_end = Column(Float, nullable=True)
    grade_id = Column(String(128), nullable=True)
    event_type = Column(String(10), nullable=False)
    model_type = Column(String(10), nullable=False)
    strand_id = Column(String(128), nullable=False)

    def __repr__(self):
        # method is not required but is useful for debugging
        return f"EventID (id={self.id!r} of event type {self.event_type} of model: {self.model_name!r})"

