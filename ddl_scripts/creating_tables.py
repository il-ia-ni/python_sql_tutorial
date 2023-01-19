from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, relationship

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB

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