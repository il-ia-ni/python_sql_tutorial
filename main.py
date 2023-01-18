from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

Base = declarative_base()
# For establishing connections see https://docs.sqlalchemy.org/en/14/tutorial/engine.html#tutorial-engine
# For configuring the engine see https://docs.sqlalchemy.org/en/14/core/engines.html
# For working with engines see https://docs.sqlalchemy.org/en/14/core/connections.html
dbdriver = "mssql+pyodbc"
username = "SA"
password = "ilzen92!"
connection_url_SQLServer_str = f"{dbdriver}://{username}:{password}@localhost\\SQLEXPRESSTEST:1433/"  # See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#dialect-mssql-pyodbc-connect
engine_Cracks_Preventer = create_engine(connection_url_SQLServer_str + "master?driver=ODBC+Driver+17+for+SQL+Server", echo=True, future=True)

with engine_Cracks_Preventer.connect() as connection:
    result = connection.execute(text("select id, category, name from Cracs_preventer_test.signal_meta"))
    counter = 1
    for row in result:
        print(f"id{counter}:", row["id"], f" has a category", row["category"], "with name:", row["name"])
        counter += 1

# connection_url_SQLServer_obj = URL.create(
#     "mssql+pyodbc",
#     username="SA",
#     password="ilzen92!",
#     host="localhost/SQLEXPRESSTEST",
#     port=1433,
#     database="master/Cracs_preventer_test",
#     query={
#         "driver": "ODBC Driver 19 for SQL Server",
#         "TrustServerCertificate": "yes"
#     },
# )


class SignalMeta(Base):  # ORM class
    # __atble_name__ + Column-props form a SQLAlchemy table metadata with Declarative Table configuration using both
    # Core and ORM approaches See @ https://docs.sqlalchemy.org/en/14/glossary.html#term-table-metadata
    # and https://docs.sqlalchemy.org/en/14/orm/declarative_tables.html#orm-declarative-table
    __tablename__ = "signal_meta"

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

