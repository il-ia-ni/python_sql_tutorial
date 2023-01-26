"""
Defines SQLAlchemy database connectors.
"""
from typing import Optional

import pyodbc
from loguru import logger
from sqlalchemy.engine import URL, Engine, create_engine, make_url
from sqlalchemy.orm import Session, sessionmaker

from cracs_backend import settings


def build_full_url(url: str) -> URL:
    """Generates a new database connection string based on the application settings.

    Adds "odbc_driver" param to the URL when pyodbc connector is used.

    Args:
        url: database connection string
    """
    db_url = make_url(url)
    return db_url


def make_engine(db_url: URL, timeout: int) -> Engine:
    """Creates a new SQLAlchemy Engine instance

    Args:
        db_url: connection string to use
        timeout: connection timeout in seconds
    """
    # It's recommended to disable internal pyodbc pooling. See:
    # https://docs.sqlalchemy.org/en/14/dialects/mssql.html#pyodbc-pooling-connection-close-behavior
    if db_url.drivername == "mssql+pyodbc":
        pyodbc.pooling = False

    return create_engine(
        db_url,
        connect_args={"timeout": timeout},
    )


app_db_url = build_full_url(settings.app_db_url, settings.app_odbc_driver)
app_engine = make_engine(app_db_url, settings.app_connection_timeout)

pdw_url = build_full_url(settings.pdw_db_url, settings.pdw_odbc_driver)
pdw_engine = make_engine(pdw_url, settings.pdw_connection_timeout)


def get_app_db() -> Session:
    """Returns a new Session on App DB"""
    session = sessionmaker(app_engine, expire_on_commit=False)
    return session()


def get_pdw_db() -> Session:
    """Returns a new Session on PDW DB"""
    session = sessionmaker(pdw_engine, expire_on_commit=False)
    return session()