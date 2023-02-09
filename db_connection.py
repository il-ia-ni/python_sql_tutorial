"""
Defines SQLAlchemy database connectors.
"""
import pyodbc
from typing import Optional
from loguru import logger
from sqlalchemy.engine import URL, Engine, create_engine, make_url
from sqlalchemy.orm import Session, sessionmaker


def build_full_url(url: str, odbc_driver: Optional[str]) -> URL:
    """Generates a new database connection string based on the application settings.

    Adds "odbc_driver" param to the URL when pyodbc connector is used.

    Args:
        url: database connection string
        odbc_driver: ODBC driver to use
    """
    db_url = make_url(url)

    if db_url.drivername == "mssql+pyodbc":
        odbc_driver = _ensure_valid_odbc_driver(odbc_driver)

        db_url = db_url.update_query_dict({"driver": odbc_driver})

    return db_url


def _ensure_valid_odbc_driver(odbc_driver: Optional[str]) -> str:
    """Ensure that a valid ODBC driver name is provided.

    Args:
        odbc_driver: name of ODBC driver, if None default driver is used

    Returns:
        existing ODBC driver name
    """
    existing_drivers = pyodbc.drivers()
    if len(existing_drivers) == 0:
        raise Exception("No ODBC drivers found")
    if odbc_driver is None:
        odbc_driver = existing_drivers[0]
        logger.debug("ODBC driver not specified, using default: {}", odbc_driver)
    else:
        if odbc_driver not in existing_drivers:
            raise ValueError(f"ODBC driver {odbc_driver} not found in {existing_drivers=}")
    return odbc_driver


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


def get_session(engine: Engine) -> Session:
    """Returns a new Session
    """
    return sessionmaker(engine, expire_on_commit=False)()


def create_session_from_url(url: str, odbc_driver, timeout: int = 3) -> Session():
    """Returns a new Session based on given URL

    """
    db_url = build_full_url(url=url, odbc_driver=odbc_driver)
    engine = make_engine(db_url=db_url, timeout=timeout)
    return get_session(engine=engine)


if __name__ == "__main__":
    app_odbc_driver = "ODBC Driver 17 for SQL Server"
    URL = "mssql+pyodbc://Admin:Very(!)Secure-Password_123@127.0.0.1:1433/app_db"
    session = create_session_from_url(url=URL, odbc_driver=app_odbc_driver)
    print()
