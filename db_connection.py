"""
Defines SQLAlchemy database connectors.
"""
from typing import Optional
from sqlalchemy.engine import URL, Engine, create_engine, make_url
from sqlalchemy.orm import Session, sessionmaker


def build_full_url(url: str) -> URL:
    """Generates a new database connection string.

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

    return create_engine(
        db_url,
        connect_args={"timeout": timeout},
    )


def get_session(engine: Engine) -> Session:
    """Returns a new Session
    """
    session = sessionmaker(engine, expire_on_commit=False)
    return session()
