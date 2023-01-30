"""
Defines SQLAlchemy database connectors.
"""
from loguru import logger
from sqlalchemy.engine import URL, Engine, create_engine, make_url
from sqlalchemy.orm import Session, sessionmaker


@logger.catch
def build_full_url(url: str) -> URL:
    """Generates a new database connection string.

    Args:
        url: database connection string
    """

    db_url = make_url(url)
    logger.debug(f"db_connection script build_full_url() creates url: {db_url}")
    return db_url


@logger.catch
def make_engine(db_url: URL, timeout: int) -> Engine:
    """Creates a new SQLAlchemy Engine instance

    Args:
        db_url: connection string to use
        timeout: connection timeout in seconds
    """
    engine = create_engine(
        db_url,
        connect_args={"timeout": timeout},
    )

    logger.debug(f"db_connection script make_engine() builds: {engine}")
    return engine


@logger.catch
def get_session(engine: Engine) -> Session:
    """Returns a new Session
    """
    session = sessionmaker(engine, expire_on_commit=False)()

    logger.debug(f"db_connection script get_session() establishes: {session}")
    return session
