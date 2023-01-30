"""
Defines DB MetaData and Base model class
"""
from datetime import datetime

from sqlalchemy import Column, DateTime, MetaData
from sqlalchemy.ext.declarative import as_declarative

Base = declarative_base()


class CreateUpdateTimeMixin:
    """Adds create_date and update_date columns to support data versioning"""

    create_date = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Row creation time in UTC")
    update_date = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        comment="Row last update time in UTC",
    )