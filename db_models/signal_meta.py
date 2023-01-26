"""
Defines signals metadata table.
"""
from sqlalchemy import Column, Enum, Float, String

from .base import Base, CreateUpdateTimeMixin


class SignalMeta(Base, CreateUpdateTimeMixin):
    """
    Contains Signals metadata.
    """

    __tablename__ = "signal_meta"
    __table_args__ = {
        "comment": "Defines supported signals. As usual, a signal represents a time series which "
        "can be displayed in the frontend in different formats."
    }

    id = Column(String(128), primary_key=True, comment="Internal signal id")
    name = Column(String(128), nullable=False, comment="Human-readable signal name")
    caster_id = Column(
        String(256),
        nullable=False,
        index=True,
    )
    category = Column(
        String(256),
        nullable=False,
        index=True,
        comment="Which category this signal belongs to",
    )
    external_id = Column(
        String(256),
        nullable=False,
        index=True,
        comment="External id of the signal used by the customer (e.g. PI Tags)",
    )
    units = Column(String(256), nullable=False, comment="Units of measurement")
    description = Column(String(1024), nullable=True, comment="Additional description")
    y_lim_min = Column(Float, nullable=True, comment="Y axis min limit")
    y_lim_max = Column(Float, nullable=True, comment="Y axis max limit")