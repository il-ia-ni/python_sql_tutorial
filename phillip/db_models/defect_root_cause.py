"""
Defines defect root causes table.
"""

from sqlalchemy import JSON, BigInteger, Column, DateTime, Float, ForeignKey, String

from .base import Base, CreateUpdateTimeMixin


class DefectRootCause(Base, CreateUpdateTimeMixin):
    """
    Defines root cause signals to defect events mapping
    """

    __tablename__ = "defect_root_cause"
    __table_args__ = {"comment": "Defines root cause signals to defect events mapping"}

    event_id = Column(
        BigInteger(),
        ForeignKey("defect_event.event_id", name="fk_defect_root_cause_event_id", ondelete="cascade"),
        index=True,
        comment="defect event id",
        primary_key=True,
    )

    signal_id = Column(
        String(128),
        ForeignKey("signal_meta.id", name="fk_defect_root_cause_signal_id"),
        index=True,
        comment="Id of the signal that became the root cause of the detected behavior",
        primary_key=True,
    )

    importance = Column(Float, nullable=True, default=0.0, comment="Importance root cause signals")

    data_start_time = Column(
        DateTime,
        nullable=True,
        comment="Start time of data batch",
    )

    data_end_time = Column(DateTime, nullable=True, comment="End time of data batch")

    signal_data = Column(JSON, nullable=True, comment="Data for belonging signal id")