"""
    Defines defect events table.
"""
from enum import Enum
from itertools import groupby
from typing import Dict, Iterable, List

from cracs_common.definitions.v2.behaviour_pattern import BehaviourPattern
from cracs_common.definitions.v2.model_type import ModelType
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    case,
    false,
    func,
    select,
    true,
)
from sqlalchemy.orm import aliased, column_property, relationship

from ...helpers.utils import StrEnum
from ..pdw import L2Slab, SPHeat
from .base import Base, CreateUpdateTimeMixin
from .behavior_pattern_mapping import BehaviorPatternMapping
from .defect_meta import DefectMeta
from .defect_root_causes import DefectRootCause

# pylint: disable=no-member


class ModelType(str, Enum):
    """Defines model type"""

    RULE_BASED = "rules"
    AI = "ai"
    DUMMY = "dummy"


class DefectEventType(str, Enum):
    """
    Defines the types of defect events.
    """

    SLAB_BASED = "slab_based"
    LIVE = "live"


class BehaviourPattern(str, Enum):
    """Defines known behavior patterns"""

    BULGING = "bulging"
    CLOGGING = "clogging"
    TUNDISH_VORTEX = "tundish_vortex"


class DefectEvent(Base, CreateUpdateTimeMixin):
    """
    Contains defect event data
    """

    __tablename__ = "defect_event"
    __table_args__ = {"comment": "Contains defect event data"}

    # TODO: Add columns
    #           session_id - UUID that for detections connected to one model detection session
    #                        (just in case a slab would pass the model twice)

    # message identifiers
    event_id = Column(BigInteger, primary_key=True, comment="unique event id")
    event_type = Column(
        Enum(DefectEventType),
        nullable=False,
        comment="Defect event type",
    )

    created_at = Column(DateTime, nullable=False, comment="Timestamp when the inspection message was generated")

    # production id genealogy
    pdw_product_id = Column(BigInteger, index=True, comment="Id of the corresponding PDW entry")
    caster_id = Column(
        String(256),
        index=True,
        nullable=False,
        comment="Id of the caster to which this detection belongs to",
    )
    strand_id = Column(
        String(256),
        index=True,
        nullable=False,
        comment="Id of the strand to which this detection belongs to",
    )
    pk_heat = Column(
        Integer,
        index=True,
        nullable=True,
        comment="PK of heat in which the current slab was casted",
    )
    heat_id = Column(
        String(128),
        index=True,
        nullable=True,
        comment="Id of heat in which the current slab was casted",
    )
    l2_slab_id = Column(
        Integer,
        index=True,
        nullable=True,
        comment="PK of slab for which prediction is valid",
    )
    slab_id = Column(
        String(128),
        index=True,
        nullable=True,
        comment="Id of slab for which prediction is valid",
    )
    grade_id = Column(String(128), index=True, nullable=True, comment="Id of the steel grade")

    # detections data
    # used model
    model_name = Column(String(128), nullable=False, comment="Name of model used for prediction")
    model_number = Column(Integer, nullable=False, comment="Version of model used for prediction")
    model_type = Column(Enum(ModelType), nullable=False, comment="Type of the model used for prediction")

    # input data intervals
    # TODO: data_start_time and data_end_time are marked as required in cracs-common now.
    #  make these fields not null in the DB
    data_start_time = Column(
        DateTime(),
        comment="Start of the time interval considered by the inspection",
    )
    data_end_time = Column(DateTime(), comment="End of the time interval considered by the inspection")
    cast_length_start = Column(Float(), comment="Start of the casting length interval considered by the inspection")
    cast_length_end = Column(Float(), comment="End of the casting length interval considered by the inspection")

    # defect data
    behaviour_pattern_id = Column(
        Enum(BehaviourPattern),
        index=True,
        comment="Id of the detected behaviour pattern (e.g. 'clogging' or 'bulging')",
    )
    detection_probability = Column(Float, comment="Probability of defect occurrence")