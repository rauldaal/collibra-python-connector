"""
Collibra Connector Library
~~~~~~~~~~~~~~~~~~~~~~~~~~

A professional Python SDK for the Collibra Data Governance Center API.
"""

from .connector import CollibraConnector
from .query_builder import (
    FilterBuilder,
    FilterOperator,
    OutputModuleQueryBuilder,
    RelationBuilder,
    RelationDirection,
    ResourceBuilder,
    ResourceType,
    UserBuilder,
)

__version__ = "1.5.0.post1"
__all__ = [
    "CollibraConnector",
    "FilterBuilder",
    "FilterOperator",
    "OutputModuleQueryBuilder",
    "RelationBuilder",
    "RelationDirection",
    "ResourceBuilder",
    "ResourceType",
    "UserBuilder",
]
