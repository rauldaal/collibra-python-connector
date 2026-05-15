"""collibra_connector.query_builder._enums
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Enumerations used across the query builder package.
"""

from __future__ import annotations

from enum import Enum


class FilterOperator(str, Enum):
    """Valid filter operators for Output Module ``Field`` predicates.

    Inherits from ``str`` so plain string literals are also accepted
    (backward-compatible).
    """

    EQUALS = "EQUALS"
    NOT_EQUALS = "NOT_EQUALS"
    IN = "IN"
    NOT_IN = "NOT_IN"
    CONTAINS = "CONTAINS"
    NOT_CONTAINS = "NOT_CONTAINS"
    STARTS_WITH = "STARTS_WITH"
    ENDS_WITH = "ENDS_WITH"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    GREATER_THAN = "GREATER_THAN"
    LESS_THAN = "LESS_THAN"
    GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL"
    LESS_THAN_OR_EQUAL = "LESS_THAN_OR_EQUAL"


class RelationDirection(str, Enum):
    """Traversal direction for a :class:`~.RelationBuilder` entry."""

    SOURCE = "SOURCE"
    TARGET = "TARGET"


class ResourceType(str, Enum):
    """Built-in top-level resource type keys recognised by the Output Module."""

    TERM = "Term"
    ASSET = "Asset"
