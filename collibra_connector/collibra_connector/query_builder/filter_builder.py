"""
FilterBuilder — builds a filter condition tree for a Collibra Output Module query.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .constants import VALID_FILTER_OPERATORS, _NULL_OPERATORS


class FilterBuilder:
    """
    Builds a filter condition tree for a Collibra Output Module query.

    Leaf conditions are created with class methods (``equals``, ``starts_with``,
    ``in_``, etc.).  Compound conditions are created with ``and_`` / ``or_``.

    Example::

        f = FilterBuilder.and_(
            FilterBuilder.equals("AssetTypeId", "uuid-here"),
            FilterBuilder.or_(
                FilterBuilder.equals("StatusId", "active-uuid"),
                FilterBuilder.is_null("RetiredDate"),
            ),
        )
        f.build()
        # → {"AND": [
        #       {"Field": {"name": "AssetTypeId", "operator": "EQUALS", "value": "uuid-here"}},
        #       {"OR": [
        #           {"Field": {"name": "StatusId", "operator": "EQUALS", "value": "active-uuid"}},
        #           {"Field": {"name": "RetiredDate", "operator": "IS_NULL"}},
        #       ]},
        #   ]}
    """

    def __init__(self) -> None:
        self._node: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Compound constructors
    # ------------------------------------------------------------------

    @classmethod
    def and_(cls, *conditions: "FilterBuilder") -> "FilterBuilder":
        """Combine multiple conditions with AND."""
        if not conditions:
            raise ValueError("and_() requires at least one condition.")
        f = cls()
        f._node = {"AND": [c.build() for c in conditions]}
        return f

    @classmethod
    def or_(cls, *conditions: "FilterBuilder") -> "FilterBuilder":
        """Combine multiple conditions with OR."""
        if not conditions:
            raise ValueError("or_() requires at least one condition.")
        f = cls()
        f._node = {"OR": [c.build() for c in conditions]}
        return f

    # ------------------------------------------------------------------
    # Leaf constructors
    # ------------------------------------------------------------------

    @classmethod
    def field(cls, name: str, operator: str, value: Any = None) -> "FilterBuilder":
        """
        Build a leaf Field condition.

        Args:
            name:     The column alias name defined in the resource projection.
            operator: One of the VALID_FILTER_OPERATORS.
            value:    The comparison value (omit for IS_NULL / IS_NOT_NULL).
        """
        op = operator.upper()
        if op not in VALID_FILTER_OPERATORS:
            raise ValueError(
                f"Invalid operator '{operator}'. "
                f"Valid operators: {sorted(VALID_FILTER_OPERATORS)}"
            )
        if op not in _NULL_OPERATORS and value is None:
            raise ValueError(f"Operator '{operator}' requires a value.")
        f = cls()
        condition: Dict[str, Any] = {"name": name, "operator": op}
        if value is not None:
            condition["value"] = value
        f._node = {"Field": condition}
        return f

    @classmethod
    def equals(cls, name: str, value: Any) -> "FilterBuilder":
        """Field equals value."""
        return cls.field(name, "EQUALS", value)

    @classmethod
    def not_equals(cls, name: str, value: Any) -> "FilterBuilder":
        """Field does not equal value."""
        return cls.field(name, "NOT_EQUALS", value)

    @classmethod
    def starts_with(cls, name: str, value: str) -> "FilterBuilder":
        """Field starts with value."""
        return cls.field(name, "STARTS_WITH", value)

    @classmethod
    def contains(cls, name: str, value: str) -> "FilterBuilder":
        """Field contains value."""
        return cls.field(name, "CONTAINS", value)

    @classmethod
    def greater_than(cls, name: str, value: Any) -> "FilterBuilder":
        """Field greater than value."""
        return cls.field(name, "GREATER_THAN", value)

    @classmethod
    def less_than(cls, name: str, value: Any) -> "FilterBuilder":
        """Field less than value."""
        return cls.field(name, "LESS_THAN", value)

    @classmethod
    def in_(cls, name: str, values: List[Any]) -> "FilterBuilder":
        """Field value is in the given list."""
        return cls.field(name, "IN", values)

    @classmethod
    def not_in(cls, name: str, values: List[Any]) -> "FilterBuilder":
        """Field value is not in the given list."""
        return cls.field(name, "NOT_IN", values)

    @classmethod
    def is_null(cls, name: str) -> "FilterBuilder":
        """Field is NULL."""
        return cls.field(name, "IS_NULL")

    @classmethod
    def is_not_null(cls, name: str) -> "FilterBuilder":
        """Field is not NULL."""
        return cls.field(name, "IS_NOT_NULL")

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self) -> Dict[str, Any]:
        """Return the filter dict representation."""
        if self._node is None:
            raise ValueError("FilterBuilder has no condition set.")
        return self._node

    def __repr__(self) -> str:
        return f"FilterBuilder({self._node!r})"
