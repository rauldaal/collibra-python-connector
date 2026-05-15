"""collibra_connector.query_builder._filter_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Static factory for constructing Output Module filter expressions.
"""

from __future__ import annotations

from typing import Any, Dict, Union

from ._enums import FilterOperator


class FilterBuilder:
    """Static factory for Output Module filter expressions.

    All methods return plain dicts that can be nested arbitrarily::

        FilterBuilder.and_(
            FilterBuilder.field("Asset Type", FilterOperator.EQUALS, "Data Set"),
            FilterBuilder.or_(
                FilterBuilder.field("Status", FilterOperator.EQUALS, "Approved"),
                FilterBuilder.field("Status", FilterOperator.EQUALS, "Accepted"),
            ),
            FilterBuilder.field("SFStatus", FilterOperator.IS_NULL),
        )

    Plain strings are also accepted for ``operator`` for backward compatibility.
    """

    def __new__(cls) -> "FilterBuilder":  # pragma: no cover
        raise TypeError("FilterBuilder is a static namespace — do not instantiate it.")

    @staticmethod
    def field(
        name: str,
        operator: Union[str, FilterOperator],
        value: Any = None,
    ) -> Dict[str, Any]:
        """Build a single ``Field`` predicate.

        Args:
            name:     Output column alias matching a ``name`` in the resource tree.
            operator: A :class:`FilterOperator` member or raw string (e.g. ``"EQUALS"``).
            value:    Comparison value; omit for ``IS NULL`` / ``IS NOT NULL``.

        Raises:
            ValueError: If *name* or *operator* are blank.
        """
        if not name:
            raise ValueError("FilterBuilder.field: 'name' must not be empty.")
        if not operator:
            raise ValueError("FilterBuilder.field: 'operator' must not be empty.")
        predicate: Dict[str, Any] = {"name": name, "operator": operator.upper()}
        if value is not None:
            predicate["value"] = value
        return {"Field": predicate}

    @staticmethod
    def and_(*conditions: Dict[str, Any]) -> Dict[str, Any]:
        """Join conditions with logical AND.  Raises ``ValueError`` if empty."""
        if not conditions:
            raise ValueError("FilterBuilder.and_: at least one condition is required.")
        return {"AND": list(conditions)}

    @staticmethod
    def or_(*conditions: Dict[str, Any]) -> Dict[str, Any]:
        """Join conditions with logical OR.  Raises ``ValueError`` if empty."""
        if not conditions:
            raise ValueError("FilterBuilder.or_: at least one condition is required.")
        return {"OR": list(conditions)}
