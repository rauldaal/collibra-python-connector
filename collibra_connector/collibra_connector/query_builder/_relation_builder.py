"""collibra_connector.query_builder._relation_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fluent builder for a single ``Relation`` entry inside a resource block.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ._enums import RelationDirection

if TYPE_CHECKING:
    from ._resource_builder import ResourceBuilder


class RelationBuilder:
    """Builds a single ``Relation`` entry.

    Args:
        type_id:   Relation type UUID.
        direction: :class:`RelationDirection` (or ``"SOURCE"`` / ``"TARGET"``
                   string for backward compatibility).
        name:      Output column group label for this relation.

    Attach the related asset with :meth:`source`, :meth:`target`,
    :meth:`source_asset`, or :meth:`target_asset`.

    Raises:
        ValueError: If any required argument is blank or direction is invalid.
    """

    def __init__(
        self,
        type_id: str,
        direction: Union[str, RelationDirection],
        name: str,
    ) -> None:
        if not type_id:
            raise ValueError("RelationBuilder: 'type_id' must not be empty.")
        if not name:
            raise ValueError("RelationBuilder: 'name' must not be empty.")
        try:
            self._direction: str = RelationDirection(direction.upper()).value
        except ValueError:
            raise ValueError(
                f"RelationBuilder: direction must be 'SOURCE' or 'TARGET', "
                f"got {direction!r}."
            )
        self._type_id = type_id
        self._name = name
        self._related: Optional[Dict[str, Any]] = None
        self._related_key: Optional[str] = None

    def _set_related(
        self,
        key: str,
        resource: Union["ResourceBuilder", Dict[str, Any]],
    ) -> "RelationBuilder":
        # Lazy import breaks the circular dependency with _resource_builder.
        from ._resource_builder import ResourceBuilder  # noqa: PLC0415

        c = copy.deepcopy(self)
        c._related_key = key
        if isinstance(resource, ResourceBuilder):
            c._related = resource.build()
        elif isinstance(resource, dict):
            c._related = copy.deepcopy(resource)
        else:
            raise TypeError(
                f"RelationBuilder: expected ResourceBuilder or dict, "
                f"got {type(resource).__name__!r}."
            )
        return c

    def source(self, resource: Union["ResourceBuilder", Dict[str, Any]]) -> "RelationBuilder":
        """Attach the ``Source`` asset (root asset is the *target* end)."""
        return self._set_related("Source", resource)

    def target(self, resource: Union["ResourceBuilder", Dict[str, Any]]) -> "RelationBuilder":
        """Attach the ``Target`` asset (root asset is the *source* end)."""
        return self._set_related("Target", resource)

    def source_asset(self, resource: Union["ResourceBuilder", Dict[str, Any]]) -> "RelationBuilder":
        """Attach a ``SourceAsset`` block (used in some sharing-request queries)."""
        return self._set_related("SourceAsset", resource)

    def target_asset(self, resource: Union["ResourceBuilder", Dict[str, Any]]) -> "RelationBuilder":
        """Attach a ``TargetAsset`` block."""
        return self._set_related("TargetAsset", resource)

    def build(self) -> Dict[str, Any]:
        """Return the relation dict."""
        result: Dict[str, Any] = {
            "typeId": self._type_id,
            "type": self._direction,
            "name": self._name,
        }
        if self._related_key and self._related is not None:
            result[self._related_key] = copy.deepcopy(self._related)
        return result
