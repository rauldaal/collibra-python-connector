"""
OutputModuleQueryBuilder — top-level builder for a Collibra Output Module query body.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from .resource_builder import ResourceBuilder


class OutputModuleQueryBuilder:
    """
    Top-level builder for a Collibra Output Module query body.

    Produces a ``ViewConfig`` (for JSON / XML exports) or
    ``TableViewConfig`` (for CSV / Excel / job-based exports).

    Example — ViewConfig::

        from collibra_connector import OutputModuleQueryBuilder, ResourceBuilder, FilterBuilder

        query = (
            OutputModuleQueryBuilder()          # ViewConfig by default
            .asset(
                ResourceBuilder("Request")
                .signifier("Request_FullName")
                .id("Request_Id")
                .status("Status", value_name="StatusValue", id_name="StatusId")
                .attribute("attr-uuid", "AccessType", "AccessType_Value")
                .filter(
                    FilterBuilder.and_(
                        FilterBuilder.equals("StatusId", "pending-uuid"),
                        FilterBuilder.starts_with("Request_FullName", "SF_"),
                    )
                )
            )
        )

        data = conn.output_module.export_json(body=query.build())

    Example — TableViewConfig for CSV::

        query = (
            OutputModuleQueryBuilder(use_table_view_config=True)
            .asset(ResourceBuilder("MyAsset").signifier("Name").id("Id"))
        )

        conn.output_module.export_csv(body=query.build())
    """

    def __init__(self, use_table_view_config: bool = False) -> None:
        """
        Args:
            use_table_view_config: When ``True`` wraps in ``TableViewConfig``
                                   (required for CSV / Excel / job exports).
                                   When ``False`` (default) wraps in ``ViewConfig``
                                   (used for JSON / XML direct exports).
        """
        self._config_key = "TableViewConfig" if use_table_view_config else "ViewConfig"
        self._resources: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Add resources
    # ------------------------------------------------------------------

    def add_resource(
        self,
        resource_type: str,
        builder: ResourceBuilder,
    ) -> "OutputModuleQueryBuilder":
        """
        Add a resource block to the query.

        Args:
            resource_type: Collibra entity type key, e.g. ``"Asset"``, ``"Domain"``,
                           ``"Community"``, ``"Term"``.
            builder:       A configured :class:`ResourceBuilder`.

        Returns:
            self for chaining.
        """
        if not isinstance(builder, ResourceBuilder):
            raise TypeError("builder must be a ResourceBuilder instance.")
        self._resources[resource_type] = builder.build_block()
        return self

    def asset(self, builder: ResourceBuilder) -> "OutputModuleQueryBuilder":
        """Add an Asset resource — shorthand for ``add_resource("Asset", builder)``."""
        return self.add_resource("Asset", builder)

    def domain(self, builder: ResourceBuilder) -> "OutputModuleQueryBuilder":
        """Add a Domain resource — shorthand for ``add_resource("Domain", builder)``."""
        return self.add_resource("Domain", builder)

    def community(self, builder: ResourceBuilder) -> "OutputModuleQueryBuilder":
        """Add a Community resource — shorthand for ``add_resource("Community", builder)``."""
        return self.add_resource("Community", builder)

    def term(self, builder: ResourceBuilder) -> "OutputModuleQueryBuilder":
        """Add a Term resource — shorthand for ``add_resource("Term", builder)``."""
        return self.add_resource("Term", builder)

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """
        Validate the query structure.

        Raises:
            ValueError: If no resources have been added.
        """
        if not self._resources:
            raise ValueError(
                "OutputModuleQueryBuilder: query must have at least one resource. "
                "Use .asset(), .domain(), .community(), or .add_resource()."
            )

    def build(self) -> Dict[str, Any]:
        """
        Build and return the complete query body as a dict.

        Returns:
            A dict with the structure ``{"ViewConfig": {"Resources": {...}}}``
            or ``{"TableViewConfig": {"Resources": {...}}}``.

        Raises:
            ValueError: If the query has no resources.
        """
        self.validate()
        return {
            self._config_key: {
                "Resources": self._resources
            }
        }

    def build_json(self, indent: Optional[int] = None) -> str:
        """
        Build and return the complete query body as a JSON string.

        Args:
            indent: Optional JSON indentation level.

        Returns:
            JSON-encoded string of the query body.
        """
        return json.dumps(self.build(), indent=indent)

    def __repr__(self) -> str:
        return (
            f"OutputModuleQueryBuilder("
            f"config={self._config_key!r}, "
            f"resources={list(self._resources.keys())})"
        )
