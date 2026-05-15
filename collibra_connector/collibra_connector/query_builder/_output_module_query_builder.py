"""collibra_connector.query_builder._output_module_query_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Top-level builder that wraps resource blocks in a ``ViewConfig`` payload.
"""

from __future__ import annotations

import copy
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from ._enums import ResourceType
from ._resource_builder import ResourceBuilder


class OutputModuleQueryBuilder:
    """Top-level builder — wraps resources in a ``ViewConfig`` payload.

    Returned instances can be passed **directly** to any
    :class:`~collibra_connector.api.OutputModule.OutputModule` export method
    (``export_json``, ``export_csv``, ``export_json_in_job``, etc.) without
    calling :meth:`build` first.

    Example::

        from collibra_connector import (
            OutputModuleQueryBuilder, ResourceBuilder,
            RelationBuilder, FilterBuilder, FilterOperator,
        )

        query = (
            OutputModuleQueryBuilder(display_length=-1)
            .term(
                ResourceBuilder("Data Set")
                .id("AssetId")
                .status("Status", signifier="Status Value")
                .attribute("Type", col="TypeValue", label_id="<uuid>")
                .relation(
                    RelationBuilder("<uuid>", RelationDirection.SOURCE, "DataSet_Table")
                    .target(ResourceBuilder("Table").id("TableId"))
                )
                .filter(FilterBuilder.and_(
                    FilterBuilder.field("Asset Type", FilterOperator.EQUALS, "Data Set"),
                    FilterBuilder.field("SFStatus", FilterOperator.IS_NULL),
                ))
            )
        )
        data = conn.output_module.export_json(query)
        print(query.to_json())
    """

    def __init__(
        self,
        display_length: Optional[int] = None,
        display_start: Optional[int] = None,
        max_count_limit: Optional[int] = None,
        table_view_config: bool = False,
    ) -> None:
        self._display_length = display_length
        self._display_start = display_start
        self._max_count_limit = max_count_limit
        self._use_table_view: bool = table_view_config
        self._resources: Dict[str, Any] = {}
        self._columns: Optional[List[str]] = None

    def resource(
        self,
        resource_type: Union[str, ResourceType],
        resource: Union[ResourceBuilder, Dict[str, Any]],
    ) -> "OutputModuleQueryBuilder":
        """Add a root resource entry.

        Args:
            resource_type: Key under ``Resources`` — use a :class:`ResourceType`
                           member or a custom string for non-standard types.
            resource:      A :class:`ResourceBuilder` or a raw dict.

        Raises:
            ValueError: If *resource_type* is blank.
            TypeError:  If *resource* is not a ``ResourceBuilder`` or dict.
        """
        if not resource_type:
            raise ValueError("OutputModuleQueryBuilder: 'resource_type' must not be empty.")
        key = resource_type.value if isinstance(resource_type, Enum) else resource_type
        c = copy.deepcopy(self)
        if isinstance(resource, ResourceBuilder):
            c._resources[key] = resource.build()
        elif isinstance(resource, dict):
            c._resources[key] = copy.deepcopy(resource)
        else:
            raise TypeError(
                f"OutputModuleQueryBuilder: expected ResourceBuilder or dict, "
                f"got {type(resource).__name__!r}."
            )
        return c

    def term(self, resource: Union[ResourceBuilder, Dict[str, Any]]) -> "OutputModuleQueryBuilder":
        """Shorthand for ``resource(ResourceType.TERM, ...)``."""
        return self.resource(ResourceType.TERM, resource)

    def asset(self, resource: Union[ResourceBuilder, Dict[str, Any]]) -> "OutputModuleQueryBuilder":
        """Shorthand for ``resource(ResourceType.ASSET, ...)``."""
        return self.resource(ResourceType.ASSET, resource)

    def display_length(self, length: int) -> "OutputModuleQueryBuilder":
        """Set ``displayLength`` (use ``-1`` to retrieve all records)."""
        c = copy.deepcopy(self)
        c._display_length = length
        return c

    def display_start(self, start: int) -> "OutputModuleQueryBuilder":
        """Set ``displayStart`` (zero-based page offset)."""
        c = copy.deepcopy(self)
        c._display_start = start
        return c

    def max_count_limit(self, limit: int) -> "OutputModuleQueryBuilder":
        """Set ``maxCountLimit`` to cap the count computation.

        A full count over large datasets can hurt performance. Setting this
        value avoids the problem. Use ``0`` to skip the count entirely.
        """
        c = copy.deepcopy(self)
        c._max_count_limit = limit
        return c

    def table_view(self) -> "OutputModuleQueryBuilder":
        """Switch the root payload key to ``TableViewConfig``.

        By default the builder produces a ``ViewConfig`` payload (compatible
        with JSON and XML exports). Call this method when targeting CSV or
        Excel exports, which require a ``TableViewConfig`` payload.

        A ``TableViewConfig`` also requires a ``Columns`` block — use
        :meth:`columns` to define which field aliases become output columns.

        Returns a new immutable copy of the builder with the flag set.

        Example::

            query = (
                OutputModuleQueryBuilder(display_length=5)
                .table_view()
                .resource(
                    "Community",
                    ResourceBuilder("Communities")
                    .id("communityId")
                    .display_name("communityName"),
                )
                .columns(["communityId", "communityName"])
            )
            # produces TableViewConfig with Columns block
        """
        c = copy.deepcopy(self)
        c._use_table_view = True
        return c

    def columns(self, field_names: List[str]) -> "OutputModuleQueryBuilder":
        """Set the ``Columns`` list for a ``TableViewConfig`` payload.

        Required when building a ``TableViewConfig`` (i.e. after calling
        :meth:`table_view`).  Each entry in *field_names* must match a
        ``name`` alias defined in the resource block.

        Args:
            field_names: Ordered list of field alias strings.  Each becomes
                         a ``{"Column": {"fieldName": "..."}}`` entry.

        Returns:
            New immutable builder copy with the Columns list stored.

        Example::

            query = (
                OutputModuleQueryBuilder(display_length=5)
                .table_view()
                .resource(
                    "Community",
                    ResourceBuilder("Communities")
                    .id("communityId")
                    .display_name("communityName"),
                )
                .columns(["communityId", "communityName"])
            )
        """
        if not field_names:
            raise ValueError("columns: field_names must not be empty.")
        c = copy.deepcopy(self)
        c._columns = list(field_names)
        return c

    def build(self) -> Dict[str, Any]:
        """Return the complete ``ViewConfig`` or ``TableViewConfig`` query dict.

        The root key is ``ViewConfig`` by default. Use :meth:`table_view` to
        produce a ``TableViewConfig`` payload instead (required for CSV/Excel).
        When building a ``TableViewConfig`` you must also call :meth:`columns`
        to define the output column mapping.

        Raises:
            ValueError: If no resource has been added yet.
        """
        if not self._resources:
            raise ValueError(
                "OutputModuleQueryBuilder: at least one resource must be "
                "added before calling build()."
            )
        config: Dict[str, Any] = {}
        if self._display_length is not None:
            config["displayLength"] = self._display_length
        if self._display_start is not None:
            config["displayStart"] = self._display_start
        if self._max_count_limit is not None:
            config["maxCountLimit"] = self._max_count_limit
        config["Resources"] = copy.deepcopy(self._resources)
        root_key = "TableViewConfig" if self._use_table_view else "ViewConfig"
        if self._use_table_view and self._columns is not None:
            config["Columns"] = [
                {"Column": {"fieldName": fn}} for fn in self._columns
            ]
        return {root_key: config}

    def to_json(self, indent: int = 2) -> str:
        """Serialize the query to a formatted JSON string."""
        return json.dumps(self.build(), indent=indent)

    def __repr__(self) -> str:  # pragma: no cover
        root = "TableViewConfig" if self._use_table_view else "ViewConfig"
        return f"OutputModuleQueryBuilder(root={root!r}, resources={list(self._resources.keys())!r})"
