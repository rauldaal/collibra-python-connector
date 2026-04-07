"""
Collibra Output Module Query Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fluent builder for constructing Collibra ViewConfig / TableViewConfig query bodies
used by the Output Module export endpoints.

Basic usage::

    from collibra_connector import OutputModuleQueryBuilder, ResourceBuilder, FilterBuilder

    query = (
        OutputModuleQueryBuilder()
        .asset(
            ResourceBuilder("Request")
            .signifier("Request_FullName")
            .display_name("Request_DisplayName")
            .id("Request_Id")
            .asset_type("AssetType", signifier_name="AssetTypeName", id_name="AssetTypeId")
            .domain("Domain", domain_name="DomainName", id_name="DomainId")
            .status("Status", value_name="StatusValue", id_name="StatusId")
            .attribute("attr-type-uuid", "AccessType", "AccessType_Value")
            .relation(
                type_id="rel-type-uuid",
                direction="SOURCE",
                name="Asset_to_Dataset",
                related=ResourceBuilder("Dataset")
                    .signifier("Dataset_FullName")
                    .id("Dataset_Id"),
            )
            .filter(
                FilterBuilder.and_(
                    FilterBuilder.equals("AssetTypeId", "asset-type-uuid"),
                    FilterBuilder.equals("DomainId", "domain-uuid"),
                    FilterBuilder.or_(
                        FilterBuilder.equals("StatusId", "status-uuid-1"),
                        FilterBuilder.equals("StatusId", "status-uuid-2"),
                    ),
                )
            )
        )
    )

    # Build the query body for export_json
    result = conn.output_module.export_json(body=query.build())

    # Or use the convenience method
    result = conn.output_module.export_json_query(query)

Sub-modules
-----------
- :mod:`~collibra_connector.query_builder.constants`
    Operator sets and attribute-type value-key mappings.
- :mod:`~collibra_connector.query_builder.filter_builder`
    :class:`FilterBuilder` — builds filter condition trees.
- :mod:`~collibra_connector.query_builder.resource_builder`
    :class:`ResourceBuilder` — builds resource projection blocks.
- :mod:`~collibra_connector.query_builder.output_module_query_builder`
    :class:`OutputModuleQueryBuilder` — assembles the full query body.
"""
from .constants import ATTRIBUTE_VALUE_KEYS, VALID_FILTER_OPERATORS, _NULL_OPERATORS
from .filter_builder import FilterBuilder
from .output_module_query_builder import OutputModuleQueryBuilder
from .resource_builder import ResourceBuilder

__all__ = [
    "FilterBuilder",
    "ResourceBuilder",
    "OutputModuleQueryBuilder",
    "VALID_FILTER_OPERATORS",
    "ATTRIBUTE_VALUE_KEYS",
]
