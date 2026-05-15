"""collibra_connector.query_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fluent builder for Collibra Output Module query payloads.

Use :class:`OutputModuleQueryBuilder` as the entry point.  Compose resources
with :class:`ResourceBuilder`, relations with :class:`RelationBuilder`, and
filter predicates with :class:`FilterBuilder`.  The :class:`FilterOperator`,
:class:`RelationDirection`, and :class:`ResourceType` enums replace magic
strings and provide IDE auto-complete.

Sub-modules (all private, not for direct import):
    _enums                        — FilterOperator, RelationDirection, ResourceType
    _filter_builder               — FilterBuilder
    _user_builder                 — UserBuilder
    _relation_builder             — RelationBuilder
    _resource_builder             — ResourceBuilder (+ private helpers)
    _output_module_query_builder  — OutputModuleQueryBuilder
"""

from ._enums import FilterOperator, RelationDirection, ResourceType
from ._filter_builder import FilterBuilder
from ._output_module_query_builder import OutputModuleQueryBuilder
from ._relation_builder import RelationBuilder
from ._resource_builder import ResourceBuilder
from ._user_builder import UserBuilder

__all__ = [
    "FilterOperator",
    "FilterBuilder",
    "RelationDirection",
    "RelationBuilder",
    "ResourceType",
    "ResourceBuilder",
    "UserBuilder",
    "OutputModuleQueryBuilder",
]
