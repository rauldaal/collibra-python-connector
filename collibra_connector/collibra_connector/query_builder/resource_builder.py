"""
ResourceBuilder — builds a resource block for a Collibra Output Module query.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from .constants import ATTRIBUTE_VALUE_KEYS
from .filter_builder import FilterBuilder


class ResourceBuilder:
    """
    Builds a resource block (Asset, Domain, Community, User, or any related entity)
    for a Collibra Output Module query.

    The builder is fully reusable — the same class is used for top-level resources
    and for nested related entities inside ``relation()`` and ``responsibility()``.

    Example::

        rb = (
            ResourceBuilder("Request")
            .signifier("Request_FullName")
            .id("Request_Id")
            .status("Status", value_name="StatusValue", id_name="StatusId")
            .attribute("attr-uuid", "AccessType", "AccessType_Value")
            .relation(
                type_id="rel-uuid",
                direction="SOURCE",
                name="Req_to_DS",
                related=ResourceBuilder("DataSet").signifier("DS_Name").id("DS_Id"),
            )
            .filter(FilterBuilder.equals("StatusId", "status-uuid"))
        )
    """

    def __init__(self, name: str) -> None:
        """
        Args:
            name: The alias for this resource block. Used as a reference key in Filter
                  conditions and as a human-readable label for nested blocks.
        """
        self._block: Dict[str, Any] = {"name": name}
        self._filter: Optional[FilterBuilder] = None

    # ------------------------------------------------------------------
    # Scalar field projections
    # ------------------------------------------------------------------

    def field(self, field_type: str, projection_name: str) -> "ResourceBuilder":
        """
        Add a scalar field projection.

        Common field types: ``Signifier``, ``DisplayName``, ``Id``, ``Name``,
        ``UserName``, ``FirstName``, ``LastName``, ``EmailAddress``.

        Args:
            field_type:      The Collibra field key (case-sensitive).
            projection_name: Output column alias.
        """
        self._block[field_type] = {"name": projection_name}
        return self

    def signifier(self, projection_name: str) -> "ResourceBuilder":
        """Add the Signifier (full name) projection."""
        return self.field("Signifier", projection_name)

    def display_name(self, projection_name: str) -> "ResourceBuilder":
        """Add the DisplayName projection."""
        return self.field("DisplayName", projection_name)

    def id(self, projection_name: str) -> "ResourceBuilder":
        """Add the Id projection."""
        return self.field("Id", projection_name)

    # ------------------------------------------------------------------
    # Structured sub-blocks
    # ------------------------------------------------------------------

    def status(
        self,
        name: str,
        value_name: Optional[str] = None,
        id_name: Optional[str] = None,
    ) -> "ResourceBuilder":
        """
        Add a Status sub-block.

        Args:
            name:       Alias for the status block.
            value_name: Output column for the status label (Signifier).
            id_name:    Output column for the status UUID.
        """
        block: Dict[str, Any] = {"name": name}
        if value_name:
            block["Signifier"] = {"name": value_name}
        if id_name:
            block["Id"] = {"name": id_name}
        self._block["Status"] = block
        return self

    def asset_type(
        self,
        name: str,
        signifier_name: Optional[str] = None,
        id_name: Optional[str] = None,
    ) -> "ResourceBuilder":
        """
        Add an AssetType sub-block.

        Args:
            name:           Alias for the asset type block.
            signifier_name: Output column for the asset type label.
            id_name:        Output column for the asset type UUID.
        """
        block: Dict[str, Any] = {"name": name}
        if signifier_name:
            block["signifier"] = {"name": signifier_name}
        if id_name:
            block["Id"] = {"name": id_name}
        self._block["AssetType"] = block
        return self

    def domain(
        self,
        name: str,
        domain_name: Optional[str] = None,
        id_name: Optional[str] = None,
    ) -> "ResourceBuilder":
        """
        Add a Domain sub-block.

        Args:
            name:        Alias for the domain block.
            domain_name: Output column for the domain's Name.
            id_name:     Output column for the domain UUID.
        """
        block: Dict[str, Any] = {"name": name}
        if domain_name:
            block["Name"] = {"name": domain_name}
        if id_name:
            block["Id"] = {"name": id_name}
        self._block["Domain"] = block
        return self

    def community(
        self,
        name: str,
        community_name: Optional[str] = None,
        id_name: Optional[str] = None,
    ) -> "ResourceBuilder":
        """
        Add a Community sub-block.

        Args:
            name:           Alias for the community block.
            community_name: Output column for the community's Name.
            id_name:        Output column for the community UUID.
        """
        block: Dict[str, Any] = {"name": name}
        if community_name:
            block["Name"] = {"name": community_name}
        if id_name:
            block["Id"] = {"name": id_name}
        self._block["Community"] = block
        return self

    # ------------------------------------------------------------------
    # Attribute projections
    # ------------------------------------------------------------------

    def attribute(
        self,
        label_id: str,
        name: str,
        value_name: str,
        attr_type: str = "Attribute",
    ) -> "ResourceBuilder":
        """
        Add an attribute projection.

        Args:
            label_id:   UUID of the attribute type definition.
            name:       Alias for this attribute entry.
            value_name: Output column for the attribute value.
            attr_type:  Collibra attribute type key. One of:
                        ``Attribute``, ``StringAttribute``, ``DateAttribute``,
                        ``SingleValueListAttribute``, ``MultiValueListAttribute``,
                        ``NumericAttribute``, ``BooleanAttribute``,
                        ``FormattedStringAttribute``, ``ScriptAttribute``.
        """
        if attr_type not in ATTRIBUTE_VALUE_KEYS:
            raise ValueError(
                f"Unknown attribute type '{attr_type}'. "
                f"Valid: {sorted(ATTRIBUTE_VALUE_KEYS)}"
            )
        if attr_type not in self._block:
            self._block[attr_type] = []
        value_key = ATTRIBUTE_VALUE_KEYS[attr_type]
        self._block[attr_type].append({
            "labelId": label_id,
            "name": name,
            value_key: {"name": value_name},
        })
        return self

    def string_attribute(self, label_id: str, name: str, value_name: str) -> "ResourceBuilder":
        """Shorthand for StringAttribute."""
        return self.attribute(label_id, name, value_name, "StringAttribute")

    def date_attribute(self, label_id: str, name: str, value_name: str) -> "ResourceBuilder":
        """Shorthand for DateAttribute."""
        return self.attribute(label_id, name, value_name, "DateAttribute")

    def list_attribute(
        self,
        label_id: str,
        name: str,
        value_name: str,
        multi: bool = False,
    ) -> "ResourceBuilder":
        """Shorthand for SingleValueListAttribute or MultiValueListAttribute."""
        attr_type = "MultiValueListAttribute" if multi else "SingleValueListAttribute"
        return self.attribute(label_id, name, value_name, attr_type)

    def numeric_attribute(self, label_id: str, name: str, value_name: str) -> "ResourceBuilder":
        """Shorthand for NumericAttribute."""
        return self.attribute(label_id, name, value_name, "NumericAttribute")

    def boolean_attribute(self, label_id: str, name: str, value_name: str) -> "ResourceBuilder":
        """Shorthand for BooleanAttribute."""
        return self.attribute(label_id, name, value_name, "BooleanAttribute")

    # ------------------------------------------------------------------
    # Responsibility
    # ------------------------------------------------------------------

    def responsibility(
        self,
        role_id: str,
        name: str,
        id_name: Optional[str] = None,
        user: Optional["ResourceBuilder"] = None,
    ) -> "ResourceBuilder":
        """
        Add a Responsibility projection.

        Args:
            role_id:  UUID of the role definition.
            name:     Alias for this responsibility entry.
            id_name:  Output column for the responsibility UUID.
            user:     Optional ResourceBuilder for the User sub-block.
                      Typically built with .field("UserName", ...) etc.
        """
        if "Responsibility" not in self._block:
            self._block["Responsibility"] = []
        entry: Dict[str, Any] = {"name": name, "roleId": role_id}
        if id_name:
            entry["Id"] = {"name": id_name}
        if user is not None:
            entry["User"] = user.build_block()
        self._block["Responsibility"].append(entry)
        return self

    # ------------------------------------------------------------------
    # Relation traversal
    # ------------------------------------------------------------------

    def relation(
        self,
        type_id: str,
        direction: str,
        name: str,
        related: Optional["ResourceBuilder"] = None,
    ) -> "ResourceBuilder":
        """
        Add a Relation traversal to a related entity.

        Args:
            type_id:   UUID of the relation type definition.
            direction: ``"SOURCE"`` — this asset is the source of the relation,
                       the related entity is placed under ``TargetAsset``.
                       ``"TARGET"`` — this asset is the target of the relation,
                       the related entity is placed under ``SourceAsset``.
            name:      Alias for this relation entry.
            related:   Optional ResourceBuilder describing the related entity's
                       projections and nested structure.
        """
        direction = direction.upper()
        if direction not in ("SOURCE", "TARGET"):
            raise ValueError(
                f"direction must be 'SOURCE' or 'TARGET', got '{direction}'."
            )
        if "Relation" not in self._block:
            self._block["Relation"] = []
        entry: Dict[str, Any] = {
            "typeId": type_id,
            "type": direction,
            "name": name,
        }
        if related is not None:
            related_key = "TargetAsset" if direction == "SOURCE" else "SourceAsset"
            entry[related_key] = related.build_block()
        self._block["Relation"].append(entry)
        return self

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------

    def filter(self, f: FilterBuilder) -> "ResourceBuilder":
        """Attach a filter tree to this resource."""
        if not isinstance(f, FilterBuilder):
            raise TypeError("filter() expects a FilterBuilder instance.")
        self._filter = f
        return self

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build_block(self) -> Dict[str, Any]:
        """Return the inner resource dict (without the resource-type wrapper key)."""
        result = dict(self._block)
        if self._filter is not None:
            result["Filter"] = self._filter.build()
        return result

    def __repr__(self) -> str:
        return f"ResourceBuilder(name={self._block.get('name')!r})"
