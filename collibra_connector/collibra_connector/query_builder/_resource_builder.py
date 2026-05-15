"""collibra_connector.query_builder._resource_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Private helper functions and the main ResourceBuilder class.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Union

from ._relation_builder import RelationBuilder
from ._user_builder import UserBuilder


# ---------------------------------------------------------------------------
# Private helpers (used only within this module)
# ---------------------------------------------------------------------------


def _resolve_user(
    user: Union[UserBuilder, Dict[str, Any]],
    context: str,
) -> Dict[str, Any]:
    """Return a plain ``User`` dict from a :class:`UserBuilder` or a raw dict."""
    if isinstance(user, UserBuilder):
        return user.build()
    if isinstance(user, dict):
        return copy.deepcopy(user)
    raise TypeError(
        f"{context}: 'user' must be a UserBuilder or dict, "
        f"got {type(user).__name__!r}."
    )


def _signifier_block(
    name: str,
    signifier: Optional[str],
    id_col: Optional[str],
    lowercase_signifier: bool = False,
) -> Dict[str, Any]:
    """Build ``{"name": ..., "Signifier": ..., "Id": ...}`` (capital Signifier)."""
    block: Dict[str, Any] = {"name": name}
    if signifier:
        signifier_key = "signifier" if lowercase_signifier else "Signifier"
        block[signifier_key] = {"name": signifier}
    if id_col:
        block["Id"] = {"name": id_col}
    return block


def _named_label_block(
    name: str,
    label: Optional[str],
    id_col: Optional[str],
) -> Dict[str, Any]:
    """Build ``{"name": ..., "Name": ..., "Id": ...}`` (capital Name/Id)."""
    block: Dict[str, Any] = {"name": name}
    if label:
        block["Name"] = {"name": label}
    if id_col:
        block["Id"] = {"name": id_col}
    return block


def _attr_entry(
    name: str,
    value_key: str,
    col: str,
    label_id: Optional[str],
) -> Dict[str, Any]:
    """Build a generic attribute entry dict."""
    entry: Dict[str, Any] = {"name": name, value_key: {"name": col}}
    if label_id:
        entry["labelId"] = label_id
    return entry


# ---------------------------------------------------------------------------
# ResourceBuilder
# ---------------------------------------------------------------------------


class ResourceBuilder:
    """Builds a Collibra Output Module resource block.

    Covers ``Term``, ``Asset``, and any nested asset node (``Table``,
    ``Column``, etc.).  All methods return a **new** instance — chaining is
    safe and non-destructive.

    Raises:
        ValueError: If *name* is empty.
    """

    def __init__(self, name: str) -> None:
        if not name:
            raise ValueError("ResourceBuilder: 'name' must not be empty.")
        self._name = name
        self._fields: Dict[str, Any] = {}       # singular (dict) entries
        self._lists: Dict[str, List[Any]] = {}  # list-typed entries

    def _set(self, key: str, value: Any) -> "ResourceBuilder":
        c = copy.deepcopy(self)
        c._fields[key] = value
        return c

    def _append(self, key: str, value: Any) -> "ResourceBuilder":
        c = copy.deepcopy(self)
        c._lists.setdefault(key, []).append(value)
        return c

    # Simple column selectors

    def id(self, col: str) -> "ResourceBuilder":
        """``Id`` column selector."""
        return self._set("Id", {"name": col})

    def display_name(self, col: str) -> "ResourceBuilder":
        """``DisplayName`` column selector."""
        return self._set("DisplayName", {"name": col})

    def signifier(self, col: str) -> "ResourceBuilder":
        """``Signifier`` column selector."""
        return self._set("Signifier", {"name": col})

    def last_modified(self, col: str) -> "ResourceBuilder":
        """``lastModifiedIsoDate`` column selector."""
        return self._set("lastModifiedIsoDate", {"name": col})

    # Structured sub-blocks

    def status(
        self,
        name: str = "Status",
        signifier: Optional[str] = None,
        id_col: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Add the ``Status`` sub-block."""
        return self._set("Status", _signifier_block(name, signifier, id_col))

    def concept_type(
        self,
        name: str = "ConceptType",
        signifier: Optional[str] = None,
        id_col: Optional[str] = None,
        lowercase_signifier: bool = False,
    ) -> "ResourceBuilder":
        """Add a ``ConceptType`` sub-block.

        By default this uses a capital ``Signifier`` key. Set
        ``lowercase_signifier=True`` for payloads that require ``signifier``.
        """
        return self._set(
            "ConceptType",
            _signifier_block(name, signifier, id_col, lowercase_signifier=lowercase_signifier),
        )

    def asset_type(
        self,
        name: str = "AssetType",
        signifier: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Add an ``AssetType`` sub-block (lowercase ``signifier`` key).

        Note: some Collibra endpoints require the lowercase variant here.
        """
        block: Dict[str, Any] = {"name": name}
        if signifier:
            block["signifier"] = {"name": signifier}
        return self._set("AssetType", block)

    def vocabulary(
        self,
        name: str = "Vocabulary",
        domain_name: Optional[str] = None,
        id_col: Optional[str] = None,
        community: Optional[Union["ResourceBuilder", Dict[str, Any]]] = None,
    ) -> "ResourceBuilder":
        """Add a ``Vocabulary`` sub-block.

        Args:
            community: Optional community data — a :class:`ResourceBuilder`
                       (``build()`` is called) or a raw dict.
        """
        block = _named_label_block(name, domain_name, id_col)
        if community is not None:
            if isinstance(community, ResourceBuilder):
                block["Community"] = community.build()
            elif isinstance(community, dict):
                block["Community"] = copy.deepcopy(community)
            else:
                raise TypeError(
                    f"vocabulary: 'community' must be a ResourceBuilder or dict, "
                    f"got {type(community).__name__!r}."
                )
        return self._set("Vocabulary", block)

    def domain(
        self,
        name: str = "Domain",
        domain_name: Optional[str] = None,
        id_col: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Add a ``Domain`` sub-block (used in ``Asset``-rooted queries)."""
        return self._set("Domain", _named_label_block(name, domain_name, id_col))

    # Attribute types

    def attribute(
        self,
        name: str,
        col: str,
        label_id: Optional[str] = None,
        id_col: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Append a generic ``Attribute`` entry (``value`` key)."""
        entry = _attr_entry(name, "value", col, label_id)
        if id_col:
            entry["Id"] = {"name": id_col}
        return self._append("Attribute", entry)

    def string_attribute(
        self,
        name: str,
        col: str,
        label_id: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Append a ``StringAttribute`` entry (``value`` key)."""
        return self._append("StringAttribute", _attr_entry(name, "value", col, label_id))

    def single_value_list_attribute(
        self,
        name: Optional[str],
        col: str,
        label_id: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Append a ``SingleValueListAttribute`` entry (capital ``Value`` key).

        Some payloads omit ``name`` for this block; pass ``None`` to suppress it.
        """
        entry = _attr_entry(name if name is not None else "", "Value", col, label_id)
        if name is None:
            entry.pop("name", None)
        return self._append("SingleValueListAttribute", entry)

    def date_attribute(
        self,
        name: str,
        col: str,
        label_id: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Append a ``DateAttribute`` entry (``date`` key)."""
        return self._append("DateAttribute", _attr_entry(name, "date", col, label_id))

    def long_expression_attribute(
        self,
        name: str,
        col: str,
        label_id: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Append an ``Attribute`` entry using the ``LongExpression`` value key."""
        return self._append("Attribute", _attr_entry(name, "LongExpression", col, label_id))

    # Member / Responsibility

    def member(
        self,
        name: Optional[str] = None,
        role_id: Optional[str] = None,
        include_inherited: bool = False,
        user: Optional[Union[UserBuilder, Dict[str, Any]]] = None,
        id_col: Optional[str] = None,
        responsibility_source_col: Optional[str] = None,
        inherited_col: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Append a ``Member`` entry (``Term``-rooted queries)."""
        entry: Dict[str, Any] = {}
        if name is not None:
            entry["name"] = name
        if role_id:
            entry["roleId"] = role_id
        if include_inherited:
            entry["includeInherited"] = True
        if responsibility_source_col:
            entry["ResponsibilitySource"] = {"name": responsibility_source_col}
        if inherited_col:
            entry["Inherited"] = {"name": inherited_col}
        if id_col:
            entry["Id"] = {"name": id_col}
        if user is not None:
            entry["User"] = _resolve_user(user, "member")
        return self._append("Member", entry)

    def responsibility(
        self,
        name: str,
        role_id: Optional[str] = None,
        user: Optional[Union[UserBuilder, Dict[str, Any]]] = None,
        id_col: Optional[str] = None,
    ) -> "ResourceBuilder":
        """Append a ``Responsibility`` entry (``Asset``-rooted queries)."""
        entry: Dict[str, Any] = {"name": name}
        if role_id:
            entry["roleId"] = role_id
        if id_col:
            entry["Id"] = {"name": id_col}
        if user is not None:
            entry["User"] = _resolve_user(user, "responsibility")
        return self._append("Responsibility", entry)

    # Relations and filters

    def relation(
        self,
        *relations: Union[RelationBuilder, Dict[str, Any]],
    ) -> "ResourceBuilder":
        """Append one or more ``Relation`` entries.

        Raises:
            TypeError: If an argument is not a :class:`RelationBuilder` or dict.
        """
        result: "ResourceBuilder" = self
        for rel in relations:
            if isinstance(rel, RelationBuilder):
                result = result._append("Relation", rel.build())
            elif isinstance(rel, dict):
                result = result._append("Relation", copy.deepcopy(rel))
            else:
                raise TypeError(
                    f"relation: expected RelationBuilder or dict, "
                    f"got {type(rel).__name__!r}."
                )
        return result

    def filter(self, filter_dict: Dict[str, Any]) -> "ResourceBuilder":
        """Set the ``Filter`` block (use :class:`FilterBuilder` to construct it).

        Raises:
            TypeError: If *filter_dict* is not a dict.
        """
        if not isinstance(filter_dict, dict):
            raise TypeError(
                f"filter: expected dict, got {type(filter_dict).__name__!r}."
            )
        return self._set("Filter", copy.deepcopy(filter_dict))

    def raw_field(self, key: str, value: Any) -> "ResourceBuilder":
        """Set an arbitrary key — escape hatch for unsupported field types."""
        return self._set(key, copy.deepcopy(value))

    def build(self) -> Dict[str, Any]:
        """Return the resource dict ready for use in a query or relation block."""
        result: Dict[str, Any] = {"name": self._name}
        result.update(copy.deepcopy(self._fields))
        for key, lst in self._lists.items():
            result[key] = copy.deepcopy(lst)
        return result

    def __repr__(self) -> str:  # pragma: no cover
        return f"ResourceBuilder(name={self._name!r})"
