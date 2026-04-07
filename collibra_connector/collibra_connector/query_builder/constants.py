"""
Constants for the Collibra Output Module Query Builder.
"""
from __future__ import annotations

from typing import Dict


VALID_FILTER_OPERATORS = frozenset({
    "EQUALS",
    "NOT_EQUALS",
    "GREATER_THAN",
    "LESS_THAN",
    "GREATER_THAN_OR_EQUALS",
    "LESS_THAN_OR_EQUALS",
    "CONTAINS",
    "NOT_CONTAINS",
    "STARTS_WITH",
    "ENDS_WITH",
    "IN",
    "NOT_IN",
    "IS_NULL",
    "IS_NOT_NULL",
})

_NULL_OPERATORS = frozenset({"IS_NULL", "IS_NOT_NULL"})

# Maps attribute type name → the key used to hold its projected value
ATTRIBUTE_VALUE_KEYS: Dict[str, str] = {
    "Attribute": "value",
    "StringAttribute": "value",
    "ScriptAttribute": "value",
    "FormattedStringAttribute": "value",
    "BooleanAttribute": "value",
    "NumericAttribute": "value",
    "DateAttribute": "date",
    "SingleValueListAttribute": "Value",
    "MultiValueListAttribute": "Value",
}
