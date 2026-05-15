"""collibra_connector.query_builder._user_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fluent builder for the ``User`` sub-block inside Member / Responsibility entries.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, Optional


class UserBuilder:
    """Fluent builder for the ``User`` sub-block inside Member / Responsibility.

    All methods return a **new** instance — chaining is safe and non-destructive.
    """

    def __init__(self, name: Optional[str] = None) -> None:
        self._data: Dict[str, Any] = {}
        if name is not None:
            self._data["name"] = name

    def _set(self, key: str, value: Any) -> "UserBuilder":
        c = copy.deepcopy(self)
        c._data[key] = value
        return c

    def name(self, name: str) -> "UserBuilder":
        """Set the block group label."""
        return self._set("name", name)

    def username(self, col: str) -> "UserBuilder":
        """Add the ``UserName`` column."""
        return self._set("UserName", {"name": col})

    def first_name(self, col: str) -> "UserBuilder":
        """Add the ``FirstName`` column."""
        return self._set("FirstName", {"name": col})

    def last_name(self, col: str) -> "UserBuilder":
        """Add the ``LastName`` column."""
        return self._set("LastName", {"name": col})

    def email(self, col: str) -> "UserBuilder":
        """Add the ``EmailAddress`` column."""
        return self._set("EmailAddress", {"name": col})

    def id(self, col: str) -> "UserBuilder":
        """Add the ``Id`` column."""
        return self._set("Id", {"name": col})

    def build(self) -> Dict[str, Any]:
        """Return the ``User`` dict."""
        return copy.deepcopy(self._data)
