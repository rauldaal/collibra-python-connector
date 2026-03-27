import uuid
from .Base import BaseAPI


class Role(BaseAPI):
    """API class for role operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/roles"

    def find_roles(
        self,
        count_limit: int = -1,
        description: str = None,
        global_role: bool = None,
        include_disabled: bool = None,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
        owner_id: str = None,
        role_inheritance_mode: str = None,
        sort_field: str = "NAME",
        sort_order: str = "ASC",
        system_name: str = None,
        role_type: str = None,
    ):
        """
        Returns roles matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param description: Description to search for.
        :param global_role: Whether to filter by global roles.
        :param include_disabled: Whether to include disabled roles.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Name matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :param owner_id: UUID of the owner to filter by.
        :param role_inheritance_mode: Mode for inheriting roles.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :param system_name: System name to filter by.
        :param role_type: Type of role to filter by.
        :return: List of roles.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if owner_id is not None:
            try:
                uuid.UUID(owner_id)
            except ValueError as exc:
                raise ValueError("owner_id must be a valid UUID") from exc

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if description is not None:
            params["description"] = description
        if global_role is not None:
            params["global"] = global_role
        if include_disabled is not None:
            params["includeDisabled"] = include_disabled
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset
        if owner_id is not None:
            params["ownerId"] = owner_id
        if role_inheritance_mode is not None:
            params["roleInheritanceMode"] = role_inheritance_mode
        if sort_field != "NAME":
            params["sortField"] = sort_field
        if sort_order != "ASC":
            params["sortOrder"] = sort_order
        if system_name is not None:
            params["systemName"] = system_name
        if role_type is not None:
            params["type"] = role_type

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_role(self, role_id: str):
        """
        Returns the role identified by the given UUID.
        :param role_id: The UUID of the role.
        :return: Role details.
        """
        if not role_id:
            raise ValueError("role_id is required")
        try:
            uuid.UUID(role_id)
        except ValueError as exc:
            raise ValueError("role_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{role_id}")
        return self._handle_response(response)

    def add_role(self, name: str, description: str = None, global_role: bool = None):
        """
        Adds a new role.
        :param name: The name of the role (required).
        :param description: Optional description.
        :param global_role: Whether this is a global role.
        :return: Created role details.
        """
        if not name:
            raise ValueError("name is required")

        data = {"name": name}
        if description is not None:
            data["description"] = description
        if global_role is not None:
            data["global"] = global_role

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def change_role(self, role_id: str, name: str = None, description: str = None):
        """
        Changes the role with the given ID.
        :param role_id: The UUID of the role to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :return: Updated role details.
        """
        if not role_id:
            raise ValueError("role_id is required")
        try:
            uuid.UUID(role_id)
        except ValueError as exc:
            raise ValueError("role_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{role_id}", data=data)
        return self._handle_response(response)

    def remove_role(self, role_id: str):
        """
        Removes the role identified by the given UUID.
        :param role_id: The UUID of the role.
        :return: None
        """
        if not role_id:
            raise ValueError("role_id is required")
        try:
            uuid.UUID(role_id)
        except ValueError as exc:
            raise ValueError("role_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{role_id}")
        return self._handle_response(response)
