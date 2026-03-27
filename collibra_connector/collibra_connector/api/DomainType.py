import uuid
from .Base import BaseAPI


class DomainType(BaseAPI):
    """API class for domain type operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/domainTypes"

    def find_domain_types(
        self,
        count_limit: int = -1,
        exclude_meta: bool = None,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
        parent_id: str = None,
        top_level: bool = None,
    ):
        """
        Returns domain types matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param exclude_meta: Whether to exclude meta domain types.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :param parent_id: UUID of parent domain type to filter by.
        :param top_level: Whether to only return top-level domain types.
        :return: List of domain types matching criteria.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if parent_id is not None:
            try:
                uuid.UUID(parent_id)
            except ValueError as exc:
                raise ValueError("parent_id must be a valid UUID") from exc

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if exclude_meta is not None:
            params["excludeMeta"] = exclude_meta
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset
        if parent_id is not None:
            params["parentId"] = parent_id
        if top_level is not None:
            params["topLevel"] = top_level

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_domain_type(self, domain_type_id: str):
        """
        Returns the domain type identified by the given UUID.
        :param domain_type_id: The UUID of the domain type.
        :return: Domain type details.
        """
        if not domain_type_id:
            raise ValueError("domain_type_id is required")
        try:
            uuid.UUID(domain_type_id)
        except ValueError as exc:
            raise ValueError("domain_type_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{domain_type_id}")
        return self._handle_response(response)

    def get_domain_type_by_public_id(self, public_id: str):
        """
        Returns the domain type identified by the given public ID.
        :param public_id: The public ID of the domain type.
        :return: Domain type details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)

    def add_domain_type(self, name: str, description: str = None, parent_id: str = None):
        """
        Adds a new domain type.
        :param name: The name of the domain type (required).
        :param description: Optional description.
        :param parent_id: Optional UUID of the parent domain type.
        :return: Created domain type details.
        """
        if not name:
            raise ValueError("name is required")

        if parent_id is not None:
            try:
                uuid.UUID(parent_id)
            except ValueError as exc:
                raise ValueError("parent_id must be a valid UUID") from exc

        data = {"name": name}
        if description is not None:
            data["description"] = description
        if parent_id is not None:
            data["parentId"] = parent_id

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_domain_types(self, domain_types: list):
        """
        Adds multiple new domain types in one go.
        :param domain_types: List of domain type objects.
        :return: Created domain types.
        """
        if not domain_types or not isinstance(domain_types, list):
            raise ValueError("domain_types must be a non-empty list")
        response = self._post(url=f"{self.__base_api}/bulk", data={"domainTypes": domain_types})
        return self._handle_response(response)

    def change_domain_type(self, domain_type_id: str, name: str = None,
                            description: str = None, parent_id: str = None):
        """
        Changes the domain type with the given ID.
        :param domain_type_id: The UUID of the domain type to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :param parent_id: Optional new parent UUID.
        :return: Updated domain type details.
        """
        if not domain_type_id:
            raise ValueError("domain_type_id is required")
        try:
            uuid.UUID(domain_type_id)
        except ValueError as exc:
            raise ValueError("domain_type_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if parent_id is not None:
            try:
                uuid.UUID(parent_id)
            except ValueError as exc:
                raise ValueError("parent_id must be a valid UUID") from exc
            data["parentId"] = parent_id

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{domain_type_id}", data=data)
        return self._handle_response(response)

    def change_domain_types(self, domain_types: list):
        """
        Changes multiple domain types in one go.
        :param domain_types: List of domain type change objects (must include id).
        :return: Updated domain types.
        """
        if not domain_types or not isinstance(domain_types, list):
            raise ValueError("domain_types must be a non-empty list")
        response = self._patch(url=f"{self.__base_api}/bulk", data={"domainTypes": domain_types})
        return self._handle_response(response)

    def remove_domain_type(self, domain_type_id: str):
        """
        Removes the domain type identified by the given UUID.
        :param domain_type_id: The UUID of the domain type.
        :return: None
        """
        if not domain_type_id:
            raise ValueError("domain_type_id is required")
        try:
            uuid.UUID(domain_type_id)
        except ValueError as exc:
            raise ValueError("domain_type_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{domain_type_id}")
        return self._handle_response(response)

    def remove_domain_types(self, domain_type_ids: list):
        """
        Removes multiple domain types.
        :param domain_type_ids: List of domain type UUIDs to remove.
        :return: None
        """
        if not domain_type_ids or not isinstance(domain_type_ids, list):
            raise ValueError("domain_type_ids must be a non-empty list")
        response = self._delete(url=f"{self.__base_api}/bulk")
        return self._handle_response(response)

    def find_sub_domain_types(self, domain_type_id: str, include_parent: bool = None):
        """
        Returns sub domain types of the domain type with the given ID.
        :param domain_type_id: The UUID of the domain type.
        :param include_parent: Whether to include the parent type in the results.
        :return: List of sub domain types.
        """
        if not domain_type_id:
            raise ValueError("domain_type_id is required")
        try:
            uuid.UUID(domain_type_id)
        except ValueError as exc:
            raise ValueError("domain_type_id must be a valid UUID") from exc

        params = {}
        if include_parent is not None:
            params["includeParent"] = include_parent

        response = self._get(url=f"{self.__base_api}/{domain_type_id}/subTypes",
                             params=params or None)
        return self._handle_response(response)
