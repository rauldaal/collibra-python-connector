import uuid
from .Base import BaseAPI


class AttributeType(BaseAPI):
    """API class for attribute type operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/attributeTypes"

    def find_attribute_types(
        self,
        count_limit: int = -1,
        is_integer: bool = None,
        kind: str = None,
        language: str = None,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
        sort_field: str = "NAME",
        sort_order: str = "ASC",
        statistics_enabled: bool = None,
    ):
        """
        Returns attribute types matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param is_integer: Filter for numeric attribute types by whether they store integers.
        :param kind: The kind of attribute type. Options: STRING, BOOLEAN, DATE, NUMERIC,
                     SINGLE_VALUE_LIST, MULTI_VALUE_LIST, SCRIPT.
        :param language: Language code filter.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :param statistics_enabled: Whether statistics are enabled.
        :return: List of attribute types matching criteria.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if is_integer is not None:
            params["isInteger"] = is_integer
        if kind is not None:
            params["kind"] = kind
        if language is not None:
            params["language"] = language
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset
        if sort_field != "NAME":
            params["sortField"] = sort_field
        if sort_order != "ASC":
            params["sortOrder"] = sort_order
        if statistics_enabled is not None:
            params["statisticsEnabled"] = statistics_enabled

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_attribute_type(self, attribute_type_id: str):
        """
        Returns the attribute type identified by the given UUID.
        :param attribute_type_id: The UUID of the attribute type.
        :return: Attribute type details.
        """
        if not attribute_type_id:
            raise ValueError("attribute_type_id is required")
        try:
            uuid.UUID(attribute_type_id)
        except ValueError as exc:
            raise ValueError("attribute_type_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{attribute_type_id}")
        return self._handle_response(response)

    def get_attribute_type_by_name(self, attribute_type_name: str):
        """
        Returns the attribute type identified by the given name.
        :param attribute_type_name: The name of the attribute type.
        :return: Attribute type details.
        """
        if not attribute_type_name:
            raise ValueError("attribute_type_name is required")

        response = self._get(url=f"{self.__base_api}/name/{attribute_type_name}")
        return self._handle_response(response)

    def get_attribute_type_by_public_id(self, public_id: str):
        """
        Returns the attribute type identified by the given public ID.
        :param public_id: The public ID of the attribute type.
        :return: Attribute type details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)

    def add_attribute_type(self, name: str, kind: str, description: str = None,
                           is_integer: bool = None, statistics_enabled: bool = None):
        """
        Adds a new Attribute Type.
        :param name: The name of the attribute type (required).
        :param kind: The kind of the attribute type (required). Options: STRING, BOOLEAN, DATE,
                     NUMERIC, SINGLE_VALUE_LIST, MULTI_VALUE_LIST, SCRIPT.
        :param description: Optional description.
        :param is_integer: For NUMERIC types, whether to store integers only.
        :param statistics_enabled: Whether statistics are enabled.
        :return: Created attribute type details.
        """
        if not name:
            raise ValueError("name is required")
        if not kind:
            raise ValueError("kind is required")

        data = {"name": name, "kind": kind}
        if description is not None:
            data["description"] = description
        if is_integer is not None:
            data["isInteger"] = is_integer
        if statistics_enabled is not None:
            data["statisticsEnabled"] = statistics_enabled

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_attribute_types(self, attribute_types: list):
        """
        Adds multiple Attribute Types in one go.
        :param attribute_types: List of attribute type objects.
        :return: Created attribute types.
        """
        if not attribute_types or not isinstance(attribute_types, list):
            raise ValueError("attribute_types must be a non-empty list")
        response = self._post(url=f"{self.__base_api}/bulk", data={"attributeTypes": attribute_types})
        return self._handle_response(response)

    def change_attribute_type(self, attribute_type_id: str, name: str = None,
                               description: str = None, is_integer: bool = None,
                               statistics_enabled: bool = None):
        """
        Changes the attribute type with the given ID.
        :param attribute_type_id: The UUID of the attribute type to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :param is_integer: Optional integer setting.
        :param statistics_enabled: Optional statistics enabled setting.
        :return: Updated attribute type details.
        """
        if not attribute_type_id:
            raise ValueError("attribute_type_id is required")
        try:
            uuid.UUID(attribute_type_id)
        except ValueError as exc:
            raise ValueError("attribute_type_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if is_integer is not None:
            data["isInteger"] = is_integer
        if statistics_enabled is not None:
            data["statisticsEnabled"] = statistics_enabled

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{attribute_type_id}", data=data)
        return self._handle_response(response)

    def change_attribute_types(self, attribute_types: list):
        """
        Changes multiple attribute types in one go.
        :param attribute_types: List of attribute type change objects (must include id).
        :return: Updated attribute types.
        """
        if not attribute_types or not isinstance(attribute_types, list):
            raise ValueError("attribute_types must be a non-empty list")
        response = self._patch(url=f"{self.__base_api}/bulk", data={"attributeTypes": attribute_types})
        return self._handle_response(response)

    def remove_attribute_type(self, attribute_type_id: str):
        """
        Removes the attribute type identified by the given UUID.
        :param attribute_type_id: The UUID of the attribute type.
        :return: None
        """
        if not attribute_type_id:
            raise ValueError("attribute_type_id is required")
        try:
            uuid.UUID(attribute_type_id)
        except ValueError as exc:
            raise ValueError("attribute_type_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{attribute_type_id}")
        return self._handle_response(response)

    def remove_attribute_types(self, attribute_type_ids: list):
        """
        Removes multiple attribute types.
        :param attribute_type_ids: List of attribute type UUIDs to remove.
        :return: None
        """
        if not attribute_type_ids or not isinstance(attribute_type_ids, list):
            raise ValueError("attribute_type_ids must be a non-empty list")
        response = self._delete(url=f"{self.__base_api}/bulk")
        return self._handle_response(response)
