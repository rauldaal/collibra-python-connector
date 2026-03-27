import uuid
from .Base import BaseAPI


class ComplexRelationType(BaseAPI):
    """API class for complex relation type operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/complexRelationTypes"

    def find_complex_relation_types(
        self,
        count_limit: int = -1,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
    ):
        """
        Returns complex relation types matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :return: List of complex relation types.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_complex_relation_type(self, complex_relation_type_id: str):
        """
        Returns the complex relation type identified by the given UUID.
        :param complex_relation_type_id: The UUID of the complex relation type.
        :return: Complex relation type details.
        """
        if not complex_relation_type_id:
            raise ValueError("complex_relation_type_id is required")
        try:
            uuid.UUID(complex_relation_type_id)
        except ValueError as exc:
            raise ValueError("complex_relation_type_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{complex_relation_type_id}")
        return self._handle_response(response)

    def get_complex_relation_type_by_public_id(self, public_id: str):
        """
        Returns the complex relation type identified by the given public ID.
        :param public_id: The public ID of the complex relation type.
        :return: Complex relation type details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)

    def add_complex_relation_type(self, name: str, description: str = None):
        """
        Adds a new complex relation type.
        :param name: The name of the complex relation type (required).
        :param description: Optional description.
        :return: Created complex relation type details.
        """
        if not name:
            raise ValueError("name is required")

        data = {"name": name}
        if description is not None:
            data["description"] = description

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def change_complex_relation_type(self, complex_relation_type_id: str, name: str = None,
                                      description: str = None):
        """
        Changes the complex relation type with the given ID.
        :param complex_relation_type_id: The UUID of the complex relation type to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :return: Updated complex relation type details.
        """
        if not complex_relation_type_id:
            raise ValueError("complex_relation_type_id is required")
        try:
            uuid.UUID(complex_relation_type_id)
        except ValueError as exc:
            raise ValueError("complex_relation_type_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{complex_relation_type_id}", data=data)
        return self._handle_response(response)

    def remove_complex_relation_type(self, complex_relation_type_id: str):
        """
        Removes the complex relation type identified by the given UUID.
        :param complex_relation_type_id: The UUID of the complex relation type.
        :return: None
        """
        if not complex_relation_type_id:
            raise ValueError("complex_relation_type_id is required")
        try:
            uuid.UUID(complex_relation_type_id)
        except ValueError as exc:
            raise ValueError("complex_relation_type_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{complex_relation_type_id}")
        return self._handle_response(response)
