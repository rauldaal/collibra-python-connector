import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class AttributeTypes(BaseAPI):
    """API class for attribute type operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/attributeTypes"

    def find_attribute_types(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        kind: str = None,
        language: str = None,
        statistics_enabled: bool = None,
        is_integer: bool = None,
        sort_field: str = "NAME",
        sort_order: str = "ASC"
    ) -> Dict[str, Any]:
        """
        Searches for attribute types based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param name: The name of the attribute type to filter by (optional).
        :param name_match_mode: The mode for matching the name (default: "ANYWHERE").
        :param kind: The kind of the attribute type to filter by (optional).
        :param language: The language of the attribute type to filter by (optional).
        :param statistics_enabled: Whether to filter by attribute types with statistics enabled (optional).
        :param is_integer: Whether to filter by attribute types that are integers (optional).
        :param sort_field: The field to sort the results by (default: "NAME").
        :param sort_order: The order to sort the results in (default: "ASC").
        :return: A dictionary containing the matching attribute types.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode,
            "kind": kind,
            "language": language,
            "statisticsEnabled": statistics_enabled,
            "isInteger": is_integer,
            "sortField": sort_field,
            "sortOrder": sort_order
        }
        
        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_attribute_type(
        self,
        name: str,
        kind: str,
        id: str = None,
        public_id: str = None,
        description: str = None,
        language: str = None,
        statistics_enabled: bool = None,
        is_integer: bool = None,
        allowed_values: List[str] = None,
        string_type: str = None,
        id_string: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new attribute type with the specified parameters.

        :param name: The name of the attribute type (required).
        :param kind: The kind of the attribute type (required).
        :param id: The unique identifier of the attribute type (optional).
        :param public_id: The public ID of the attribute type (optional).
        :param description: A description of the attribute type (optional).
        :param language: The language of the attribute type (optional).
        :param statistics_enabled: Whether statistics are enabled for the attribute type (optional).
        :param is_integer: Whether the attribute type is an integer (optional).
        :param allowed_values: A list of allowed values for the attribute type (optional).
        :param string_type: The string type of the attribute type (optional).
        :param id_string: The ID string of the attribute type (optional).
        :return: A dictionary containing the details of the created attribute type.
        """
        if not name or not kind:
            raise ValueError("name and kind are required")

        data = {
            "name": name,
            "kind": kind,
            "id": id,
            "publicId": public_id,
            "description": description,
            "language": language,
            "statisticsEnabled": statistics_enabled,
            "isInteger": is_integer,
            "allowedValues": allowed_values,
            "stringType": string_type,
            "idString": id_string
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_attribute_types(self, attribute_types: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adds multiple attribute types in bulk.

        :param attribute_types: A list of dictionaries, each representing an attribute type to add.
        :return: A list of dictionaries containing the details of the added attribute types.
        """
        if not attribute_types or not isinstance(attribute_types, list):
            raise ValueError("attribute_types must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=attribute_types)
        return self._handle_response(response)

    def remove_attribute_types(self, attribute_type_ids: List[str]) -> None:
        """
        Deletes multiple attribute types identified by their IDs.

        :param attribute_type_ids: A list of unique identifiers for the attribute types to delete.
        """
        if not attribute_type_ids or not isinstance(attribute_type_ids, list):
            raise ValueError("attribute_type_ids must be a non-empty list")
        
        response = self._delete(url=f"{self.__base_api}/bulk", data=attribute_type_ids)
        return self._handle_response(response)

    def change_attribute_types(self, attribute_types: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Updates multiple attribute types in bulk.

        :param attribute_types: A list of dictionaries, each representing an attribute type to update.
        :return: A list of dictionaries containing the details of the updated attribute types.
        """
        if not attribute_types or not isinstance(attribute_types, list):
            raise ValueError("attribute_types must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=attribute_types)
        return self._handle_response(response)

    def get_attribute_type(self, attribute_type_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of an attribute type by its ID.

        :param attribute_type_id: The unique identifier of the attribute type to retrieve.
        :return: A dictionary containing the details of the attribute type.
        """
        if not self._uuid_validation(attribute_type_id):
            raise ValueError("attribute_type_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{attribute_type_id}")
        return self._handle_response(response)

    def remove_attribute_type(self, attribute_type_id: str) -> None:
        """
        Deletes an attribute type identified by its ID.

        :param attribute_type_id: The unique identifier of the attribute type to delete.
        """
        if not self._uuid_validation(attribute_type_id):
            raise ValueError("attribute_type_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{attribute_type_id}")
        return self._handle_response(response)

    def change_attribute_type(
        self,
        attribute_type_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        public_id: Optional[str] = None,
        language: Optional[str] = None,
        is_integer: Optional[bool] = None,
        statistics_enabled: Optional[bool] = None,
        allowed_values: Optional[List[str]] = None,
        id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Updates the details of an attribute type identified by its ID.

        :param attribute_type_id: The unique identifier of the attribute type to update.
        :param name: The new name for the attribute type (optional).
        :param description: The new description for the attribute type (optional).
        :param public_id: The new public ID for the attribute type (optional).
        :param language: The new language for the attribute type (optional).
        :param is_integer: Whether the attribute type is an integer (optional).
        :param statistics_enabled: Whether statistics are enabled for the attribute type (optional).
        :param allowed_values: The new list of allowed values for the attribute type (optional).
        :param id: The new unique identifier for the attribute type (optional).
        :return: A dictionary containing the updated details of the attribute type.
        """
        if not self._uuid_validation(attribute_type_id):
            raise ValueError("attribute_type_id must be a valid UUID")

        data = {
            "name": name,
            "description": description,
            "publicId": public_id,
            "language": language,
            "isInteger": is_integer,
            "statisticsEnabled": statistics_enabled,
            "allowedValues": allowed_values,
            "id": id
        }

        # Remove keys with None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._patch(url=f"{self.__base_api}/{attribute_type_id}", data=data)
        return self._handle_response(response)

    def get_attribute_type_by_name(self, attribute_type_name: str) -> Dict[str, Any]:
        """
        Retrieves the details of an attribute type by its name.

        :param attribute_type_name: The name of the attribute type to retrieve.
        :return: A dictionary containing the details of the attribute type.
        """
        response = self._get(url=f"{self.__base_api}/name/{attribute_type_name}")
        return self._handle_response(response)

    def get_attribute_type_by_public_id(self, public_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of an attribute type by its public ID.

        :param public_id: The public ID of the attribute type to retrieve.
        :return: A dictionary containing the details of the attribute type.
        """
        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)
