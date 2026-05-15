import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class ComplexRelationTypes(BaseAPI):
    """API class for complex relation type operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/complexRelationTypes"

    def find_complex_relation_types(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE"
    ) -> Dict[str, Any]:
        """
        Searches for complex relation types based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param name: The name of the complex relation type to search for (optional).
        :param name_match_mode: The mode for matching the name (default: "ANYWHERE").
        :return: A dictionary containing the matching complex relation types.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode
        }
        
        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_complex_relation_type(
        self,
        name: str,
        id: str = None,
        description: str = None,
        attribute_types: Optional[List[str]] = None,
        acronym_code: Optional[str] = None,
        public_id: Optional[str] = None,
        symbol_type: Optional[str] = None,
        icon_code: Optional[str] = None,
        leg_types: Optional[List[str]] = None,
        color: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Creates a new complex relation type with the specified parameters.

        :param name: The name of the complex relation type (required).
        :param id: The unique identifier of the complex relation type (optional).
        :param description: A description of the complex relation type (optional).
        :param attribute_types: A list of attribute types associated with the relation type (optional).
        :param acronym_code: The acronym code for the relation type (optional).
        :param public_id: The public ID of the relation type (optional).
        :param symbol_type: The symbol type for the relation type (optional).
        :param icon_code: The icon code for the relation type (optional).
        :param leg_types: A list of leg types associated with the relation type (optional).
        :param color: The color associated with the relation type (optional).
        :return: A dictionary containing the details of the created complex relation type.
        """
        if not name:
            raise ValueError("name is required")

        data = {
            "name": name,
            "id": id,
            "description": description,
            "attributeTypes": attribute_types,
            "acronymCode": acronym_code,
            "publicId": public_id,
            "symbolType": symbol_type,
            "iconCode": icon_code,
            "legTypes": leg_types,
            "color": color
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_complex_relation_type(self, complex_relation_type_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a complex relation type by its ID.

        :param complex_relation_type_id: The unique identifier of the complex relation type to retrieve.
        :return: A dictionary containing the details of the complex relation type.
        """
        if not self._uuid_validation(complex_relation_type_id):
            raise ValueError("complex_relation_type_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{complex_relation_type_id}")
        return self._handle_response(response)

    def remove_complex_relation_type(self, complex_relation_type_id: str) -> None:
        """
        Deletes a complex relation type identified by its ID.

        :param complex_relation_type_id: The unique identifier of the complex relation type to delete.
        """
        if not self._uuid_validation(complex_relation_type_id):
            raise ValueError("complex_relation_type_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{complex_relation_type_id}")
        return self._handle_response(response)

    def change_complex_relation_type(
        self,
        complex_relation_type_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        attribute_types: Optional[List[str]] = None,
        acronym_code: Optional[str] = None,
        public_id: Optional[str] = None,
        symbol_type: Optional[str] = None,
        icon_code: Optional[str] = None,
        leg_types: Optional[List[str]] = None,
        color: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a complex relation type identified by its ID.

        :param complex_relation_type_id: The unique identifier of the complex relation type to update.
        :param name: The new name of the complex relation type (optional).
        :param description: The new description of the complex relation type (optional).
        :param attribute_types: The new list of attribute types associated with the relation type (optional).
        :param acronym_code: The new acronym code for the relation type (optional).
        :param public_id: The new public ID of the relation type (optional).
        :param symbol_type: The new symbol type for the relation type (optional).
        :param icon_code: The new icon code for the relation type (optional).
        :param leg_types: The new list of leg types associated with the relation type (optional).
        :param color: The new color associated with the relation type (optional).
        :return: A dictionary containing the updated details of the complex relation type.
        """
        if not self._uuid_validation(complex_relation_type_id):
            raise ValueError("complex_relation_type_id must be a valid UUID")

        data = {
            "name": name,
            "description": description,
            "attributeTypes": attribute_types,
            "acronymCode": acronym_code,
            "publicId": public_id,
            "symbolType": symbol_type,
            "iconCode": icon_code,
            "legTypes": leg_types,
            "color": color
        }

        # Remove keys with None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._patch(url=f"{self.__base_api}/{complex_relation_type_id}", data=data)
        return self._handle_response(response)

    def get_complex_relation_type_by_public_id(self, public_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a complex relation type by its public ID.

        :param public_id: The public ID of the complex relation type to retrieve.
        :return: A dictionary containing the details of the complex relation type.
        """
        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)
