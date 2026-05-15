import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Attributes(BaseAPI):
    """API class for attribute operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/attributes"

    def find_attributes(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        cursor: str = None,
        type_ids: List[str] = None,
        asset_id: str = None,
        sort_order: str = "DESC",
        sort_field: str = "LAST_MODIFIED",
        type_public_ids: List[str] = None
    ) -> Dict[str, Any]:
        """
        Searches for attributes based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param cursor: The cursor for paginated results (optional).
        :param type_ids: A list of UUIDs for the attribute types to filter by (optional).
        :param asset_id: The UUID of the asset to filter by (optional).
        :param sort_order: The order to sort the results in (default: "DESC").
        :param sort_field: The field to sort the results by (default: "LAST_MODIFIED").
        :param type_public_ids: A list of public IDs for the attribute types to filter by (optional).
        :return: A dictionary containing the matching attributes.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "cursor": cursor,
            "typeIds": type_ids,
            "assetId": asset_id,
            "sortOrder": sort_order,
            "sortField": sort_field,
            "typePublicIds": type_public_ids
        }
        
        if asset_id and not self._uuid_validation(asset_id):
            raise ValueError("assetId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_attribute(
        self,
        asset_id: str,
        type_id: str = None,
        value: Any = None,
        type_public_id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new attribute for an asset.

        :param asset_id: The UUID of the asset to add the attribute to (required).
        :param type_id: The UUID of the attribute type (optional).
        :param value: The value of the attribute (optional).
        :param type_public_id: The public ID of the attribute type (optional).
        :return: A dictionary containing the details of the created attribute.
        """
        if not asset_id:
            raise ValueError("asset_id is required")

        data = {
            "assetId": asset_id,
            "typeId": type_id,
            "value": value,
            "typePublicId": type_public_id
        }

        # UUID validation
        for param_name in ["assetId", "typeId"]:
            val = data.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_attributes(self, attributes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adds multiple attributes in bulk.

        :param attributes: A list of dictionaries, each representing an attribute to add.
        :return: A list of dictionaries containing the details of the added attributes.
        """
        if not attributes or not isinstance(attributes, list):
            raise ValueError("attributes must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=attributes)
        return self._handle_response(response)

    def remove_attributes(self, attribute_ids: List[str]) -> None:
        """
        Deletes multiple attributes identified by their IDs.

        :param attribute_ids: A list of unique identifiers for the attributes to delete.
        """
        if not attribute_ids or not isinstance(attribute_ids, list):
            raise ValueError("attribute_ids must be a non-empty list")
        
        response = self._delete(url=f"{self.__base_api}/bulk", data=attribute_ids)
        return self._handle_response(response)

    def change_attributes(self, attributes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Updates multiple attributes in bulk.

        :param attributes: A list of dictionaries, each representing an attribute to update.
        :return: A list of dictionaries containing the details of the updated attributes.
        """
        if not attributes or not isinstance(attributes, list):
            raise ValueError("attributes must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=attributes)
        return self._handle_response(response)

    def get_attribute(self, attribute_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of an attribute by its ID.

        :param attribute_id: The unique identifier of the attribute to retrieve.
        :return: A dictionary containing the details of the attribute.
        """
        if not self._uuid_validation(attribute_id):
            raise ValueError("attribute_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{attribute_id}")
        return self._handle_response(response)

    def remove_attribute(self, attribute_id: str) -> None:
        """
        Deletes an attribute identified by its ID.

        :param attribute_id: The unique identifier of the attribute to delete.
        """
        if not self._uuid_validation(attribute_id):
            raise ValueError("attribute_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{attribute_id}")
        return self._handle_response(response)

    def change_attribute(self, attribute_id: str = "", id: str = "", value: Any = None) -> Dict[str, Any]:
        """
        Updates the details of an attribute identified by its ID.

        :param attribute_id: The unique identifier of the attribute to update.
        :param id: The new unique identifier for the attribute (optional).
        :param value: The new value for the attribute (optional).
        :return: A dictionary containing the updated details of the attribute.
        """
        if not self._uuid_validation(attribute_id):
            raise ValueError("attribute_id must be a valid UUID")
        
        data = {"value": value}
        if id:
            if not self._uuid_validation(id):
                raise ValueError("id must be a valid UUID")
            data["id"] = id
        response = self._patch(url=f"{self.__base_api}/{attribute_id}", data=data)
        return self._handle_response(response)

    # --- Convenience and Legacy Methods ---

    def get_attributes(self, asset_id: str, type_ids: List[str] = None, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Retrieves attributes for an asset using legacy parameters.

        :param asset_id: The UUID of the asset to retrieve attributes for.
        :param type_ids: A list of UUIDs for the attribute types to filter by (optional).
        :param limit: The maximum number of results to return (default: 100).
        :param offset: The starting point for the search results (default: 0).
        :return: A dictionary containing the matching attributes.
        """
        return self.find_attributes(asset_id=asset_id, type_ids=type_ids, limit=limit, offset=offset)

    def get_attributes_as_dict(self, asset_id: str) -> Dict[str, Any]:
        """
        Retrieves all attributes for an asset as a dictionary.

        :param asset_id: The UUID of the asset to retrieve attributes for.
        :return: A dictionary where the keys are attribute type names and the values are attribute values.
        """
        result = self.find_attributes(asset_id=asset_id, limit=500)
        attrs_dict: Dict[str, Any] = {}

        for attr in result.get('results', []):
            type_name = attr.get('type', {}).get('name', 'Unknown')
            value = attr.get('value')
            attrs_dict[type_name] = value

        return attrs_dict
