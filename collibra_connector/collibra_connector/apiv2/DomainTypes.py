import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class DomainTypes(BaseAPI):
    """API class for domain type operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/domainTypes"

    def find_domain_types(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        parent_id: str = None,
        exclude_meta: bool = True,
        top_level: bool = False
    ) -> Dict[str, Any]:
        """
        Searches for domain types based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param name: The name of the domain type to search for (optional).
        :param name_match_mode: The mode for matching the name (default: "ANYWHERE").
        :param parent_id: The UUID of the parent domain type (optional).
        :param exclude_meta: Whether to exclude metadata domain types (default: True).
        :param top_level: Whether to include only top-level domain types (default: False).
        :return: A dictionary containing the matching domain types.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode,
            "parentId": parent_id,
            "excludeMeta": exclude_meta,
            "topLevel": top_level
        }
        
        if parent_id and not self._uuid_validation(parent_id):
            raise ValueError("parentId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_domain_type(
        self,
        name: str,
        id: str = None,
        description: str = None,
        parent_id: str = None,
        public_id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new domain type with the specified parameters.

        :param name: The name of the domain type (required).
        :param id: The unique identifier of the domain type (optional).
        :param description: A description of the domain type (optional).
        :param parent_id: The UUID of the parent domain type (optional).
        :param public_id: The public ID of the domain type (optional).
        :return: A dictionary containing the details of the created domain type.
        """
        if not name:
            raise ValueError("name is required")

        data = {
            "name": name,
            "id": id,
            "description": description,
            "parentId": parent_id,
            "publicId": public_id
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")
        if parent_id and not self._uuid_validation(parent_id):
            raise ValueError("parentId must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_domain_types(self, domain_types: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Creates multiple domain types in a single operation.

        :param domain_types: A list of dictionaries, each containing the details of a domain type to create.
        :return: A list of dictionaries containing the details of the created domain types.
        """
        if not domain_types or not isinstance(domain_types, list):
            raise ValueError("domain_types must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=domain_types)
        return self._handle_response(response)

    def remove_domain_types(self, domain_type_ids: List[str]) -> None:
        """
        Deletes multiple domain types identified by their IDs.

        :param domain_type_ids: A list of UUIDs for the domain types to delete.
        """
        if not domain_type_ids or not isinstance(domain_type_ids, list):
            raise ValueError("domain_type_ids must be a non-empty list")
        
        response = self._delete(url=f"{self.__base_api}/bulk", data=domain_type_ids)
        return self._handle_response(response)

    def change_domain_types(self, domain_types: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Updates multiple domain types with the specified details.

        :param domain_types: A list of dictionaries, each containing the updated details of a domain type.
        :return: A list of dictionaries containing the updated domain types.
        """
        if not domain_types or not isinstance(domain_types, list):
            raise ValueError("domain_types must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=domain_types)
        return self._handle_response(response)

    def get_domain_type(self, domain_type_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a domain type by its ID.

        :param domain_type_id: The UUID of the domain type to retrieve.
        :return: A dictionary containing the details of the domain type.
        """
        if not self._uuid_validation(domain_type_id):
            raise ValueError("domain_type_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{domain_type_id}")
        return self._handle_response(response)

    def remove_domain_type(self, domain_type_id: str) -> None:
        """
        Deletes a domain type identified by its ID.

        :param domain_type_id: The UUID of the domain type to delete.
        """
        if not self._uuid_validation(domain_type_id):
            raise ValueError("domain_type_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{domain_type_id}")
        return self._handle_response(response)

    def change_domain_type(
        self,
        domain_type_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parent_id: Optional[str] = None,
        public_id: Optional[str] = None,
        id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a domain type identified by its ID.

        :param domain_type_id: The UUID of the domain type to update.
        :param name: The new name of the domain type (optional).
        :param description: The new description of the domain type (optional).
        :param parent_id: The UUID of the new parent domain type (optional).
        :param public_id: The new public ID of the domain type (optional).
        :param id: The new unique identifier of the domain type (optional).
        :return: A dictionary containing the updated details of the domain type.
        """
        if not self._uuid_validation(domain_type_id):
            raise ValueError("domain_type_id must be a valid UUID")

        data = {
            "name": name,
            "description": description,
            "parentId": parent_id,
            "publicId": public_id,
            "id": id
        }

        # Remove keys with None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._patch(url=f"{self.__base_api}/{domain_type_id}", data=data)
        return self._handle_response(response)

    def find_sub_domain_types(self, domain_type_id: str, include_parent: bool = False) -> List[Dict[str, Any]]:
        """
        Finds all sub-domain types of a domain type by its ID.

        :param domain_type_id: The UUID of the domain type to find sub-types for.
        :param include_parent: Whether to include the parent domain type in the results (default: False).
        :return: A list of dictionaries containing the sub-domain types.
        """
        if not self._uuid_validation(domain_type_id):
            raise ValueError("domain_type_id must be a valid UUID")

        params = {"includeParent": include_parent}
        response = self._get(url=f"{self.__base_api}/{domain_type_id}/subTypes", params=params)
        return self._handle_response(response)

    def get_domain_type_by_public_id(self, public_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a domain type by its public ID.

        :param public_id: The public ID of the domain type to retrieve.
        :return: A dictionary containing the details of the domain type.
        """
        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)
