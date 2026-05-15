import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Responsibilities(BaseAPI):
    """API class for responsibility operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/responsibilities"

    def find_responsibilities(
        self,
        offset: int = 0,
        limit: int = 100,
        count_limit: int = -1,
        sort_field: str = None,
        sort_order: str = "ASC",
        owner_ids: List[str] = None,
        role_ids: List[str] = None,
        resource_ids: List[str] = None,
        include_inherited: bool = False,
        exclude_empty_groups: bool = False,
        type: str = None,
        global_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Finds responsibilities based on the provided filters.
        :param offset: The starting point for the results.
        :param limit: The maximum number of results to return.
        :param count_limit: The maximum count of results.
        :param sort_field: The field to sort by.
        :param sort_order: The order of sorting (ASC or DESC).
        :param owner_ids: Filter by owner IDs.
        :param role_ids: Filter by role IDs.
        :param resource_ids: Filter by resource IDs.
        :param include_inherited: Include inherited responsibilities.
        :param exclude_empty_groups: Exclude empty groups.
        :param type: Filter by type.
        :param global_only: Filter for global responsibilities only.
        :return: A list of responsibilities matching the criteria.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "sortField": sort_field,
            "sortOrder": sort_order,
            "includeInherited": include_inherited,
            "excludeEmptyGroups": exclude_empty_groups,
            "type": type,
            "globalOnly": global_only
        }

        if owner_ids:
            params["ownerIds"] = owner_ids
        if role_ids:
            params["roleIds"] = role_ids
        if resource_ids:
            params["resourceIds"] = resource_ids

        response = self._get(url=f"{self.__base_api}/responsibilities", params=params)
        return self._handle_response(response)

    def add_responsibility(
        self,
        role_id: str,
        owner_id: str,
        resource_id: str,
        resource_type: str,
        resource_discriminator: str = None
    ) -> Dict[str, Any]:
        """
        Adds a single responsibility.
        :param role_id: The ID of the role.
        :param owner_id: The ID of the owner.
        :param resource_id: The ID of the resource.
        :param resource_type: The type of the resource.
        :param resource_discriminator: Additional discriminator for the resource.
        :return: Details of the added responsibility.
        """
        data = {
            "roleId": role_id,
            "ownerId": owner_id,
            "resourceId": resource_id,
            "resourceType": resource_type
        }

        if resource_discriminator:
            data["resourceDiscriminator"] = resource_discriminator

        response = self._post(url=f"{self.__base_api}/responsibilities", data=data)
        return self._handle_response(response)

    def add_responsibilities(self, responsibilities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adds multiple responsibilities in bulk.
        :param responsibilities: A list of responsibility objects to add.
        :return: Details of the added responsibilities.
        """
        if not responsibilities or not isinstance(responsibilities, list):
            raise ValueError("responsibilities must be a non-empty list")

        response = self._post(url=f"{self.__base_api}/responsibilities/bulk", data=responsibilities)
        return self._handle_response(response)

    def get_responsibility(self, responsibility_id: str) -> Dict[str, Any]:
        """
        Returns the responsibility identified by the given UUID.
        """
        if not self._uuid_validation(responsibility_id):
            raise ValueError("responsibility_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{responsibility_id}")
        return self._handle_response(response)

    def remove_responsibility(self, responsibility_id: str) -> None:
        """
        Removes the responsibility identified by the given UUID.
        """
        if not self._uuid_validation(responsibility_id):
            raise ValueError("responsibility_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{responsibility_id}")
        return self._handle_response(response)

    def remove_responsibilities(self, responsibility_ids: List[str]) -> None:
        """
        Removes multiple responsibilities identified by their IDs in bulk.
        :param responsibility_ids: A list of responsibility IDs to remove.
        :return: None.
        """
        if not responsibility_ids or not isinstance(responsibility_ids, list):
            raise ValueError("responsibility_ids must be a non-empty list")

        for responsibility_id in responsibility_ids:
            if not isinstance(responsibility_id, str):
                raise ValueError(f"responsibility_id {responsibility_id} must be a string")

        response = self._delete(url=f"{self.__base_api}/responsibilities/bulk", data=responsibility_ids)
        return self._handle_response(response)

    def change_responsibility(self, responsibility_id: str, role_id: str = None, owner_id: str = None) -> Dict[str, Any]:
        """
        Changes the responsibility with the given ID.
        """
        if not self._uuid_validation(responsibility_id):
            raise ValueError("responsibility_id must be a valid UUID")
        
        data = {"roleId": role_id, "ownerId": owner_id}
        response = self._patch(url=f"{self.__base_api}/{responsibility_id}", data=data)
        return self._handle_response(response)
