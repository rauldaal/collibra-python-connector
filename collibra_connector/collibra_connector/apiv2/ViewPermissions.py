import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class ViewPermissions(BaseAPI):
    """API class for view permission operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/viewPermissions"

    def find_view_permissions(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        user_id: str = None,
        user_group_id: str = None,
        resource_id: str = None,
        resource_type: str = None,
        resource_discriminator: str = None,
        include_inherited: bool = None
    ) -> Dict[str, Any]:
        """
        Searches for view permissions based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param user_id: The ID of the user to filter by (optional).
        :param user_group_id: The ID of the user group to filter by (optional).
        :param resource_id: The ID of the resource to filter by (optional).
        :param resource_type: The type of the resource to filter by (optional).
        :param resource_discriminator: Additional discriminator for the resource (optional).
        :param include_inherited: Whether to include inherited permissions (optional).
        :return: A dictionary containing the matching view permissions.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "userId": user_id,
            "userGroupId": user_group_id,
            "resourceId": resource_id,
            "resourceType": resource_type,
            "resourceDiscriminator": resource_discriminator,
            "includeInherited": include_inherited
        }
        
        # UUID validation
        for param_name in ["userId", "userGroupId", "resourceId"]:
            val = params.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_view_permission(
        self,
        resource_id: str,
        resource_type: str,
        user_id: str = None,
        user_group_id: str = None,
        base_resource: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new view permission for a resource.

        :param resource_id: The unique identifier of the resource (required).
        :param resource_type: The type of the resource (required).
        :param user_id: The unique identifier of the user (optional).
        :param user_group_id: The unique identifier of the user group (optional).
        :param base_resource: The base resource for the permission (optional).
        :return: A dictionary containing the details of the created view permission.
        """
        if not resource_id or not resource_type:
            raise ValueError("resource_id and resource_type are required")

        data = {
            "resourceId": resource_id,
            "resourceType": resource_type,
            "userId": user_id,
            "userGroupId": user_group_id,
            "baseResource": base_resource
        }

        if not self._uuid_validation(resource_id):
            raise ValueError("resource_id must be a valid UUID")
        if user_id and not self._uuid_validation(user_id):
            raise ValueError("userId must be a valid UUID")
        if user_group_id and not self._uuid_validation(user_group_id):
            raise ValueError("userGroupId must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_view_permission(self, view_permission_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a view permission by its ID.

        :param view_permission_id: The unique identifier of the view permission to retrieve.
        :return: A dictionary containing the details of the view permission.
        """
        response = self._get(url=f"{self.__base_api}/{view_permission_id}")
        return self._handle_response(response)

    def remove_view_permission(self, view_permission_id: str) -> None:
        """
        Deletes a view permission identified by its ID.

        :param view_permission_id: The unique identifier of the view permission to remove.
        """
        response = self._delete(url=f"{self.__base_api}/{view_permission_id}")
        return self._handle_response(response)
