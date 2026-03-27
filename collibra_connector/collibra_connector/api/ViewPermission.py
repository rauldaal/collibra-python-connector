import uuid
from .Base import BaseAPI


class ViewPermission(BaseAPI):
    """API class for view permission operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/viewPermissions"

    def find_view_permissions(
        self,
        count_limit: int = -1,
        include_inherited: bool = None,
        limit: int = 0,
        offset: int = 0,
        resource_discriminator: str = None,
        resource_id: str = None,
        resource_type: str = None,
        user_group_id: str = None,
        user_id: str = None,
    ):
        """
        Finds view permissions with given criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param include_inherited: Whether to include inherited permissions.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param resource_discriminator: The discriminator of the resource.
        :param resource_id: UUID of the resource to filter by.
        :param resource_type: Type of the resource to filter by.
        :param user_group_id: UUID of the user group to filter by.
        :param user_id: UUID of the user to filter by.
        :return: List of view permissions.
        """
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if resource_id is not None:
            try:
                uuid.UUID(resource_id)
            except ValueError as exc:
                raise ValueError("resource_id must be a valid UUID") from exc

        if user_group_id is not None:
            try:
                uuid.UUID(user_group_id)
            except ValueError as exc:
                raise ValueError("user_group_id must be a valid UUID") from exc

        if user_id is not None:
            try:
                uuid.UUID(user_id)
            except ValueError as exc:
                raise ValueError("user_id must be a valid UUID") from exc

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if include_inherited is not None:
            params["includeInherited"] = include_inherited
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if resource_discriminator is not None:
            params["resourceDiscriminator"] = resource_discriminator
        if resource_id is not None:
            params["resourceId"] = resource_id
        if resource_type is not None:
            params["resourceType"] = resource_type
        if user_group_id is not None:
            params["userGroupId"] = user_group_id
        if user_id is not None:
            params["userId"] = user_id

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_view_permission(self, view_permission_id: str):
        """
        Retrieves the view permission identified by the given UUID.
        :param view_permission_id: The UUID of the view permission.
        :return: View permission details.
        """
        if not view_permission_id:
            raise ValueError("view_permission_id is required")
        try:
            uuid.UUID(view_permission_id)
        except ValueError as exc:
            raise ValueError("view_permission_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{view_permission_id}")
        return self._handle_response(response)

    def add_view_permission(self, resource_id: str, resource_type: str,
                             user_id: str = None, user_group_id: str = None):
        """
        Adds a view permission. Can be applied to Community and Domain resource types.
        :param resource_id: The UUID of the resource (required).
        :param resource_type: The type of the resource (required). Options: Community, Domain.
        :param user_id: Optional UUID of the user to grant access to.
        :param user_group_id: Optional UUID of the user group to grant access to.
        :return: Created view permission details.
        """
        if not resource_id or not resource_type:
            raise ValueError("resource_id and resource_type are required")
        try:
            uuid.UUID(resource_id)
        except ValueError as exc:
            raise ValueError("resource_id must be a valid UUID") from exc

        if not user_id and not user_group_id:
            raise ValueError("Either user_id or user_group_id must be provided")

        if user_id is not None:
            try:
                uuid.UUID(user_id)
            except ValueError as exc:
                raise ValueError("user_id must be a valid UUID") from exc

        if user_group_id is not None:
            try:
                uuid.UUID(user_group_id)
            except ValueError as exc:
                raise ValueError("user_group_id must be a valid UUID") from exc

        data = {"resourceId": resource_id, "resourceType": resource_type}
        if user_id is not None:
            data["userId"] = user_id
        if user_group_id is not None:
            data["userGroupId"] = user_group_id

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def remove_view_permission(self, view_permission_id: str):
        """
        Removes the view permission identified by the given UUID.
        :param view_permission_id: The UUID of the view permission.
        :return: None
        """
        if not view_permission_id:
            raise ValueError("view_permission_id is required")
        try:
            uuid.UUID(view_permission_id)
        except ValueError as exc:
            raise ValueError("view_permission_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{view_permission_id}")
        return self._handle_response(response)
