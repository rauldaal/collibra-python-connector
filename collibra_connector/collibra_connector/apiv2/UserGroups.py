import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class UserGroups(BaseAPI):
    """API class for user group operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/userGroups"

    def find_user_groups(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        user_id: str = None,
        include_everyone: bool = None
    ) -> Dict[str, Any]:
        """
        Returns user groups matching the given search criteria.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode,
            "userId": user_id,
            "includeEveryone": include_everyone
        }
        
        if user_id and not self._uuid_validation(user_id):
            raise ValueError("userId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_user_group(self, name: str, id: Optional[str] = None, locally_managed: bool = False) -> Dict[str, Any]:
        """
        Adds a new user group.
        :param name: The name of the user group.
        :param id: The ID of the user group (optional).
        :param locally_managed: Whether the user group is locally managed (default: False).
        """
        payload = {"name": name, "locallyManaged": locally_managed}
        if id:
            payload["id"] = id

        response = self._post(url=self.__base_api, data=payload)
        return self._handle_response(response)

    def change_user_group(self, user_group_id: str, name: str, locally_managed: bool = False) -> Dict[str, Any]:
        """
        Updates an existing user group.
        :param user_group_id: The ID of the user group.
        :param name: The new name of the user group.
        :param locally_managed: Whether the user group is locally managed (default: False).
        """
        if not self._uuid_validation(user_group_id):
            raise ValueError("userGroupId must be a valid UUID")

        payload = {"name": name, "locallyManaged": locally_managed}

        response = self._put(url=f"{self.__base_api}/{user_group_id}", data=payload)
        return self._handle_response(response)

    def get_user_group(self, user_group_id: str) -> Dict[str, Any]:
        """
        Returns the user group with the given ID.
        """
        if not self._uuid_validation(user_group_id):
            raise ValueError("user_group_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{user_group_id}")
        return self._handle_response(response)

    def remove_user_group(self, user_group_id: str) -> None:
        """
        Removes the user group identified by the given ID.
        """
        if not self._uuid_validation(user_group_id):
            raise ValueError("user_group_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{user_group_id}")
        return self._handle_response(response)

    def find_users_of_group(self, user_group_id: str, offset: int = 0, limit: int = 0) -> Dict[str, Any]:
        """
        Returns the users belonging to the user group with the given ID.
        """
        if not self._uuid_validation(user_group_id):
            raise ValueError("user_group_id must be a valid UUID")
        
        params = {"offset": offset, "limit": limit}
        response = self._get(url=f"{self.__base_api}/{user_group_id}/users", params=params)
        return self._handle_response(response)

    def add_user_to_group(self, user_group_id: str, user_id: str) -> None:
        """
        Adds a user to a user group.
        """
        if not self._uuid_validation(user_group_id) or not self._uuid_validation(user_id):
            raise ValueError("user_group_id and user_id must be valid UUIDs")
        
        response = self._post(url=f"{self.__base_api}/{user_group_id}/users/{user_id}", data={})
        return self._handle_response(response)

    def remove_user_from_group(self, user_group_id: str, user_id: str) -> None:
        """
        Removes a user from a user group.
        """
        if not self._uuid_validation(user_group_id) or not self._uuid_validation(user_id):
            raise ValueError("user_group_id and user_id must be valid UUIDs")
        
        response = self._delete(url=f"{self.__base_api}/{user_group_id}/users/{user_id}")
        return self._handle_response(response)

    def add_users_to_user_group(self, user_group_id: str, user_ids: List[str]) -> None:
        """
        Adds users to a specific user group.
        :param user_group_id: The ID of the user group.
        :param user_ids: List of user IDs to add to the user group.
        """
        if not self._uuid_validation(user_group_id):
            raise ValueError("userGroupId must be a valid UUID")

        response = self._post(
            url=f"{self.__base_api}/{user_group_id}/users",
            data={"userIds": user_ids}
        )
        return self._handle_response(response)

    def remove_users_from_user_group(self, user_group_id: str, user_ids: List[str]) -> None:
        """
        Removes users from a specific user group.
        :param user_group_id: The ID of the user group.
        :param user_ids: List of user IDs to remove from the user group.
        """
        if not self._uuid_validation(user_group_id):
            raise ValueError("userGroupId must be a valid UUID")

        response = self._delete(
            url=f"{self.__base_api}/{user_group_id}/users",
            data={"userIds": user_ids}
        )
        return self._handle_response(response)
