import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Roles(BaseAPI):
    """API class for role operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/roles"

    def find_roles(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        role_type: str = None,
        system_name: str = None,
        description: str = None,
        include_disabled: bool = None,
        owner_id: str = None,
        role_inheritance_mode: str = None,
        sort_field: str = "NAME",
        sort_order: str = "ASC",
        global_: bool = None,
        type: str = None
    ) -> Dict[str, Any]:
        """
        Searches for roles based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param name: The name of the role to search for (optional).
        :param name_match_mode: The matching mode for the name (default: "ANYWHERE").
        :param role_type: The type of the role (optional).
        :param system_name: The system name of the role (optional).
        :param description: The description of the role (optional).
        :param include_disabled: Whether to include disabled roles (optional).
        :param owner_id: The ID of the owner of the role (optional).
        :param role_inheritance_mode: The inheritance mode of the role (optional).
        :param sort_field: The field to sort the results by (default: "NAME").
        :param sort_order: The order to sort the results in (default: "ASC").
        :param global_: Whether to include global roles (optional).
        :param type: The type of the role (optional).
        :return: A dictionary containing the matching roles.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode,
            "roleType": role_type,
            "systemName": system_name,
            "description": description,
            "includeDisabled": include_disabled,
            "ownerId": owner_id,
            "roleInheritanceMode": role_inheritance_mode,
            "sortField": sort_field,
            "sortOrder": sort_order,
            "global": global_,
            "type": type
        }

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_role(
        self,
        name: str,
        id: str = None,
        description: str = None,
        permissions: List[str] = None,
        disabled: bool = None,
        global_: bool = None
    ) -> Dict[str, Any]:
        """
        Creates a new role with the specified parameters.

        :param name: The name of the role (required).
        :param id: The unique identifier for the role (optional).
        :param description: A description of the role (optional).
        :param permissions: A list of permissions associated with the role (optional).
        :param disabled: Whether the role is disabled (optional).
        :param global_: Whether the role is global (optional).
        :return: A dictionary containing the details of the created role.
        """
        if not name:
            raise ValueError("name is required")

        data = {
            "name": name,
            "id": id,
            "description": description,
            "global": global_,
            "permissions": permissions,
            "disabled": disabled
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_role(self, role_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a role by its ID.

        :param role_id: The unique identifier of the role to retrieve.
        :return: A dictionary containing the details of the role.
        """
        if not self._uuid_validation(role_id):
            raise ValueError("role_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{role_id}")
        return self._handle_response(response)

    def remove_role(self, role_id: str) -> None:
        """
        Deletes a role identified by its ID.

        :param role_id: The unique identifier of the role to remove.
        """
        if not self._uuid_validation(role_id):
            raise ValueError("role_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{role_id}")
        return self._handle_response(response)

    def change_role(
        self,
        role_id: str,
        name: str = None,
        description: str = None,
        permissions: List[str] = None,
        disabled: bool = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a role identified by its ID.

        :param role_id: The unique identifier of the role to update.
        :param name: The new name of the role (optional).
        :param description: The new description of the role (optional).
        :param permissions: A list of new permissions for the role (optional).
        :param disabled: Whether the role is disabled (optional).
        :return: A dictionary containing the updated details of the role.
        """
        if not self._uuid_validation(role_id):
            raise ValueError("role_id must be a valid UUID")

        data = {
            "name": name,
            "description": description,
            "permissions": permissions,
            "disabled": disabled
        }

        response = self._patch(url=f"{self.__base_api}/{role_id}", data=data)
        return self._handle_response(response)
