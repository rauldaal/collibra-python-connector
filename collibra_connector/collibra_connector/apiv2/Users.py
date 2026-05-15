import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Users(BaseAPI):
    """API class for user operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/users"

    def find_users(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        sort_field: str = "USERNAME",
        sort_order: str = "ASC",
        user_ids: List[str] = None,
        name: str = None,
        name_search_fields: List[str] = None,
        group_id: str = None,
        include_disabled: bool = None,
        only_logged_in: bool = None,
        department: str = None,
        user_id: str = None,
        title: str = None
    ) -> Dict[str, Any]:
        """
        Returns users matching the given search criteria.

        :param offset: The starting offset for pagination.
        :param limit: The maximum number of results to return.
        :param count_limit: The maximum number of items to count.
        :param sort_field: The field to sort by.
        :param sort_order: The order to sort by ('ASC', 'DESC').
        :param user_ids: A list of user IDs.
        :param name: The name of the user to search for.
        :param name_search_fields: Fields to search for the name.
        :param group_id: The ID of the user group.
        :param include_disabled: Whether to include disabled users.
        :param only_logged_in: Whether to only include logged in users.
        :param department: The department of the user.
        :param user_id: Specific user ID to search for.
        :param title: The title of the user.
        :return: Response from the API.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "sortField": sort_field,
            "sortOrder": sort_order,
            "userId": user_ids,
            "name": name,
            "nameSearchFields": name_search_fields,
            "groupId": group_id,
            "includeDisabled": include_disabled,
            "onlyLoggedIn": only_logged_in,
            "department": department,
            "userId": user_id,
            "title": title
        }

        if group_id and not self._uuid_validation(group_id):
            raise ValueError("groupId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_user(
        self,
        user_name: str,
        email_address: str,
        first_name: str = None,
        last_name: str = None,
        gender: str = None,
        phone_number: str = None,
        mobile_number: str = None,
        id: str = None,
        is_disabled: bool = None,
        language: str = None,
        department: str = None,
        websites: List[str] = None,
        instant_messaging_accounts: List[str] = None,
        phones: List[str] = None,
        addresses: List[str] = None,
        license_type: str = None,
        additional_email_addresses: List[str] = None,
        user_group_ids: List[str] = None,
        title: str = None
    ) -> Dict[str, Any]:
        """
        Adds a new user.

        :param user_name: The username of the user.
        :param email_address: The email address of the user.
        :param first_name: The first name of the user.
        :param last_name: The last name of the user.
        :param gender: The gender of the user.
        :param phone_number: The phone number of the user.
        :param mobile_number: The mobile number of the user.
        :param id: The unique ID for the user.
        :param is_disabled: Whether the user is disabled.
        :param language: The preferred language of the user.
        :param department: The department of the user.
        :param websites: A list of websites associated with the user.
        :param instant_messaging_accounts: A list of IM accounts.
        :param phones: A list of phone numbers.
        :param addresses: A list of addresses.
        :param license_type: The license type of the user.
        :param additional_email_addresses: A list of additional email addresses.
        :param user_group_ids: A list of user group IDs.
        :param title: The title of the user.
        :return: Response from the API.
        """
        if not user_name or not email_address:
            raise ValueError("userName and emailAddress are required")

        data = {
            "userName": user_name,
            "emailAddress": email_address,
            "firstName": first_name,
            "lastName": last_name,
            "gender": gender,
            "phoneNumber": phone_number,
            "mobileNumber": mobile_number,
            "id": id,
            "isDisabled": is_disabled,
            "language": language,
            "department": department,
            "websites": websites,
            "instantMessagingAccounts": instant_messaging_accounts,
            "phones": phones,
            "addresses": addresses,
            "licenseType": license_type,
            "additionalEmailAddresses": additional_email_addresses,
            "userGroupIds": user_group_ids,
            "title": title
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Returns the user identified by given id.
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{user_id}")
        return self._handle_response(response)

    def remove_user(self, user_id: str) -> None:
        """
        Removes the user identified by the given id.
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{user_id}")
        return self._handle_response(response)

    def change_user(
        self,
        user_id: str,
        user_name: str = None,
        email_address: str = None,
        first_name: str = None,
        last_name: str = None,
        gender: str = None,
        phone_number: str = None,
        mobile_number: str = None,
        is_disabled: bool = None,
        language: str = None,
        department: str = None,
        websites: List[str] = None,
        instant_messaging_accounts: List[str] = None,
        phones: List[str] = None,
        addresses: List[str] = None,
        license_type: str = None,
        additional_email_addresses: List[str] = None,
        title: str = None,
        password_confirmation: str = None,
        enabled: bool = None
    ) -> Dict[str, Any]:
        """
        Changes the user with the information that is present in the request.
        :param user_id: The ID of the user to change.
        :param user_name: Username of the user.
        :param email_address: Email address of the user.
        :param first_name: First name of the user.
        :param last_name: Last name of the user.
        :param gender: Gender of the user.
        :param phone_number: Phone number of the user.
        :param mobile_number: Mobile number of the user.
        :param is_disabled: Whether the user is disabled.
        :param language: Preferred language of the user.
        :param department: Department of the user.
        :param websites: List of websites associated with the user.
        :param instant_messaging_accounts: List of IM accounts.
        :param phones: List of phone numbers.
        :param addresses: List of addresses.
        :param license_type: License type of the user.
        :param additional_email_addresses: Additional email addresses.
        :param title: Title of the user.
        :param password_confirmation: Password confirmation.
        :param enabled: Whether the user is enabled.
        :return: Response from the API.
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        data = {
            "userName": user_name,
            "emailAddress": email_address,
            "firstName": first_name,
            "lastName": last_name,
            "gender": gender,
            "phoneNumber": phone_number,
            "mobileNumber": mobile_number,
            "isDisabled": is_disabled,
            "language": language,
            "department": department,
            "websites": websites,
            "instantMessagingAccounts": instant_messaging_accounts,
            "phones": phones,
            "addresses": addresses,
            "licenseType": license_type,
            "additionalEmailAddresses": additional_email_addresses,
            "title": title,
            "passwordConfirmation": password_confirmation,
            "enabled": enabled
        }

        # Remove keys with None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._patch(url=f"{self.__base_api}/{user_id}", data=data)
        return self._handle_response(response)

    def get_current_user(self) -> Dict[str, Any]:
        """
        Returns the currently logged in user.
        """
        response = self._get(url=f"{self.__base_api}/current")
        return self._handle_response(response)

    def remove_users_in_job(self, user_ids: List[str]) -> Dict[str, Any]:
        """
        Removes multiple users in a job.
        """
        if not user_ids or not isinstance(user_ids, list):
            raise ValueError("user_ids must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/removalJobs", data=user_ids)
        return self._handle_response(response)

    def set_user_groups_for_user(self, user_id: str, user_group_ids: List[str]) -> None:
        """
        Sets the user groups for the specified user.
        :param user_id: The ID of the user.
        :param user_group_ids: List of user group IDs to set for the user.
        :return: None
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        data = {"userGroupIds": user_group_ids}
        response = self._put(url=f"{self.__base_api}/{user_id}/userGroups", data=data)
        return self._handle_response(response)

    def add_user_groups_for_user(self, user_id: str, user_group_ids: List[str]) -> None:
        """
        Adds user groups to the specified user.
        :param user_id: The ID of the user.
        :param user_group_ids: List of user group IDs to add to the user.
        :return: None
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        data = {"userGroupIds": user_group_ids}
        response = self._post(url=f"{self.__base_api}/{user_id}/userGroups", data=data)
        return self._handle_response(response)

    def remove_user_from_user_groups(self, user_id: str, user_group_ids: List[str]) -> None:
        """
        Removes the user from the specified user groups.
        :param user_id: The ID of the user.
        :param user_group_ids: List of user group IDs to remove the user from.
        :return: None
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        params = {"userGroupIds": user_group_ids}
        response = self._delete(url=f"{self.__base_api}/{user_id}/userGroups", params=params)
        return self._handle_response(response)

    def add_users(self, users: List[Dict[str, Any]]) -> None:
        """
        Adds multiple users in bulk.
        :param users: List of user data dictionaries.
        :return: None
        """
        response = self._post(url=f"{self.__base_api}/bulk", data=users)
        return self._handle_response(response)

    def delete_user(self, user_id: str) -> None:
        """
        Deletes the specified user.
        :param user_id: The ID of the user to delete.
        :return: None
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        response = self._delete(url=f"{self.__base_api}/{user_id}")
        return self._handle_response(response)

    def get_avatar_file(self, user_id: str, width: int = None, height: int = None, use_default: bool = False) -> Any:
        """
        Retrieves the avatar file for the specified user.
        :param user_id: The ID of the user.
        :param width: Width of the avatar.
        :param height: Height of the avatar.
        :param use_default: Whether to use the default avatar if none is set.
        :return: Avatar file content.
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        params = {
            "width": width,
            "height": height,
            "useDefault": use_default
        }
        response = self._get(url=f"{self.__base_api}/{user_id}/avatar", params=params)
        return self._handle_response(response)

    def change_user_avatar(self, user_id: str, avatar_data: Any, file_id: str = None) -> None:
        """
        Changes the avatar for the specified user.
        :param user_id: The ID of the user.
        :param avatar_data: The new avatar data.
        :param file_id: The ID of the file to use as the avatar.
        :return: None
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        data = {
            "avatarData": avatar_data,
            "fileId": file_id
        }
        response = self._patch(url=f"{self.__base_api}/{user_id}/avatar", data=data)
        return self._handle_response(response)

    def delete_user_avatar(self, user_id: str) -> None:
        """
        Deletes the avatar for the specified user.
        :param user_id: The ID of the user.
        :return: None
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        response = self._delete(url=f"{self.__base_api}/{user_id}/avatar")
        return self._handle_response(response)

    def get_current_user_global_permissions(self) -> Dict[str, Any]:
        """
        Retrieves the global permissions for the current user.
        :return: Global permissions as a dictionary.
        """
        response = self._get(url=f"{self.__base_api}/current/globalPermissions")
        return self._handle_response(response)

    def get_current_user_permissions(self) -> Dict[str, Any]:
        """
        Retrieves the permissions for the current user.
        :return: Permissions as a dictionary.
        """
        response = self._get(url=f"{self.__base_api}/current/permissions")
        return self._handle_response(response)

    def get_user_by_email_address(self, email_address: str) -> Dict[str, Any]:
        """
        Retrieves the user by their email address.
        :param email_address: The email address of the user.
        :return: User details as a dictionary.
        """
        response = self._get(url=f"{self.__base_api}/email/{email_address}")
        return self._handle_response(response)

    def get_user_effective_license_type(self, user_id: str) -> Dict[str, Any]:
        """
        Retrieves the effective license type for the specified user.
        :param user_id: The ID of the user.
        :return: Effective license type as a dictionary.
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{user_id}/effectiveLicenseType")
        return self._handle_response(response)

    def get_user_required_license_type(self, user_id: str) -> Dict[str, Any]:
        """
        Retrieves the required license type for the specified user.
        :param user_id: The ID of the user.
        :return: Required license type as a dictionary.
        """
        if not self._uuid_validation(user_id):
            raise ValueError("user_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{user_id}/licenseType")
        return self._handle_response(response)
