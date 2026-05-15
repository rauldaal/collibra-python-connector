import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Attachments(BaseAPI):
    """API class for attachment operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/attachments"

    def find_attachments(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        file_name: str = None,
        file_content_type: str = None,
        upload_date: int = None,
        user_id: str = None,
        base_resource_id: str = None,
        sort_field: str = "LAST_MODIFIED",
        sort_order: str = "DESC"
    ) -> Dict[str, Any]:
        """
        Searches for attachments based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param file_name: The name of the file to filter by (optional).
        :param file_content_type: The content type of the file to filter by (optional).
        :param upload_date: The upload date of the file to filter by (optional).
        :param user_id: The UUID of the user who uploaded the file (optional).
        :param base_resource_id: The UUID of the base resource associated with the attachment (optional).
        :param sort_field: The field to sort the results by (default: "LAST_MODIFIED").
        :param sort_order: The order to sort the results in (default: "DESC").
        :return: A dictionary containing the matching attachments.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "fileName": file_name,
            "fileContentType": file_content_type,
            "uploadDate": upload_date,
            "userId": user_id,
            "baseResourceId": base_resource_id,
            "sortField": sort_field,
            "sortOrder": sort_order
        }
        
        if user_id and not self._uuid_validation(user_id):
            raise ValueError("userId must be a valid UUID")
        if base_resource_id and not self._uuid_validation(base_resource_id):
            raise ValueError("baseResourceId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_attachment(
        self,
        file_path: str,
        file_name: str,
        resource_id: str = None,
        resource_type: str = None,
        resource_discriminator: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new attachment with the specified parameters.

        :param file_path: The path to the file to be uploaded (required).
        :param file_name: The name of the file to be uploaded (required).
        :param resource_id: The UUID of the resource associated with the attachment (optional).
        :param resource_type: The type of the resource associated with the attachment (optional).
        :param resource_discriminator: The discriminator for the resource associated with the attachment (optional).
        :return: A dictionary containing the details of the created attachment.
        """
        files = {
            "file": (file_name, open(file_path, "rb")),
            "fileName": (None, file_name)
        }
        data = {}
        if resource_id:
            data["resourceId"] = resource_id
        if resource_type:
            data["resourceType"] = resource_type
        if resource_discriminator:
            data["resourceDiscriminator"] = resource_discriminator

        if resource_id and not self._uuid_validation(resource_id):
            raise ValueError("resourceId must be a valid UUID")

        response = self._post(url=self.__base_api, files=files, data=data)
        return self._handle_response(response)

    def get_attachment(self, attachment_id: str) -> Dict[str, Any]:
        """
        Retrieves information about an attachment by its ID.

        :param attachment_id: The unique identifier of the attachment to retrieve.
        :return: A dictionary containing the details of the attachment.
        """
        if not self._uuid_validation(attachment_id):
            raise ValueError("attachment_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{attachment_id}")
        return self._handle_response(response)

    def remove_attachment(self, attachment_id: str) -> None:
        """
        Deletes an attachment identified by its ID.

        :param attachment_id: The unique identifier of the attachment to delete.
        """
        if not self._uuid_validation(attachment_id):
            raise ValueError("attachment_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{attachment_id}")
        return self._handle_response(response)

    def get_attachment_content(self, attachment_id: str) -> Any:
        """
        Retrieves the content of an attachment by its ID.

        :param attachment_id: The unique identifier of the attachment to retrieve content for.
        :return: The content of the attachment.
        """
        if not self._uuid_validation(attachment_id):
            raise ValueError("attachment_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{attachment_id}/file")
        return self._handle_response(response)
