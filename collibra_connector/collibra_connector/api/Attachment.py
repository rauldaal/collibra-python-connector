import uuid
from .Base import BaseAPI


class Attachment(BaseAPI):
    """API class for attachment operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/attachments"

    def find_attachments(
        self,
        base_resource_id: str = None,
        count_limit: int = -1,
        file_content_type: str = None,
        file_name: str = None,
        limit: int = 0,
        offset: int = 0,
        sort_field: str = "LAST_MODIFIED",
        sort_order: str = "DESC",
        upload_date: int = None,
        user_id: str = None,
    ):
        """
        Returns attachments matching the given search criteria.
        :param base_resource_id: The UUID of the resource which the attachments belong to.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param file_content_type: MIME type of the attachment file.
        :param file_name: Name of the attachment file.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :param upload_date: Unix timestamp in milliseconds of the upload date.
        :param user_id: UUID of the user who uploaded the attachment.
        :return: List of attachments.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if base_resource_id is not None:
            try:
                uuid.UUID(base_resource_id)
            except ValueError as exc:
                raise ValueError("base_resource_id must be a valid UUID") from exc

        if user_id is not None:
            try:
                uuid.UUID(user_id)
            except ValueError as exc:
                raise ValueError("user_id must be a valid UUID") from exc

        params = {}
        if base_resource_id is not None:
            params["baseResourceId"] = base_resource_id
        if count_limit != -1:
            params["countLimit"] = count_limit
        if file_content_type is not None:
            params["fileContentType"] = file_content_type
        if file_name is not None:
            params["fileName"] = file_name
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if sort_field != "LAST_MODIFIED":
            params["sortField"] = sort_field
        if sort_order != "DESC":
            params["sortOrder"] = sort_order
        if upload_date is not None:
            params["uploadDate"] = upload_date
        if user_id is not None:
            params["userId"] = user_id

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_attachment(self, attachment_id: str):
        """
        Returns the attachment identified by the given UUID.
        :param attachment_id: The UUID of the attachment.
        :return: Attachment details.
        """
        if not attachment_id:
            raise ValueError("attachment_id is required")
        try:
            uuid.UUID(attachment_id)
        except ValueError as exc:
            raise ValueError("attachment_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{attachment_id}")
        return self._handle_response(response)

    def get_attachment_content(self, attachment_id: str):
        """
        Returns the file content of the attachment identified by the given UUID.
        :param attachment_id: The UUID of the attachment.
        :return: Attachment file content.
        """
        if not attachment_id:
            raise ValueError("attachment_id is required")
        try:
            uuid.UUID(attachment_id)
        except ValueError as exc:
            raise ValueError("attachment_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{attachment_id}/file")
        return self._handle_response(response)

    def add_attachment(self, resource_id: str, resource_type: str, file_id: str,
                       file_name: str = None):
        """
        Adds a new attachment to a resource.
        :param resource_id: The UUID of the resource to attach to.
        :param resource_type: The type of the resource (e.g., Asset, Community, Domain).
        :param file_id: The UUID of the file to attach (uploaded via Files API).
        :param file_name: Optional display name for the attachment.
        :return: Created attachment details.
        """
        if not resource_id or not resource_type or not file_id:
            raise ValueError("resource_id, resource_type, and file_id are required")

        try:
            uuid.UUID(resource_id)
        except ValueError as exc:
            raise ValueError("resource_id must be a valid UUID") from exc

        try:
            uuid.UUID(file_id)
        except ValueError as exc:
            raise ValueError("file_id must be a valid UUID") from exc

        data = {
            "resourceId": resource_id,
            "resourceType": resource_type,
            "fileId": file_id,
        }
        if file_name is not None:
            data["fileName"] = file_name

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def remove_attachment(self, attachment_id: str):
        """
        Removes the attachment identified by the given UUID.
        :param attachment_id: The UUID of the attachment.
        :return: None
        """
        if not attachment_id:
            raise ValueError("attachment_id is required")
        try:
            uuid.UUID(attachment_id)
        except ValueError as exc:
            raise ValueError("attachment_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{attachment_id}")
        return self._handle_response(response)
