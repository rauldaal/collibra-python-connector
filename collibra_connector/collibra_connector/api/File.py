from .Base import BaseAPI


class File(BaseAPI):
    """API class for file operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/files"

    def upload_file(self, file_content: bytes, file_name: str, content_type: str = "application/octet-stream"):
        """
        Uploads a file to Collibra.
        :param file_content: The binary content of the file.
        :param file_name: The name of the file.
        :param content_type: The MIME type of the file. Default: application/octet-stream.
        :return: Created file details including the file ID.
        """
        if not file_content:
            raise ValueError("file_content is required")
        if not file_name:
            raise ValueError("file_name is required")

        import requests
        connector = self._BaseAPI__connector
        headers = {"Accept": "application/json"}
        files = {"file": (file_name, file_content, content_type)}

        response = requests.post(
            self.__base_api,
            auth=connector.auth,
            files=files,
            headers=headers,
            timeout=connector.timeout
        )
        return self._handle_response(response)

    def get_file(self, file_id: str):
        """
        Downloads the file identified by the given ID.
        :param file_id: The UUID of the file.
        :return: File content.
        """
        if not file_id:
            raise ValueError("file_id is required")

        response = self._get(url=f"{self.__base_api}/{file_id}")
        return self._handle_response(response)

    def get_file_info(self, file_id: str):
        """
        Returns information about the file identified by the given ID.
        :param file_id: The UUID of the file.
        :return: File information details.
        """
        if not file_id:
            raise ValueError("file_id is required")

        response = self._get(url=f"{self.__base_api}/{file_id}/info")
        return self._handle_response(response)

    def delete_file(self, file_id: str):
        """
        Deletes the temporary file with the given ID.
        :param file_id: The UUID of the file.
        :return: None
        """
        if not file_id:
            raise ValueError("file_id is required")

        response = self._delete(url=f"{self.__base_api}/{file_id}")
        return self._handle_response(response)

    def delete_files(self, time_to_live: int = None):
        """
        Deletes files that are older than the given time to live.
        :param time_to_live: Time to live in milliseconds. Files older than this will be deleted.
        :return: None
        """
        params = {}
        if time_to_live is not None:
            if not isinstance(time_to_live, int) or time_to_live < 0:
                raise ValueError("time_to_live must be a non-negative integer")
            params["timeToLive"] = time_to_live

        response = self._delete(url=f"{self.__base_api}/bulk")
        return self._handle_response(response)
