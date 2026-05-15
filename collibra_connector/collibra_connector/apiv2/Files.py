import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Files(BaseAPI):
    """API class for file operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/files"

    def add_files(self, file_path: str, file_name: str, content_type: str = "application/octet-stream") -> List[Dict[str, Any]]:
        """
        Uploads one or more files to Collibra.

        :param file_path: The path to the file to upload.
        :param file_name: The name of the file to upload.
        :param content_type: The MIME type of the file (default: "application/octet-stream").
        :return: A list of dictionaries containing details of the uploaded files.
        """
        files = {
            "file": (file_name, open(file_path, "rb"), content_type)
        }
        
        response = self._post(url=self.__base_api, files=files)
        return self._handle_response(response)

    def get_file(self, file_id: str) -> Any:
        """
        Downloads the file identified by the given ID.

        :param file_id: The unique identifier of the file to download.
        :return: The content of the downloaded file.
        """
        if not self._uuid_validation(file_id):
            raise ValueError("file_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{file_id}")
        return self._handle_response(response)

    def get_file_info(self, file_id: str) -> Dict[str, Any]:
        """
        Retrieves metadata information about a file by its ID.

        :param file_id: The unique identifier of the file.
        :return: A dictionary containing metadata about the file.
        """
        if not self._uuid_validation(file_id):
            raise ValueError("file_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{file_id}/info")
        return self._handle_response(response)

    def delete_file(self, file_id: str) -> None:
        """
        Deletes a temporary file identified by its ID.

        :param file_id: The unique identifier of the file to delete.
        """
        if not self._uuid_validation(file_id):
            raise ValueError("file_id must be a valid UUID")

        response = self._delete(url=f"{self.__base_api}/{file_id}")
        return self._handle_response(response)

    def delete_files(self, time_to_live: int = None) -> None:
        """
        Deletes files that are older than the specified time to live.

        :param time_to_live: The age threshold (in seconds) for deleting files. Files older than this value will be deleted.
                             If not provided, all files will be deleted.
        """
        params = {}
        if time_to_live is not None:
            if not isinstance(time_to_live, int) or time_to_live < 0:
                raise ValueError("time_to_live must be a non-negative integer")
            params["timeToLive"] = time_to_live

        response = self._delete(url=f"{self.__base_api}/bulk", params=params)
        return self._handle_response(response)
