import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Statuses(BaseAPI):
    """API class for status operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/statuses"

    def find_statuses(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        description: str = None
    ) -> Dict[str, Any]:
        """
        Searches for statuses based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param name: The name of the status to search for (optional).
        :param name_match_mode: The matching mode for the name (default: "ANYWHERE").
        :param description: The description of the status (optional).
        :return: A dictionary containing the matching statuses.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode,
            "description": description
        }
        
        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_status(self, name: str, id: str = None, description: str = None) -> Dict[str, Any]:
        """
        Creates a new status with the specified parameters.

        :param name: The name of the status (required).
        :param id: The unique identifier for the status (optional).
        :param description: A description of the status (optional).
        :return: A dictionary containing the details of the created status.
        """
        if not name:
            raise ValueError("name is required")

        data = {"name": name, "id": id, "description": description}

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_statuses(self, statuses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Creates multiple statuses in bulk.

        :param statuses: A list of dictionaries, each containing the details of a status to create.
        :return: A list of dictionaries containing the details of the created statuses.
        """
        if not statuses or not isinstance(statuses, list):
            raise ValueError("statuses must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=statuses)
        return self._handle_response(response)

    def change_statuses(self, statuses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Updates multiple statuses in bulk.

        :param statuses: A list of dictionaries, each containing the updated details of a status.
        :return: A list of dictionaries containing the updated statuses.
        """
        if not statuses or not isinstance(statuses, list):
            raise ValueError("statuses must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=statuses)
        return self._handle_response(response)

    def get_status(self, status_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a status by its ID.

        :param status_id: The unique identifier of the status to retrieve.
        :return: A dictionary containing the details of the status.
        """
        if not self._uuid_validation(status_id):
            raise ValueError("status_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{status_id}")
        return self._handle_response(response)

    def remove_status(self, status_id: str) -> None:
        """
        Deletes a status identified by its ID.

        :param status_id: The unique identifier of the status to remove.
        """
        if not self._uuid_validation(status_id):
            raise ValueError("status_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{status_id}")
        return self._handle_response(response)

    def change_status(
        self,
        status_id: str,
        name: str = None,
        description: str = None,
        id: str = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a status identified by its ID.

        :param status_id: The unique identifier of the status to update.
        :param name: The new name of the status (optional).
        :param description: The new description of the status (optional).
        :param id: The new unique identifier for the status (optional).
        :return: A dictionary containing the updated details of the status.
        """
        if not self._uuid_validation(status_id):
            raise ValueError("status_id must be a valid UUID")

        data = {"name": name, "description": description}
        if id is not None:
            data["id"] = id

        response = self._patch(url=f"{self.__base_api}/{status_id}", data=data)
        return self._handle_response(response)

    def get_status_by_name(self, status_name: str) -> Dict[str, Any]:
        """
        Retrieves the details of a status by its name.

        :param status_name: The name of the status to retrieve.
        :return: A dictionary containing the details of the status.
        """
        response = self._get(url=f"{self.__base_api}/name/{status_name}")
        return self._handle_response(response)

    def remove_statuses(self, status_ids: List[str]) -> None:
        """
        Deletes multiple statuses identified by their IDs.

        :param status_ids: A list of unique identifiers for the statuses to remove.
        """
        if not status_ids or not isinstance(status_ids, list):
            raise ValueError("status_ids must be a non-empty list")
        
        response = self._delete(url=f"{self.__base_api}/bulk", data=status_ids)
        return self._handle_response(response)
