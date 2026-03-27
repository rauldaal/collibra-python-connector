import uuid
from .Base import BaseAPI


class Status(BaseAPI):
    """API class for status operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/statuses"

    def find_statuses(
        self,
        count_limit: int = -1,
        description: str = None,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
    ):
        """
        Returns statuses matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param description: Description to filter by.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :return: List of statuses.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if description is not None:
            params["description"] = description
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_status(self, status_id: str):
        """
        Returns the Status identified by the given UUID.
        :param status_id: The UUID of the status.
        :return: Status details.
        """
        if not status_id:
            raise ValueError("status_id is required")
        try:
            uuid.UUID(status_id)
        except ValueError as exc:
            raise ValueError("status_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{status_id}")
        return self._handle_response(response)

    def get_status_by_name(self, status_name: str):
        """
        Returns the Status identified by the given name.
        :param status_name: The name of the status.
        :return: Status details.
        """
        if not status_name:
            raise ValueError("status_name is required")

        response = self._get(url=f"{self.__base_api}/name/{status_name}")
        return self._handle_response(response)

    def add_status(self, name: str, description: str = None):
        """
        Adds a new Status.
        :param name: The name of the status (required).
        :param description: Optional description.
        :return: Created status details.
        """
        if not name:
            raise ValueError("name is required")

        data = {"name": name}
        if description is not None:
            data["description"] = description

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_statuses(self, statuses: list):
        """
        Adds multiple statuses in one go.
        :param statuses: List of status objects.
        :return: Created statuses.
        """
        if not statuses or not isinstance(statuses, list):
            raise ValueError("statuses must be a non-empty list")
        response = self._post(url=f"{self.__base_api}/bulk", data={"statuses": statuses})
        return self._handle_response(response)

    def change_status(self, status_id: str, name: str = None, description: str = None):
        """
        Changes the status with the given ID.
        :param status_id: The UUID of the status to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :return: Updated status details.
        """
        if not status_id:
            raise ValueError("status_id is required")
        try:
            uuid.UUID(status_id)
        except ValueError as exc:
            raise ValueError("status_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{status_id}", data=data)
        return self._handle_response(response)

    def change_statuses(self, statuses: list):
        """
        Changes multiple statuses in one go.
        :param statuses: List of status change objects (must include id).
        :return: Updated statuses.
        """
        if not statuses or not isinstance(statuses, list):
            raise ValueError("statuses must be a non-empty list")
        response = self._patch(url=f"{self.__base_api}/bulk", data={"statuses": statuses})
        return self._handle_response(response)

    def remove_status(self, status_id: str):
        """
        Removes the Status identified by the given UUID.
        :param status_id: The UUID of the status.
        :return: None
        """
        if not status_id:
            raise ValueError("status_id is required")
        try:
            uuid.UUID(status_id)
        except ValueError as exc:
            raise ValueError("status_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{status_id}")
        return self._handle_response(response)

    def remove_statuses(self, status_ids: list):
        """
        Removes multiple statuses.
        :param status_ids: List of status UUIDs to remove.
        :return: None
        """
        if not status_ids or not isinstance(status_ids, list):
            raise ValueError("status_ids must be a non-empty list")
        response = self._delete(url=f"{self.__base_api}/bulk")
        return self._handle_response(response)
