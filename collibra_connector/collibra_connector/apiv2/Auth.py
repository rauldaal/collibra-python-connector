from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Auth(BaseAPI):
    """API class for authentication/session operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/auth/sessions"

    def login(self, username: str, password: str) -> Dict[str, Any]:
        """
        Authenticates a user and creates a new session.

        :param username: The username of the user (required).
        :param password: The password of the user (required).
        :return: A dictionary containing the session details.
        """
        if not username or not password:
            raise ValueError("username and password are required")

        data = {"username": username, "password": password}
        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_current_session(self, include: List[str] = None) -> Dict[str, Any]:
        """
        Retrieves information about the current session.

        :param include: A list of additional fields to include in the response (optional).
        :return: A dictionary containing the current session details.
        """
        params = {"include": include} if include else None

        response = self._get(url=f"{self.__base_api}/current", params=params)
        return self._handle_response(response)

    def logout(self) -> None:
        """
        Logs out the current user and destroys the active session.

        :return: None
        """
        response = self._delete(url=f"{self.__base_api}/current")
        return self._handle_response(response)

    def heartbeat(self) -> None:
        """
        Checks if the user session is active.

        :return: None
        """
        response = self._get(url=f"{self.__base_api}/heartbeat")
        return self._handle_response(response)
