from .Base import BaseAPI


class Auth(BaseAPI):
    """API class for authentication/session operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/auth/sessions"

    def login(self, username: str, password: str):
        """
        Creates a new session (logs in).
        :param username: The username.
        :param password: The password.
        :return: Session details.
        """
        if not username or not password:
            raise ValueError("username and password are required")

        data = {"username": username, "password": password}
        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_current_session(self, include: str = None):
        """
        Returns the current session information.
        :param include: Optional additional info to include.
        :return: Current session details.
        """
        params = {}
        if include is not None:
            params["include"] = include

        response = self._get(url=f"{self.__base_api}/current", params=params or None)
        return self._handle_response(response)

    def logout(self):
        """
        Terminates the current session (logs out).
        :return: None
        """
        response = self._delete(url=f"{self.__base_api}/current")
        return self._handle_response(response)

    def heartbeat(self):
        """
        Checks if the current user session is still active.
        :return: Session heartbeat response.
        """
        response = self._get(url=f"{self.__base_api}/heartbeat")
        return self._handle_response(response)
