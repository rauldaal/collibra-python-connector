from .Base import BaseAPI


class License(BaseAPI):
    """API class for license operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/license"

    def has_capabilities(self, capabilities: list = None):
        """
        Checks the status of one or more license capabilities.
        :param capabilities: Optional list of capability names to check.
        :return: License capability status details.
        """
        params = {}
        if capabilities is not None:
            if not isinstance(capabilities, list):
                raise ValueError("capabilities must be a list")
            params["capabilities"] = capabilities

        response = self._get(url=f"{self.__base_api}/capabilities", params=params or None)
        return self._handle_response(response)
