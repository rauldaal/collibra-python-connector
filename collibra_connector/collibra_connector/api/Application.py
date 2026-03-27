from .Base import BaseAPI


class Application(BaseAPI):
    """API class for application info operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/application"

    def get_info(self):
        """
        Returns the basic information about the Collibra application.
        :return: Application info details.
        """
        response = self._get(url=f"{self.__base_api}/info")
        return self._handle_response(response)
