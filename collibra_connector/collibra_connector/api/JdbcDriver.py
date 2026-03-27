from .Base import BaseAPI


class JdbcDriver(BaseAPI):
    """API class for JDBC driver operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/jdbc"

    def find_jdbc_drivers(self):
        """
        Returns all available JDBC drivers.
        :return: List of JDBC drivers.
        """
        response = self._get(url=self.__base_api)
        return self._handle_response(response)
