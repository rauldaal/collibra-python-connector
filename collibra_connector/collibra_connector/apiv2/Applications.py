from typing import Any, Dict
from .Base import BaseAPI


class Applications(BaseAPI):
    """API class for application info operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/application"

    def get_info(self) -> Dict[str, Any]:
        """
        Returns the basic information about the Collibra application.
        """
        response = self._get(url=f"{self.__base_api}/info")
        return self._handle_response(response)
