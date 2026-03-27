from .Base import BaseAPI


class NavigationStatistics(BaseAPI):
    """API class for navigation statistics operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/navigation"

    def find_most_viewed_assets(self, limit: int = None, offset: int = None):
        """
        Returns the most viewed assets.
        :param limit: Maximum number of results to retrieve.
        :param offset: First result to retrieve.
        :return: List of most viewed assets.
        """
        params = {}
        if limit is not None:
            if limit < 0 or limit > 1000:
                raise ValueError("limit must be between 0 and 1000")
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._get(url=f"{self.__base_api}/most_viewed", params=params or None)
        return self._handle_response(response)

    def find_recently_viewed_assets(self, limit: int = None, offset: int = None):
        """
        Returns the most recently viewed assets.
        :param limit: Maximum number of results to retrieve.
        :param offset: First result to retrieve.
        :return: List of recently viewed assets.
        """
        params = {}
        if limit is not None:
            if limit < 0 or limit > 1000:
                raise ValueError("limit must be between 0 and 1000")
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._get(url=f"{self.__base_api}/recently_viewed", params=params or None)
        return self._handle_response(response)
