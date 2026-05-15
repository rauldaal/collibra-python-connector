from .Base import BaseAPI


class NavigationStatistics(BaseAPI):
    """API class for navigation statistics operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/navigation"

    def find_most_viewed_assets(self, limit: int = None, offset: int = None, is_guest_excluded: bool = False, count_limit: int = 100, period: str = "daily"):
        """
        Returns the most viewed assets.
        :param limit: Maximum number of results to retrieve.
        :param offset: First result to retrieve.
        :param is_guest_excluded: Whether to exclude guest views.
        :param count_limit: Maximum count of assets to consider.
        :param period: Time period for the statistics (e.g., daily, weekly).
        :return: List of most viewed assets.
        """
        params = {
            "is_guest_excluded": is_guest_excluded,
            "count_limit": count_limit,
            "period": period
        }
        if limit is not None:
            if limit < 0 or limit > 1000:
                raise ValueError("limit must be between 0 and 1000")
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._get(url=f"{self.__base_api}/most_viewed", params=params or None)
        return self._handle_response(response)

    def find_recently_viewed_assets(self, limit: int = None, offset: int = None, count_limit: int = 100):
        """
        Returns the most recently viewed assets.
        :param limit: Maximum number of results to retrieve.
        :param offset: First result to retrieve.
        :param count_limit: Maximum count of assets to consider.
        :return: List of recently viewed assets.
        """
        params = {
            "count_limit": count_limit
        }
        if limit is not None:
            if limit < 0 or limit > 1000:
                raise ValueError("limit must be between 0 and 1000")
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._get(url=f"{self.__base_api}/recently_viewed", params=params or None)
        return self._handle_response(response)
