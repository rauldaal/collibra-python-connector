from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class JdbcDrivers(BaseAPI):
    """API class for JDBC driver operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/jdbc"

    def find_jdbc_drivers(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        database_name: str = None,
        database_version: str = None
    ) -> Dict[str, Any]:
        """
        Searches for JDBC drivers based on the provided criteria.

        **Deprecated**: This operation will be removed in the future.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param database_name: The name of the database to filter by (optional).
        :param database_version: The version of the database to filter by (optional).
        :return: A dictionary containing the matching JDBC drivers.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "databaseName": database_name,
            "databaseVersion": database_version
        }
        
        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)
