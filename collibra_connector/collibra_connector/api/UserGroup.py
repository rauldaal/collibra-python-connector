import uuid
from .Base import BaseAPI


class UserGroup(BaseAPI):
    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/userGroups"

    def find_user_groups(
        self,
        count_limit: int = -1,
        include_everyone: bool = None,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
        user_id: str = None
    ):
        """
        Find user groups matching the given search criteria.

        Returns user groups matching the given search criteria. Only parameters that are specified
        in this request and have not null values are used for filtering. All other parameters are
        ignored. By default a result containing 1000 user groups is returned.

        :param count_limit: Limit the number of elements that will be counted. -1 counts everything,
                            0 skips count. Default: -1
        :param include_everyone: Indicates if we should include the everyone group or not.
        :param limit: Maximum number of results to retrieve (0 = default limit, max 1000). Default: 0
        :param name: The name of the user group.
        :param name_match_mode: The match mode used to compare name. If the match mode is EXACT
                                the search is case-sensitive, otherwise case-insensitive.
                                Allowed values: START, END, ANYWHERE, EXACT. Default: ANYWHERE
        :param offset: First result to retrieve (0-based). Default: 0
        :param user_id: The ID of the user who should belong to searched user groups.
        :return: Search results with user groups matching the criteria.
        """
        # Validate count_limit
        if not isinstance(count_limit, int):
            raise ValueError("count_limit must be an integer")

        # Validate include_everyone
        if include_everyone is not None and not isinstance(include_everyone, bool):
            raise ValueError("include_everyone must be a boolean")

        # Validate limit
        if not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        if limit > 1000:
            raise ValueError("limit cannot exceed 1000")

        # Validate name
        if name is not None and not isinstance(name, str):
            raise ValueError("name must be a string")

        # Validate name_match_mode
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"Invalid name_match_mode: {name_match_mode}. "
                             f"Allowed values: {valid_match_modes}")

        # Validate offset
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")

        # Validate user_id if provided
        if user_id is not None:
            if not isinstance(user_id, str):
                raise ValueError("user_id must be a string")
            try:
                uuid.UUID(user_id)
            except ValueError as exc:
                raise ValueError("user_id must be a valid UUID") from exc

        # Build parameters
        params = {
            "countLimit": count_limit,
            "limit": limit,
            "nameMatchMode": name_match_mode,
            "offset": offset
        }

        # Add optional parameters
        if include_everyone is not None:
            params["includeEveryone"] = include_everyone

        if name is not None:
            params["name"] = name

        if user_id is not None:
            params["userId"] = user_id

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)
