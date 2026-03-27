import uuid
from .Base import BaseAPI


class Rating(BaseAPI):
    """API class for rating operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/ratings"

    def find_ratings(
        self,
        asset_id: str = None,
        count_limit: int = -1,
        limit: int = 0,
        offset: int = 0,
        sort_order: str = "DESC",
        user_id: str = None,
    ):
        """
        Returns ratings matching the given search criteria.
        :param asset_id: UUID of the asset to filter ratings by.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param sort_order: Sort order (ASC or DESC).
        :param user_id: UUID of the user to filter ratings by.
        :return: List of ratings.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if asset_id is not None:
            try:
                uuid.UUID(asset_id)
            except ValueError as exc:
                raise ValueError("asset_id must be a valid UUID") from exc

        if user_id is not None:
            try:
                uuid.UUID(user_id)
            except ValueError as exc:
                raise ValueError("user_id must be a valid UUID") from exc

        params = {}
        if asset_id is not None:
            params["assetId"] = asset_id
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if sort_order != "DESC":
            params["sortOrder"] = sort_order
        if user_id is not None:
            params["userId"] = user_id

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_rating(self, rating_id: str):
        """
        Returns the rating identified by the given UUID.
        :param rating_id: The UUID of the rating.
        :return: Rating details.
        """
        if not rating_id:
            raise ValueError("rating_id is required")
        try:
            uuid.UUID(rating_id)
        except ValueError as exc:
            raise ValueError("rating_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{rating_id}")
        return self._handle_response(response)

    def add_rating(self, asset_id: str, value: int):
        """
        Adds a new rating for an asset.
        :param asset_id: The UUID of the asset to rate (required).
        :param value: The rating value 1-5 (required).
        :return: Created rating details.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        if not isinstance(value, int) or value < 1 or value > 5:
            raise ValueError("value must be an integer between 1 and 5")

        data = {"assetId": asset_id, "value": value}
        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def change_rating(self, rating_id: str, value: int):
        """
        Changes the rating with the given ID.
        :param rating_id: The UUID of the rating to change.
        :param value: The new rating value 1-5.
        :return: Updated rating details.
        """
        if not rating_id:
            raise ValueError("rating_id is required")
        try:
            uuid.UUID(rating_id)
        except ValueError as exc:
            raise ValueError("rating_id must be a valid UUID") from exc

        if not isinstance(value, int) or value < 1 or value > 5:
            raise ValueError("value must be an integer between 1 and 5")

        data = {"value": value}
        response = self._patch(url=f"{self.__base_api}/{rating_id}", data=data)
        return self._handle_response(response)

    def remove_rating(self, rating_id: str):
        """
        Removes the rating identified by the given UUID.
        :param rating_id: The UUID of the rating.
        :return: None
        """
        if not rating_id:
            raise ValueError("rating_id is required")
        try:
            uuid.UUID(rating_id)
        except ValueError as exc:
            raise ValueError("rating_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{rating_id}")
        return self._handle_response(response)
