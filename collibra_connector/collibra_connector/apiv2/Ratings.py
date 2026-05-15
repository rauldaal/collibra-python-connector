import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Ratings(BaseAPI):
    """API class for rating operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/ratings"

    def find_ratings(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        sort_order: str = "DESC",
        asset_id: str = None,
        user_id: str = None
    ) -> Dict[str, Any]:
        """
        Searches for ratings based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param sort_order: The order to sort the results in (default: "DESC").
        :param asset_id: The ID of the asset to filter by (optional).
        :param user_id: The ID of the user to filter by (optional).
        :return: A dictionary containing the matching ratings.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "sortOrder": sort_order,
            "assetId": asset_id,
            "userId": user_id
        }
        
        if asset_id and not self._uuid_validation(asset_id):
            raise ValueError("assetId must be a valid UUID")
        if user_id and not self._uuid_validation(user_id):
            raise ValueError("userId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_rating(self, asset_id: str, value: int, rating: int = None, review: str = None) -> Dict[str, Any]:
        """
        Adds a new rating for an asset.

        :param asset_id: The unique identifier of the asset to rate.
        :param value: The rating value (must be an integer between 1 and 5).
        :param rating: An optional additional rating value.
        :param review: An optional review text for the rating.
        :return: A dictionary containing the details of the added rating.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")

        if not isinstance(value, int) or value < 1 or value > 5:
            raise ValueError("value must be an integer between 1 and 5")

        data = {"assetId": asset_id, "value": value, "rating": rating, "review": review}
        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_rating(self, rating_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a rating by its ID.

        :param rating_id: The unique identifier of the rating to retrieve.
        :return: A dictionary containing the details of the rating.
        """
        if not self._uuid_validation(rating_id):
            raise ValueError("rating_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{rating_id}")
        return self._handle_response(response)

    def remove_rating(self, rating_id: str) -> None:
        """
        Deletes a rating identified by its ID.

        :param rating_id: The unique identifier of the rating to remove.
        """
        if not self._uuid_validation(rating_id):
            raise ValueError("rating_id must be a valid UUID")

        response = self._delete(url=f"{self.__base_api}/{rating_id}")
        return self._handle_response(response)

    def change_rating(self, rating_id: str, value: int, rating: int = None, review: str = None) -> Dict[str, Any]:
        """
        Updates the details of a rating identified by its ID.

        :param rating_id: The unique identifier of the rating to update.
        :param value: The new rating value (must be an integer between 1 and 5).
        :param rating: An optional updated additional rating value.
        :param review: An optional updated review text for the rating.
        :return: A dictionary containing the updated details of the rating.
        """
        if not self._uuid_validation(rating_id):
            raise ValueError("rating_id must be a valid UUID")

        if not isinstance(value, int) or value < 1 or value > 5:
            raise ValueError("value must be an integer between 1 and 5")

        data = {"value": value, "rating": rating, "review": review}
        response = self._patch(url=f"{self.__base_api}/{rating_id}", data=data)
        return self._handle_response(response)
