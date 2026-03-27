import uuid
from .Base import BaseAPI


class Tag(BaseAPI):
    """API class for tag operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/tags"

    def find_tags(
        self,
        count_limit: int = -1,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
    ):
        """
        Returns tags matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :return: List of tags.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_tag(self, tag_id: str):
        """
        Returns the tag identified by the given UUID.
        :param tag_id: The UUID of the tag.
        :return: Tag details.
        """
        if not tag_id:
            raise ValueError("tag_id is required")
        try:
            uuid.UUID(tag_id)
        except ValueError as exc:
            raise ValueError("tag_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{tag_id}")
        return self._handle_response(response)

    def get_tags_by_asset_id(self, asset_id: str):
        """
        Returns all tags for the asset with the given ID.
        :param asset_id: The UUID of the asset.
        :return: List of tags for the asset.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/asset/{asset_id}")
        return self._handle_response(response)

    def tag_exists(self, tag_name: str, encoded: bool = None):
        """
        Checks if a tag with the given name exists.
        :param tag_name: The name of the tag to check.
        :param encoded: Whether the tag name is URL-encoded.
        :return: True if the tag exists, False otherwise.
        """
        if not tag_name:
            raise ValueError("tag_name is required")

        params = {}
        if encoded is not None:
            params["encoded"] = encoded

        response = self._get(url=f"{self.__base_api}/exists/{tag_name}",
                             params=params or None)
        return self._handle_response(response)

    def change_tag(self, tag_id: str, name: str):
        """
        Changes the tag with the given ID.
        :param tag_id: The UUID of the tag to change.
        :param name: The new name for the tag.
        :return: Updated tag details.
        """
        if not tag_id:
            raise ValueError("tag_id is required")
        try:
            uuid.UUID(tag_id)
        except ValueError as exc:
            raise ValueError("tag_id must be a valid UUID") from exc

        if not name:
            raise ValueError("name is required")

        data = {"name": name}
        response = self._patch(url=f"{self.__base_api}/{tag_id}", data=data)
        return self._handle_response(response)

    def remove_tag(self, tag_id: str):
        """
        Removes the tag identified by the given UUID.
        :param tag_id: The UUID of the tag.
        :return: None
        """
        if not tag_id:
            raise ValueError("tag_id is required")
        try:
            uuid.UUID(tag_id)
        except ValueError as exc:
            raise ValueError("tag_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{tag_id}")
        return self._handle_response(response)

    def remove_tags(self, tag_ids: list):
        """
        Removes multiple tags.
        :param tag_ids: List of tag UUIDs to remove.
        :return: None
        """
        if not tag_ids or not isinstance(tag_ids, list):
            raise ValueError("tag_ids must be a non-empty list")

        response = self._delete(url=f"{self.__base_api}/bulk")
        return self._handle_response(response)

    def merge_tags(self, source_tag_id: str, target_tag_id: str):
        """
        Merges one tag into another.
        :param source_tag_id: The UUID of the source tag (will be merged and removed).
        :param target_tag_id: The UUID of the target tag (will absorb the source).
        :return: Merged tag details.
        """
        if not source_tag_id:
            raise ValueError("source_tag_id is required")
        try:
            uuid.UUID(source_tag_id)
        except ValueError as exc:
            raise ValueError("source_tag_id must be a valid UUID") from exc

        if not target_tag_id:
            raise ValueError("target_tag_id is required")
        try:
            uuid.UUID(target_tag_id)
        except ValueError as exc:
            raise ValueError("target_tag_id must be a valid UUID") from exc

        data = {"sourceTagId": source_tag_id, "targetTagId": target_tag_id}
        response = self._post(url=f"{self.__base_api}/merge", data=data)
        return self._handle_response(response)
