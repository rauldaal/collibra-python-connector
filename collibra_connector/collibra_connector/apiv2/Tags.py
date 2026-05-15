import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Tags(BaseAPI):
    """API class for tag operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/tags"

    def find_tags(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE"
    ) -> Dict[str, Any]:
        """
        Returns tags matching the given search criteria.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode
        }
        
        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_tag(self, name: str, id: str = None) -> Dict[str, Any]:
        """
        Adds a new tag.
        """
        if not name:
            raise ValueError("name is required")

        data = {"name": name, "id": id}

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_tag(self, tag_id: str) -> Dict[str, Any]:
        """
        Returns the tag identified by the given UUID.
        """
        if not self._uuid_validation(tag_id):
            raise ValueError("tag_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{tag_id}")
        return self._handle_response(response)

    def remove_tag(self, tag_id: str) -> None:
        """
        Removes the tag identified by the given UUID.
        """
        if not self._uuid_validation(tag_id):
            raise ValueError("tag_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{tag_id}")
        return self._handle_response(response)

    def change_tag(self, tag_id: str, name: str) -> Dict[str, Any]:
        """
        Changes the tag with the given ID.
        """
        if not self._uuid_validation(tag_id):
            raise ValueError("tag_id must be a valid UUID")
        
        data = {"name": name}
        response = self._patch(url=f"{self.__base_api}/{tag_id}", data=data)
        return self._handle_response(response)

    def merge_tags(self, from_id: str, to_id: str) -> Dict[str, Any]:
        """
        Merges two tags into one.
        :param from_id: The ID of the source tag to merge from.
        :param to_id: The ID of the target tag to merge into.
        :return: Details of the merged tag.
        """
        if not from_id:
            raise ValueError("from_id is required")
        if not to_id:
            raise ValueError("to_id is required")

        data = {
            "sourceTagId": from_id,
            "targetTagId": to_id
        }

        response = self._post(url=f"{self.__base_api}/tags/merge", data=data)
        return self._handle_response(response)

    def exists(self, tag_name: str, encoded: bool = False) -> bool:
        """
        Checks if a tag exists by its name.
        :param tag_name: The name of the tag to check.
        :param encoded: Whether the tag name is URL-encoded.
        :return: True if the tag exists, False otherwise.
        """
        if not tag_name:
            raise ValueError("tag_name is required")

        url = f"{self.__base_api}/tags/exists/{tag_name}"
        if encoded:
            url += "?encoded=true"

        response = self._get(url=url)
        return response.get("exists", False)

    def get_tags_by_asset_id(self, asset_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves tags associated with a specific asset ID.
        :param asset_id: The ID of the asset.
        :return: A list of tags associated with the asset.
        """
        if not asset_id:
            raise ValueError("asset_id is required")

        response = self._get(url=f"{self.__base_api}/tags/asset/{asset_id}")
        return self._handle_response(response)

    def remove_tags(self, tag_ids: List[str]) -> None:
        """
        Removes multiple tags identified by their IDs in bulk.
        :param tag_ids: A list of tag IDs to remove.
        :return: None.
        """
        if not tag_ids or not isinstance(tag_ids, list):
            raise ValueError("tag_ids must be a non-empty list")

        for tag_id in tag_ids:
            if not isinstance(tag_id, str):
                raise ValueError(f"tag_id {tag_id} must be a string")

        response = self._delete(url=f"{self.__base_api}/tags/bulk", data=tag_ids)
        return self._handle_response(response)
