import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Communities(BaseAPI):
    """API class for community operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/communities"

    def find_communities(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        cursor: str = None,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        parent_id: str = None,
        exclude_meta: bool = True,
        sort_field: str = "NAME",
        sort_order: str = "ASC"
    ) -> Dict[str, Any]:
        """
        Searches for communities based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param cursor: The cursor for paginated results (optional).
        :param name: The name of the community to filter by (optional).
        :param name_match_mode: The mode for matching the name (default: "ANYWHERE").
        :param parent_id: The UUID of the parent community to filter by (optional).
        :param exclude_meta: Whether to exclude metadata from the results (default: True).
        :param sort_field: The field to sort the results by (default: "NAME").
        :param sort_order: The order to sort the results in (default: "ASC").
        :return: A dictionary containing the matching communities.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "cursor": cursor,
            "name": name,
            "nameMatchMode": name_match_mode,
            "parentId": parent_id,
            "excludeMeta": exclude_meta,
            "sortField": sort_field,
            "sortOrder": sort_order
        }
        
        if parent_id and not self._uuid_validation(parent_id):
            raise ValueError("parentId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_community(
        self,
        name: str,
        parent_id: str = None,
        description: str = None,
        id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new community with the specified parameters.

        :param name: The name of the community (required).
        :param parent_id: The UUID of the parent community (optional).
        :param description: A description of the community (optional).
        :param id: The unique identifier of the community (optional).
        :return: A dictionary containing the details of the created community.
        """
        if not name:
            raise ValueError("name is required")

        data = {
            "name": name,
            "parentId": parent_id,
            "description": description,
            "id": id
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")
        if parent_id and not self._uuid_validation(parent_id):
            raise ValueError("parentId must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_communities(self, communities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adds multiple communities in bulk.

        :param communities: A list of dictionaries, each representing a community to add.
        :return: A list of dictionaries containing the details of the added communities.
        """
        if not communities or not isinstance(communities, list):
            raise ValueError("communities must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=communities)
        return self._handle_response(response)

    def change_communities(self, communities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Updates multiple communities in bulk.

        :param communities: A list of dictionaries, each representing a community to update.
        :return: A list of dictionaries containing the details of the updated communities.
        """
        if not communities or not isinstance(communities, list):
            raise ValueError("communities must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=communities)
        return self._handle_response(response)

    def get_community(self, community_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a community by its ID.

        :param community_id: The unique identifier of the community to retrieve.
        :return: A dictionary containing the details of the community.
        """
        if not self._uuid_validation(community_id):
            raise ValueError("community_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{community_id}")
        return self._handle_response(response)

    def remove_community(self, community_id: str) -> None:
        """
        Deletes a community identified by its ID.

        :param community_id: The unique identifier of the community to delete.
        """
        if not self._uuid_validation(community_id):
            raise ValueError("community_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{community_id}")
        return self._handle_response(response)

    def change_community(
        self,
        community_id: str,
        name: str = None,
        description: str = None,
        parent_id: str = None,
        remove_scope_overlap_on_move: bool = None,
        id: str = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a community identified by its ID.

        :param community_id: The unique identifier of the community to update.
        :param name: The new name for the community (optional).
        :param description: The new description for the community (optional).
        :param parent_id: The UUID of the new parent community (optional).
        :param remove_scope_overlap_on_move: Whether to remove scope overlap when moving the community (optional).
        :param id: The new unique identifier for the community (optional).
        :return: A dictionary containing the updated details of the community.
        """
        if not self._uuid_validation(community_id):
            raise ValueError("community_id must be a valid UUID")
        
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if parent_id is not None:
            data["parentId"] = parent_id
        if remove_scope_overlap_on_move is not None:
            data["removeScopeOverlapOnMove"] = remove_scope_overlap_on_move
        if id is not None:
            data["id"] = id

        response = self._patch(url=f"{self.__base_api}/{community_id}", data=data)
        return self._handle_response(response)

    def change_to_root_community(self, community_id: str) -> Dict[str, Any]:
        """
        Converts a community to a root community.

        :param community_id: The unique identifier of the community to convert.
        :return: A dictionary containing the details of the updated community.
        """
        if not self._uuid_validation(community_id):
            raise ValueError("community_id must be a valid UUID")
        
        response = self._post(url=f"{self.__base_api}/{community_id}/root", data={})
        return self._handle_response(response)

    def get_community_breadcrumb(self, community_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves the breadcrumb trail for a community identified by its ID.

        :param community_id: The unique identifier of the community to retrieve the breadcrumb for.
        :return: A list of dictionaries representing the breadcrumb trail.
        """
        response = self._get(url=f"{self.__base_api}/{community_id}/breadcrumb")
        return self._handle_response(response)

    def remove_communities_in_job(self, community_ids: List[str]) -> Dict[str, Any]:
        """
        Removes multiple communities in a single job.

        :param community_ids: A list of unique identifiers for the communities to remove.
        :return: A dictionary containing the details of the removal job.
        """
        if not community_ids or not isinstance(community_ids, list):
            raise ValueError("community_ids must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/removalJobs", data=community_ids)
        return self._handle_response(response)

    def remove_communities(self, community_ids: List[str]) -> None:
        """
        Deletes multiple communities identified by their IDs.

        :param community_ids: A list of unique identifiers for the communities to delete.
        """
        if not community_ids or not isinstance(community_ids, list):
            raise ValueError("community_ids must be a non-empty list")

        for community_id in community_ids:
            if not self._uuid_validation(community_id):
                raise ValueError(f"community_id {community_id} must be a valid UUID")

        response = self._delete(url=f"{self.__base_api}/bulk", data=community_ids)
        return self._handle_response(response)
