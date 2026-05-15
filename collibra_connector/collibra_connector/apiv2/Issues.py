import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Issues(BaseAPI):
    """API class for issue operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/issues"

    def find_issues(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        sort_order: str = "ASC",
        sort_field: str = "NAME",
        only_open_issues: bool = True,
        user_relation: str = "ALL"
    ) -> Dict[str, Any]:
        """
        Searches for issues based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param sort_order: The order to sort the results in (default: "ASC").
        :param sort_field: The field to sort the results by (default: "NAME").
        :param only_open_issues: Whether to include only open issues (default: True).
        :param user_relation: The user relation to the issues (default: "ALL").
        :return: A dictionary containing the matching issues.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "sortOrder": sort_order,
            "sortField": sort_field,
            "onlyOpenIssues": only_open_issues,
            "userRelation": user_relation
        }
        
        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_issue(
        self,
        name: str,
        community_id: str,
        type_id: str = None,
        description: str = None,
        id: str = None,
        responsible_ids: List[str] = None,
        related_asset_ids: List[str] = None,
        priority: str = None,
        category_ids: List[str] = None,
        related_assets: List[Dict[str, Any]] = None,
        responsible_community_id: str = None,
        requester_id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new issue with the specified parameters.

        :param name: The name of the issue (required).
        :param community_id: The ID of the community associated with the issue (required).
        :param type_id: The type ID of the issue (optional).
        :param description: A description of the issue (optional).
        :param id: The unique identifier of the issue (optional).
        :param responsible_ids: A list of IDs of responsible users (optional).
        :param related_asset_ids: A list of IDs of related assets (optional).
        :param priority: The priority of the issue (optional).
        :param category_ids: A list of category IDs associated with the issue (optional).
        :param related_assets: A list of related assets with details (optional).
        :param responsible_community_id: The ID of the responsible community (optional).
        :param requester_id: The ID of the requester (optional).
        :return: A dictionary containing the details of the created issue.
        """
        if not name or not community_id:
            raise ValueError("name and community_id are required")

        data = {
            "name": name,
            "communityId": community_id,
            "typeId": type_id,
            "description": description,
            "id": id,
            "responsibleIds": responsible_ids,
            "relatedAssetIds": related_asset_ids,
            "priority": priority,
            "categoryIds": category_ids,
            "relatedAssets": related_assets,
            "responsibleCommunityId": responsible_community_id,
            "requesterId": requester_id
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")
        if community_id and not self._uuid_validation(community_id):
            raise ValueError("community_id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_issue(self, issue_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of an issue by its ID.

        :param issue_id: The unique identifier of the issue to retrieve.
        :return: A dictionary containing the details of the issue.
        """
        if not self._uuid_validation(issue_id):
            raise ValueError("issue_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{issue_id}")
        return self._handle_response(response)

    def remove_issue(self, issue_id: str) -> None:
        """
        Deletes an issue identified by its ID.

        :param issue_id: The unique identifier of the issue to remove.
        """
        if not self._uuid_validation(issue_id):
            raise ValueError("issue_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{issue_id}")
        return self._handle_response(response)

    def change_issue(self, issue_id: str, **kwargs) -> Dict[str, Any]:
        """
        Updates the details of an issue identified by its ID.

        :param issue_id: The unique identifier of the issue to update.
        :param kwargs: A dictionary of fields to update and their new values.
        :return: A dictionary containing the updated details of the issue.
        """
        if not self._uuid_validation(issue_id):
            raise ValueError("issue_id must be a valid UUID")
        
        mapping = {
            "name": "name",
            "community_id": "communityId",
            "type_id": "typeId",
            "description": "description",
            "priority": "priority"
        }
        
        data = {}
        for k, v in kwargs.items():
            if k in mapping:
                data[mapping[k]] = v
            else:
                data[k] = v

        response = self._patch(url=f"{self.__base_api}/{issue_id}", data=data)
        return self._handle_response(response)

    def move_issue(self, issue_id: str, community_id: str) -> Dict[str, Any]:
        """
        Moves an issue to a different community.

        :param issue_id: The unique identifier of the issue to move.
        :param community_id: The unique identifier of the target community.
        :return: A dictionary containing the details of the moved issue.
        """
        if not self._uuid_validation(issue_id) or not self._uuid_validation(community_id):
            raise ValueError("issue_id and community_id must be valid UUIDs")
        
        response = self._patch(url=f"{self.__base_api}/{issue_id}/community/{community_id}", data={})
        return self._handle_response(response)

    def remove_issues_in_job(self, issue_ids: List[str]) -> Dict[str, Any]:
        """
        Deletes multiple issues in a single job.

        :param issue_ids: A list of unique identifiers for the issues to remove.
        :return: A dictionary containing the details of the removal job.
        """
        if not issue_ids or not isinstance(issue_ids, list):
            raise ValueError("issue_ids must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/removalJobs", data=issue_ids)
        return self._handle_response(response)
