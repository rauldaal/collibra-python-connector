import uuid
from .Base import BaseAPI


class Issue(BaseAPI):
    """API class for issue operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/issues"

    def find_issues(
        self,
        count_limit: int = -1,
        limit: int = 0,
        offset: int = 0,
        only_open_issues: bool = None,
        sort_field: str = "LAST_MODIFIED",
        sort_order: str = "DESC",
        user_relation: str = None,
    ):
        """
        Returns issues matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param only_open_issues: Whether to return only open issues.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :param user_relation: Filter by the current user's relation to the issue
                              (e.g., REPORTER, ASSIGNEE).
        :return: List of issues.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if only_open_issues is not None:
            params["onlyOpenIssues"] = only_open_issues
        if sort_field != "LAST_MODIFIED":
            params["sortField"] = sort_field
        if sort_order != "DESC":
            params["sortOrder"] = sort_order
        if user_relation is not None:
            params["userRelation"] = user_relation

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def add_issue(self, title: str, description: str = None, community_id: str = None,
                   assigned_to_id: str = None, resource_id: str = None,
                   resource_type: str = None):
        """
        Adds a new issue.
        :param title: The title of the issue (required).
        :param description: Optional description.
        :param community_id: Optional UUID of the community.
        :param assigned_to_id: Optional UUID of the user assigned to the issue.
        :param resource_id: Optional UUID of the resource the issue is about.
        :param resource_type: Optional type of the resource.
        :return: Created issue details.
        """
        if not title:
            raise ValueError("title is required")

        if community_id is not None:
            try:
                uuid.UUID(community_id)
            except ValueError as exc:
                raise ValueError("community_id must be a valid UUID") from exc

        if assigned_to_id is not None:
            try:
                uuid.UUID(assigned_to_id)
            except ValueError as exc:
                raise ValueError("assigned_to_id must be a valid UUID") from exc

        if resource_id is not None:
            try:
                uuid.UUID(resource_id)
            except ValueError as exc:
                raise ValueError("resource_id must be a valid UUID") from exc

        data = {"title": title}
        if description is not None:
            data["description"] = description
        if community_id is not None:
            data["communityId"] = community_id
        if assigned_to_id is not None:
            data["assignedToId"] = assigned_to_id
        if resource_id is not None:
            data["resourceId"] = resource_id
        if resource_type is not None:
            data["resourceType"] = resource_type

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def move_issue(self, issue_id: str, community_id: str):
        """
        Moves an issue to another community.
        :param issue_id: The UUID of the issue to move.
        :param community_id: The UUID of the target community.
        :return: Updated issue details.
        """
        if not issue_id:
            raise ValueError("issue_id is required")
        try:
            uuid.UUID(issue_id)
        except ValueError as exc:
            raise ValueError("issue_id must be a valid UUID") from exc

        if not community_id:
            raise ValueError("community_id is required")
        try:
            uuid.UUID(community_id)
        except ValueError as exc:
            raise ValueError("community_id must be a valid UUID") from exc

        data = {"communityId": community_id}
        response = self._patch(url=f"{self.__base_api}/{issue_id}/community/{community_id}",
                               data=data)
        return self._handle_response(response)
