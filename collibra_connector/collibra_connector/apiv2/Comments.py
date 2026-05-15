import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Comments(BaseAPI):
    """API class for comment operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/comments"

    def find_comments(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        parent_id: str = None,
        user_id: str = None,
        base_resource_id: str = None,
        root_comment: bool = None,
        resolved: bool = None,
        user_threads: bool = False,
        sort_order: str = "DESC"
    ) -> Dict[str, Any]:
        """
        Searches for comments based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param parent_id: The UUID of the parent comment to filter by (optional).
        :param user_id: The UUID of the user who created the comments (optional).
        :param base_resource_id: The UUID of the base resource associated with the comments (optional).
        :param root_comment: Whether to filter for root comments only (optional).
        :param resolved: Whether to filter for resolved comments only (optional).
        :param user_threads: Whether to filter for user threads only (default: False).
        :param sort_order: The order to sort the results in (default: "DESC").
        :return: A dictionary containing the matching comments.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "parentId": parent_id,
            "userId": user_id,
            "baseResourceId": base_resource_id,
            "rootComment": root_comment,
            "resolved": resolved,
            "userThreads": user_threads,
            "sortOrder": sort_order
        }
        
        # UUID validation
        for param_name in ["parentId", "userId", "baseResourceId"]:
            val = params.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_comment(
        self,
        content: str,
        commentable_resource_id: str = None,
        commentable_resource_discriminator: str = None,
        parent_id: str = None,
        base_resource_id: str = None,
        base_resource_type: str = None,
        base_resource_discriminator: str = None,
        base_resource: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new comment with the specified parameters.

        :param content: The content of the comment (required).
        :param commentable_resource_id: The UUID of the resource the comment is associated with (optional).
        :param commentable_resource_discriminator: The discriminator for the commentable resource (optional).
        :param parent_id: The UUID of the parent comment (optional).
        :param base_resource_id: The UUID of the base resource associated with the comment (optional).
        :param base_resource_type: The type of the base resource (optional).
        :param base_resource_discriminator: The discriminator for the base resource (optional).
        :param base_resource: The base resource associated with the comment (optional).
        :return: A dictionary containing the details of the created comment.
        """
        if not content:
            raise ValueError("content is required")
        
        data = {
            "content": content,
            "commentableResourceId": commentable_resource_id,
            "commentableResourceDiscriminator": commentable_resource_discriminator,
            "parentId": parent_id,
            "baseResourceId": base_resource_id,
            "baseResourceType": base_resource_type,
            "baseResourceDiscriminator": base_resource_discriminator,
            "baseResource": base_resource
        }

        if commentable_resource_id and not self._uuid_validation(commentable_resource_id):
            raise ValueError("commentableResourceId must be a valid UUID")
        if parent_id and not self._uuid_validation(parent_id):
            raise ValueError("parentId must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_comment(self, comment_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a comment by its ID.

        :param comment_id: The unique identifier of the comment to retrieve.
        :return: A dictionary containing the details of the comment.
        """
        if not self._uuid_validation(comment_id):
            raise ValueError("comment_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{comment_id}")
        return self._handle_response(response)

    def remove_comment(self, comment_id: str) -> None:
        """
        Deletes a comment identified by its ID.

        :param comment_id: The unique identifier of the comment to delete.
        """
        if not self._uuid_validation(comment_id):
            raise ValueError("comment_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{comment_id}")
        return self._handle_response(response)

    def change_comment(self, comment_id: str, content: str = None, resolved: bool = None) -> Dict[str, Any]:
        """
        Updates the details of a comment identified by its ID.

        :param comment_id: The unique identifier of the comment to update.
        :param content: The new content for the comment (optional).
        :param resolved: Whether the comment is resolved (optional).
        :return: A dictionary containing the updated details of the comment.
        """
        if not self._uuid_validation(comment_id):
            raise ValueError("comment_id must be a valid UUID")
        
        data = {}
        if content is not None:
            data["content"] = content
        if resolved is not None:
            data["resolved"] = resolved

        response = self._patch(url=f"{self.__base_api}/{comment_id}", data=data)
        return self._handle_response(response)
