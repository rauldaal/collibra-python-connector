import uuid
from .Base import BaseAPI


class WorkflowTask(BaseAPI):
    """API class for workflow task operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/workflowTasks"

    def find_workflow_tasks(
        self,
        business_item_id: str = None,
        business_item_name: str = None,
        business_item_type: str = None,
        count_limit: int = -1,
        create_date: str = None,
        description: str = None,
        due_date: str = None,
        limit: int = 0,
        offset: int = 0,
        sort_field: str = "CREATE_DATE",
        sort_order: str = "DESC",
        task_id_phrase: str = None,
        title: str = None,
        type: str = None,
        user_id: str = None,
        workflow_task_user_relation: str = None,
    ):
        """
        Returns workflow tasks matching the given search criteria.
        :param business_item_id: UUID of the business item to filter by.
        :param business_item_name: Name of the business item to filter by.
        :param business_item_type: Type of the business item.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param create_date: Creation date filter (ISO 8601 format).
        :param description: Description phrase filter.
        :param due_date: Due date filter (ISO 8601 format).
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :param task_id_phrase: Phrase to match against task IDs.
        :param title: Title phrase filter.
        :param type: Task type filter.
        :param user_id: UUID of the user to filter by.
        :param workflow_task_user_relation: Relation of the user to the task.
        :return: List of workflow tasks.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        for param_name, param_value in [("business_item_id", business_item_id),
                                         ("user_id", user_id)]:
            if param_value is not None:
                try:
                    uuid.UUID(param_value)
                except ValueError as exc:
                    raise ValueError(f"{param_name} must be a valid UUID") from exc

        params = {}
        if business_item_id is not None:
            params["businessItemId"] = business_item_id
        if business_item_name is not None:
            params["businessItemName"] = business_item_name
        if business_item_type is not None:
            params["businessItemType"] = business_item_type
        if count_limit != -1:
            params["countLimit"] = count_limit
        if create_date is not None:
            params["createDate"] = create_date
        if description is not None:
            params["description"] = description
        if due_date is not None:
            params["dueDate"] = due_date
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if sort_field != "CREATE_DATE":
            params["sortField"] = sort_field
        if sort_order != "DESC":
            params["sortOrder"] = sort_order
        if task_id_phrase is not None:
            params["taskIdPhrase"] = task_id_phrase
        if title is not None:
            params["title"] = title
        if type is not None:
            params["type"] = type
        if user_id is not None:
            params["userId"] = user_id
        if workflow_task_user_relation is not None:
            params["workflowTaskUserRelation"] = workflow_task_user_relation

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_workflow_task(self, workflow_task_id: str):
        """
        Returns the workflow task with the given ID.
        :param workflow_task_id: The UUID of the workflow task.
        :return: Workflow task details.
        """
        if not workflow_task_id:
            raise ValueError("workflow_task_id is required")
        try:
            uuid.UUID(workflow_task_id)
        except ValueError as exc:
            raise ValueError("workflow_task_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{workflow_task_id}")
        return self._handle_response(response)

    def get_task_form_data(self, workflow_task_id: str, form_property_type: str = None):
        """
        Returns the form data for the workflow task with the given ID.
        :param workflow_task_id: The UUID of the workflow task.
        :param form_property_type: Optional type to filter form properties.
        :return: Task form data.
        """
        if not workflow_task_id:
            raise ValueError("workflow_task_id is required")
        try:
            uuid.UUID(workflow_task_id)
        except ValueError as exc:
            raise ValueError("workflow_task_id must be a valid UUID") from exc

        params = {}
        if form_property_type is not None:
            params["formPropertyType"] = form_property_type

        response = self._get(
            url=f"{self.__base_api}/{workflow_task_id}/taskFormData",
            params=params or None
        )
        return self._handle_response(response)

    def complete_workflow_tasks(self, task_ids: list, task_form_properties: dict = None):
        """
        Completes the workflow tasks with the given IDs.
        :param task_ids: List of workflow task UUIDs to complete (required).
        :param task_form_properties: Optional dict mapping task IDs to form property values.
        :return: None
        """
        if not task_ids:
            raise ValueError("task_ids is required and cannot be empty")
        if not isinstance(task_ids, list):
            raise ValueError("task_ids must be a list")
        for task_id in task_ids:
            try:
                uuid.UUID(task_id)
            except ValueError as exc:
                raise ValueError(f"'{task_id}' in task_ids is not a valid UUID") from exc

        data = {"taskIds": task_ids}
        if task_form_properties is not None:
            data["taskFormProperties"] = task_form_properties

        response = self._post(url=f"{self.__base_api}/completed", data=data)
        return self._handle_response(response)

    def cancel_workflow_task(self, workflow_task_id: str):
        """
        Cancels the workflow task with the given ID.
        :param workflow_task_id: The UUID of the workflow task to cancel.
        :return: None
        """
        if not workflow_task_id:
            raise ValueError("workflow_task_id is required")
        try:
            uuid.UUID(workflow_task_id)
        except ValueError as exc:
            raise ValueError("workflow_task_id must be a valid UUID") from exc

        response = self._post(url=f"{self.__base_api}/{workflow_task_id}/canceled", data={})
        return self._handle_response(response)

    def reassign_task(self, workflow_task_id: str,
                      users: list = None,
                      groups: list = None,
                      roles: list = None,
                      communities: list = None):
        """
        Reassigns the workflow task with the given ID.
        :param workflow_task_id: The UUID of the workflow task to reassign.
        :param users: Optional list of user UUIDs to reassign to.
        :param groups: Optional list of group UUIDs to reassign to.
        :param roles: Optional list of role UUIDs to reassign to.
        :param communities: Optional list of community UUIDs to reassign to.
        :return: None
        """
        if not workflow_task_id:
            raise ValueError("workflow_task_id is required")
        try:
            uuid.UUID(workflow_task_id)
        except ValueError as exc:
            raise ValueError("workflow_task_id must be a valid UUID") from exc

        params = {}
        if users is not None:
            params["users"] = users
        if groups is not None:
            params["groups"] = groups
        if roles is not None:
            params["roles"] = roles
        if communities is not None:
            params["communities"] = communities

        response = self._post(
            url=f"{self.__base_api}/{workflow_task_id}/reassign",
            data={},
            params=params or None
        )
        return self._handle_response(response)
