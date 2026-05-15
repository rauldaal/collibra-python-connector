import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class WorkflowTasks(BaseAPI):
    """API class for workflow task operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/workflowTasks"

    def cancel_workflow_task(self, workflow_task_id: str, reason: str = None) -> None:
        """
        Cancels a workflow task by its ID.

        :param workflow_task_id: The unique identifier of the workflow task to cancel.
        :param reason: The reason for canceling the workflow task (optional).
        """
        if not self._uuid_validation(workflow_task_id):
            raise ValueError("workflow_task_id must be a valid UUID")
        
        response = self._post(url=f"{self.__base_api}/{workflow_task_id}/canceled", data=reason)
        return self._handle_response(response)

    def complete_workflow_tasks(
        self,
        task_ids: List[str],
        task_form_properties: Dict[str, Any] = None,
        guest_user_id: str = None,
        form_properties: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Completes multiple workflow tasks based on the provided parameters.

        :param task_ids: A list of task UUIDs to complete.
        :param task_form_properties: A dictionary of form properties for the tasks (optional).
        :param guest_user_id: The ID of the guest user completing the tasks (optional).
        :param form_properties: Additional form properties for the tasks (optional).
        :return: A list of dictionaries containing the details of the completed tasks.
        """
        if not task_ids or not isinstance(task_ids, list):
            raise ValueError("task_ids must be a non-empty list")

        data = {
            "taskIds": task_ids,
            "taskFormProperties": task_form_properties,
            "guestUserId": guest_user_id,
            "formProperties": form_properties
        }

        response = self._post(url=f"{self.__base_api}/completed", data=data)
        return self._handle_response(response)

    def find_workflow_tasks(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        business_item_id: str = None,
        business_item_type: str = None,
        task_id_phrase: str = None,
        workflow_task_user_relation: str = "ALL",
        business_item_name: str = None,
        description: str = None,
        user_id: str = None,
        create_date: int = None,
        due_date: int = None,
        title: str = None,
        type: str = None,
        sort_field: str = "DUE_DATE",
        sort_order: str = "DESC"
    ) -> Dict[str, Any]:
        """
        Searches for workflow tasks based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param business_item_id: The ID of the business item to filter by (optional).
        :param business_item_type: The type of the business item to filter by (optional).
        :param task_id_phrase: A phrase to filter task IDs (optional).
        :param workflow_task_user_relation: The user relation to the workflow task (default: "ALL").
        :param business_item_name: The name of the business item to filter by (optional).
        :param description: The description of the workflow task to filter by (optional).
        :param user_id: The ID of the user to filter by (optional).
        :param create_date: The creation date of the workflow task to filter by (optional).
        :param due_date: The due date of the workflow task to filter by (optional).
        :param title: The title of the workflow task to filter by (optional).
        :param type: The type of the workflow task to filter by (optional).
        :param sort_field: The field to sort the results by (default: "DUE_DATE").
        :param sort_order: The order to sort the results in (default: "DESC").
        :return: A dictionary containing the matching workflow tasks.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "businessItemId": business_item_id,
            "businessItemType": business_item_type,
            "taskIdPhrase": task_id_phrase,
            "workflowTaskUserRelation": workflow_task_user_relation,
            "businessItemName": business_item_name,
            "description": description,
            "userId": user_id,
            "createDate": create_date,
            "dueDate": due_date,
            "title": title,
            "type": type,
            "sortField": sort_field,
            "sortOrder": sort_order
        }
        
        if business_item_id and not self._uuid_validation(business_item_id):
            raise ValueError("businessItemId must be a valid UUID")
        if user_id and not self._uuid_validation(user_id):
            raise ValueError("userId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def get_task_form_data(self, workflow_task_id: str, form_property_type: str = None) -> Dict[str, Any]:
        """
        Retrieves the form data of a workflow task by its ID.

        :param workflow_task_id: The unique identifier of the workflow task.
        :param form_property_type: The type of form property to retrieve (optional).
        :return: A dictionary containing the form data of the workflow task.
        """
        if not self._uuid_validation(workflow_task_id):
            raise ValueError("workflow_task_id must be a valid UUID")
        
        params = {"formPropertyType": form_property_type}
        response = self._get(url=f"{self.__base_api}/{workflow_task_id}/taskFormData", params=params)
        return self._handle_response(response)

    def get_workflow_task(self, workflow_task_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a workflow task by its ID.

        :param workflow_task_id: The unique identifier of the workflow task.
        :return: A dictionary containing the details of the workflow task.
        """
        if not self._uuid_validation(workflow_task_id):
            raise ValueError("workflow_task_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{workflow_task_id}")
        return self._handle_response(response)

    def reassign_task(
        self,
        workflow_task_id: str,
        users: List[str] = None,
        groups: List[str] = None,
        roles: List[str] = None,
        communities: List[str] = None
    ) -> None:
        """
        Reassigns a workflow task to specified users, groups, roles, or communities.

        :param workflow_task_id: The unique identifier of the workflow task to reassign.
        :param users: A list of user IDs to reassign the task to (optional).
        :param groups: A list of group IDs to reassign the task to (optional).
        :param roles: A list of role IDs to reassign the task to (optional).
        :param communities: A list of community IDs to reassign the task to (optional).
        """
        if not self._uuid_validation(workflow_task_id):
            raise ValueError("workflow_task_id must be a valid UUID")
        
        params = {
            "users": users,
            "groups": groups,
            "roles": roles,
            "communities": communities
        }
        
        response = self._post(url=f"{self.__base_api}/{workflow_task_id}/reassign", data=params)
        return self._handle_response(response)
