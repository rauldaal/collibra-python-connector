import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class WorkflowInstances(BaseAPI):
    """API class for workflow instance operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/workflowInstances"

    def cancel_workflow_instances(self, workflow_instance_id: str, reason: str = None) -> None:
        """
        Cancels a workflow instance by its ID.

        :param workflow_instance_id: The unique identifier of the workflow instance to cancel.
        :param reason: The reason for canceling the workflow instance (optional).
        """
        if not self._uuid_validation(workflow_instance_id):
            raise ValueError("workflow_instance_id must be a valid UUID")
        
        response = self._post(url=f"{self.__base_api}/{workflow_instance_id}/canceled", data=reason)
        return self._handle_response(response)

    def find_workflow_instances(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        business_item_name: str = None,
        business_item_id: str = None,
        workflow_definition_id: str = None,
        workflow_definition_name: str = None,
        workflow_instance_id_phrase: str = None,
        sort_field: str = "START_DATE",
        sort_order: str = "DESC",
        parent_workflow_instance_id: str = None
    ) -> Dict[str, Any]:
        """
        Searches for workflow instances based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param business_item_name: The name of the business item to filter by (optional).
        :param business_item_id: The ID of the business item to filter by (optional).
        :param workflow_definition_id: The ID of the workflow definition to filter by (optional).
        :param workflow_definition_name: The name of the workflow definition to filter by (optional).
        :param workflow_instance_id_phrase: A phrase to filter workflow instance IDs (optional).
        :param sort_field: The field to sort the results by (default: "START_DATE").
        :param sort_order: The order to sort the results in (default: "DESC").
        :param parent_workflow_instance_id: The ID of the parent workflow instance to filter by (optional).
        :return: A dictionary containing the matching workflow instances.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "businessItemName": business_item_name,
            "businessItemId": business_item_id,
            "workflowDefinitionId": workflow_definition_id,
            "workflowDefinitionName": workflow_definition_name,
            "workflowInstanceIdPhrase": workflow_instance_id_phrase,
            "sortField": sort_field,
            "sortOrder": sort_order,
            "parentWorkflowInstanceId": parent_workflow_instance_id
        }
        
        if business_item_id and not self._uuid_validation(business_item_id):
            raise ValueError("businessItemId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def start_workflow_instances(
        self,
        workflow_definition_id: str,
        business_item_ids: List[str] = None,
        business_item_type: str = None,
        form_properties: Dict[str, Any] = None,
        guest_user_id: str = None,
        send_notification: bool = None
    ) -> List[Dict[str, Any]]:
        """
        Starts multiple workflow instances based on the provided parameters.

        :param workflow_definition_id: The unique identifier of the workflow definition to start instances for.
        :param business_item_ids: A list of business item IDs to associate with the workflow instances (optional).
        :param business_item_type: The type of the business items (optional).
        :param form_properties: A dictionary of form properties to pass to the workflow instances (optional).
        :param guest_user_id: The ID of the guest user to associate with the workflow instances (optional).
        :param send_notification: Whether to send notifications for the workflow instances (optional).
        :return: A list of dictionaries containing the details of the started workflow instances.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")
        
        data = {
            "workflowDefinitionId": workflow_definition_id,
            "businessItemIds": business_item_ids,
            "businessItemType": business_item_type,
            "formProperties": form_properties,
            "guestUserId": guest_user_id,
            "sendNotification": send_notification
        }
        
        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_workflow_instance_diagram(self, workflow_instance_id: str) -> Any:
        """
        Retrieves the diagram of a workflow instance by its ID.

        :param workflow_instance_id: The unique identifier of the workflow instance.
        :return: The file representing the diagram of the workflow instance.
        """
        if not self._uuid_validation(workflow_instance_id):
            raise ValueError("workflow_instance_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{workflow_instance_id}/diagram")
        return self._handle_response(response)

    def message_event_received(self, process_instance_id: str, message_name: str, variables: Dict[str, Any] = None) -> None:
        """
        Sends a message event to the workflow engine.

        :param process_instance_id: The unique identifier of the process instance.
        :param message_name: The name of the message event.
        :param variables: A dictionary of variables to pass with the message event (optional).
        """
        data = {"variables": variables}
        response = self._post(url=f"{self.__base_api}/{process_instance_id}/messageEvents/{message_name}", data=data)
        return self._handle_response(response)

    def start_workflow_instances_in_job(
        self,
        workflow_definition_id: str,
        business_item_ids: List[str] = None,
        business_item_type: str = None,
        form_properties: Dict[str, Any] = None,
        guest_user_id: str = None,
        send_notification: bool = None
    ) -> Dict[str, Any]:
        """
        Starts multiple workflow instances asynchronously as a job.

        :param workflow_definition_id: The unique identifier of the workflow definition to start instances for.
        :param business_item_ids: A list of business item IDs to associate with the workflow instances (optional).
        :param business_item_type: The type of the business items (optional).
        :param form_properties: A dictionary of form properties to pass to the workflow instances (optional).
        :param guest_user_id: The ID of the guest user to associate with the workflow instances (optional).
        :param send_notification: Whether to send notifications for the workflow instances (optional).
        :return: A dictionary containing the details of the started workflow instances job.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")
        
        data = {
            "workflowDefinitionId": workflow_definition_id,
            "businessItemIds": business_item_ids,
            "businessItemType": business_item_type,
            "formProperties": form_properties,
            "guestUserId": guest_user_id,
            "sendNotification": send_notification
        }
        
        response = self._post(url=f"{self.__base_api}/startJobs", data=data)
        return self._handle_response(response)
