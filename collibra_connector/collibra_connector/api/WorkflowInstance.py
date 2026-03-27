import uuid
from .Base import BaseAPI


class WorkflowInstance(BaseAPI):
    """API class for workflow instance operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/workflowInstances"

    def find_workflow_instances(
        self,
        business_item_id: str = None,
        business_item_name: str = None,
        count_limit: int = -1,
        limit: int = 0,
        offset: int = 0,
        parent_workflow_instance_id: str = None,
        sort_field: str = "START_DATE",
        sort_order: str = "DESC",
        workflow_definition_id: str = None,
        workflow_definition_name: str = None,
        workflow_instance_id_phrase: str = None,
    ):
        """
        Returns workflow instances matching the given search criteria.
        :param business_item_id: UUID of the business item to filter by.
        :param business_item_name: Name of the business item to filter by.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param parent_workflow_instance_id: UUID of the parent workflow instance to filter by.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :param workflow_definition_id: UUID of the workflow definition to filter by.
        :param workflow_definition_name: Name of the workflow definition to filter by.
        :param workflow_instance_id_phrase: Phrase to match against workflow instance IDs.
        :return: List of workflow instances.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        for param_name, param_value in [("business_item_id", business_item_id),
                                         ("parent_workflow_instance_id", parent_workflow_instance_id),
                                         ("workflow_definition_id", workflow_definition_id)]:
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
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if parent_workflow_instance_id is not None:
            params["parentWorkflowInstanceId"] = parent_workflow_instance_id
        if sort_field != "START_DATE":
            params["sortField"] = sort_field
        if sort_order != "DESC":
            params["sortOrder"] = sort_order
        if workflow_definition_id is not None:
            params["workflowDefinitionId"] = workflow_definition_id
        if workflow_definition_name is not None:
            params["workflowDefinitionName"] = workflow_definition_name
        if workflow_instance_id_phrase is not None:
            params["workflowInstanceIdPhrase"] = workflow_instance_id_phrase

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def start_workflow_instances(self, workflow_definition_id: str,
                                  business_item_ids: list = None,
                                  business_item_type: str = None,
                                  form_properties: dict = None,
                                  guest_user_id: str = None,
                                  send_notification: bool = False):
        """
        Starts one or more workflow instances.
        :param workflow_definition_id: The UUID of the workflow definition (required).
        :param business_item_ids: Optional list of business item UUIDs.
        :param business_item_type: Optional type of the business items
                                   (ASSET, DOMAIN, COMMUNITY, GLOBAL, USER).
        :param form_properties: Optional dict of form properties.
        :param guest_user_id: Optional UUID of the guest user.
        :param send_notification: Whether to send a notification. Default: False.
        :return: List of created workflow instance details.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        data = {
            "workflowDefinitionId": workflow_definition_id,
            "sendNotification": send_notification,
        }

        if business_item_ids is not None:
            if not isinstance(business_item_ids, list):
                raise ValueError("business_item_ids must be a list")
            for item_id in business_item_ids:
                try:
                    uuid.UUID(item_id)
                except ValueError as exc:
                    raise ValueError(f"'{item_id}' in business_item_ids is not a valid UUID") from exc
            data["businessItemIds"] = business_item_ids

        if business_item_type is not None:
            valid_types = ["ASSET", "DOMAIN", "COMMUNITY", "GLOBAL", "USER"]
            if business_item_type not in valid_types:
                raise ValueError(f"business_item_type must be one of: {', '.join(valid_types)}")
            data["businessItemType"] = business_item_type

        if form_properties is not None:
            data["formProperties"] = form_properties

        if guest_user_id is not None:
            try:
                uuid.UUID(guest_user_id)
            except ValueError as exc:
                raise ValueError("guest_user_id must be a valid UUID") from exc
            data["guestUserId"] = guest_user_id

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def start_workflow_instances_in_job(self, workflow_definition_id: str,
                                         business_item_ids: list = None,
                                         business_item_type: str = None,
                                         form_properties: dict = None):
        """
        Starts workflow instances asynchronously via a background job.
        :param workflow_definition_id: The UUID of the workflow definition (required).
        :param business_item_ids: Optional list of business item UUIDs.
        :param business_item_type: Optional type of the business items.
        :param form_properties: Optional dict of form properties.
        :return: Job details.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        data = {"workflowDefinitionId": workflow_definition_id}

        if business_item_ids is not None:
            data["businessItemIds"] = business_item_ids
        if business_item_type is not None:
            data["businessItemType"] = business_item_type
        if form_properties is not None:
            data["formProperties"] = form_properties

        response = self._post(url=f"{self.__base_api}/startJobs", data=data)
        return self._handle_response(response)

    def cancel_workflow_instance(self, workflow_instance_id: str):
        """
        Cancels the workflow instance with the given ID.
        :param workflow_instance_id: The UUID of the workflow instance to cancel.
        :return: None
        """
        if not workflow_instance_id:
            raise ValueError("workflow_instance_id is required")
        try:
            uuid.UUID(workflow_instance_id)
        except ValueError as exc:
            raise ValueError("workflow_instance_id must be a valid UUID") from exc

        data = {"workflowInstanceId": workflow_instance_id}
        response = self._post(url=f"{self.__base_api}/{workflow_instance_id}/canceled", data=data)
        return self._handle_response(response)

    def get_workflow_instance_diagram(self, workflow_instance_id: str):
        """
        Returns the diagram for the workflow instance with the given ID.
        :param workflow_instance_id: The UUID of the workflow instance.
        :return: Workflow instance diagram.
        """
        if not workflow_instance_id:
            raise ValueError("workflow_instance_id is required")
        try:
            uuid.UUID(workflow_instance_id)
        except ValueError as exc:
            raise ValueError("workflow_instance_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{workflow_instance_id}/diagram")
        return self._handle_response(response)

    def send_message_event(self, process_instance_id: str, message_name: str,
                            payload: dict = None):
        """
        Sends a message event to a workflow instance.
        :param process_instance_id: The process instance ID of the workflow.
        :param message_name: The name of the message event.
        :param payload: Optional payload for the message event.
        :return: Result of the message event.
        """
        if not process_instance_id or not message_name:
            raise ValueError("process_instance_id and message_name are required")

        data = payload or {}
        response = self._post(
            url=f"{self.__base_api}/{process_instance_id}/messageEvents/{message_name}",
            data=data
        )
        return self._handle_response(response)
