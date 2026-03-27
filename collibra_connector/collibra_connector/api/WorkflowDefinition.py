import uuid
from .Base import BaseAPI


class WorkflowDefinition(BaseAPI):
    """API class for workflow definition operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/workflowDefinitions"

    def find_workflow_definitions(
        self,
        asset_id: str = None,
        community_id: str = None,
        count_limit: int = -1,
        definition_id_phrase: str = None,
        description: str = None,
        domain_id: str = None,
        enabled: bool = None,
        global_workflow: bool = None,
        guardrails_validation_result: str = None,
        limit: int = 0,
        name: str = None,
        offset: int = 0,
        sort_field: str = "NAME",
        sort_order: str = "ASC",
    ):
        """
        Returns workflow definitions matching the given search criteria.
        :param asset_id: UUID of asset to filter by.
        :param community_id: UUID of community to filter by.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param definition_id_phrase: Phrase to match against the workflow definition process ID.
        :param description: Description to filter by.
        :param domain_id: UUID of domain to filter by.
        :param enabled: Whether to filter by enabled/disabled state.
        :param global_workflow: Whether to filter by global workflows.
        :param guardrails_validation_result: Guardrails validation result filter.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param offset: First result to retrieve.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :return: List of workflow definitions.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        for param_name, param_value in [("asset_id", asset_id), ("community_id", community_id),
                                         ("domain_id", domain_id)]:
            if param_value is not None:
                try:
                    uuid.UUID(param_value)
                except ValueError as exc:
                    raise ValueError(f"{param_name} must be a valid UUID") from exc

        params = {}
        if asset_id is not None:
            params["assetId"] = asset_id
        if community_id is not None:
            params["communityId"] = community_id
        if count_limit != -1:
            params["countLimit"] = count_limit
        if definition_id_phrase is not None:
            params["definitionIdPhrase"] = definition_id_phrase
        if description is not None:
            params["description"] = description
        if domain_id is not None:
            params["domainId"] = domain_id
        if enabled is not None:
            params["enabled"] = enabled
        if global_workflow is not None:
            params["global"] = global_workflow
        if guardrails_validation_result is not None:
            params["guardrailsValidationResult"] = guardrails_validation_result
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if offset != 0:
            params["offset"] = offset
        if sort_field != "NAME":
            params["sortField"] = sort_field
        if sort_order != "ASC":
            params["sortOrder"] = sort_order

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_workflow_definition(self, workflow_definition_id: str):
        """
        Returns the workflow definition identified by the given UUID.
        :param workflow_definition_id: The UUID of the workflow definition.
        :return: Workflow definition details.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{workflow_definition_id}")
        return self._handle_response(response)

    def get_workflow_definition_by_process_id(self, process_id: str):
        """
        Returns the workflow definition identified by the given process ID.
        :param process_id: The process ID of the workflow definition.
        :return: Workflow definition details.
        """
        if not process_id:
            raise ValueError("process_id is required")

        response = self._get(url=f"{self.__base_api}/process/{process_id}")
        return self._handle_response(response)

    def get_workflow_definition_diagram(self, workflow_definition_id: str):
        """
        Returns the diagram for the workflow definition identified by the given UUID.
        :param workflow_definition_id: The UUID of the workflow definition.
        :return: Workflow definition diagram.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{workflow_definition_id}/diagram")
        return self._handle_response(response)

    def get_workflow_definition_xml(self, workflow_definition_id: str):
        """
        Returns the XML for the workflow definition identified by the given UUID.
        :param workflow_definition_id: The UUID of the workflow definition.
        :return: Workflow definition XML.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{workflow_definition_id}/xml")
        return self._handle_response(response)

    def get_start_form_data(self, workflow_definition_id: str, form_property_type: str = None):
        """
        Returns the start form data for the workflow definition.
        :param workflow_definition_id: The UUID of the workflow definition.
        :param form_property_type: Optional filter for form property types.
        :return: Start form data.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        params = {}
        if form_property_type is not None:
            params["formPropertyType"] = form_property_type

        response = self._get(
            url=f"{self.__base_api}/workflowDefinition/{workflow_definition_id}/startFormData",
            params=params or None
        )
        return self._handle_response(response)

    def get_configuration_start_form_data(self, workflow_definition_id: str,
                                           form_property_type: str = None):
        """
        Returns the configuration start form data for the workflow definition.
        :param workflow_definition_id: The UUID of the workflow definition.
        :param form_property_type: Optional filter for form property types.
        :return: Configuration start form data.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        params = {}
        if form_property_type is not None:
            params["formPropertyType"] = form_property_type

        response = self._get(
            url=f"{self.__base_api}/workflowDefinition/{workflow_definition_id}/configurationStartFormData",
            params=params or None
        )
        return self._handle_response(response)

    def get_possible_start_events(self):
        """
        Returns all possible workflow start events.
        :return: List of possible start events.
        """
        response = self._get(url=f"{self.__base_api}/startEvents")
        return self._handle_response(response)

    def deploy_workflow_definition(self, xml_content: str, deploy_to_all_environments: bool = None):
        """
        Deploys a new workflow definition.
        :param xml_content: The BPMN XML content of the workflow definition.
        :param deploy_to_all_environments: Whether to deploy to all environments.
        :return: Deployed workflow definition details.
        """
        if not xml_content:
            raise ValueError("xml_content is required")

        data = {"xmlContent": xml_content}
        if deploy_to_all_environments is not None:
            data["deployToAllEnvironments"] = deploy_to_all_environments

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def change_workflow_definition(self, workflow_definition_id: str, name: str = None,
                                    description: str = None, enabled: bool = None):
        """
        Changes the workflow definition with the given ID.
        :param workflow_definition_id: The UUID of the workflow definition to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :param enabled: Optional enabled/disabled state.
        :return: Updated workflow definition details.
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if enabled is not None:
            data["enabled"] = enabled

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{workflow_definition_id}", data=data)
        return self._handle_response(response)

    def remove_workflow_definition(self, workflow_definition_id: str):
        """
        Removes the workflow definition identified by the given UUID.
        :param workflow_definition_id: The UUID of the workflow definition.
        :return: None
        """
        if not workflow_definition_id:
            raise ValueError("workflow_definition_id is required")
        try:
            uuid.UUID(workflow_definition_id)
        except ValueError as exc:
            raise ValueError("workflow_definition_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{workflow_definition_id}")
        return self._handle_response(response)

    def remove_workflow_definitions_in_job(self, workflow_definition_ids: list):
        """
        Removes multiple workflow definitions in a background job.
        :param workflow_definition_ids: List of workflow definition UUIDs to remove.
        :return: Job details.
        """
        if not workflow_definition_ids or not isinstance(workflow_definition_ids, list):
            raise ValueError("workflow_definition_ids must be a non-empty list")
        data = {"workflowDefinitionIds": workflow_definition_ids}
        response = self._post(url=f"{self.__base_api}/removalJobs", data=data)
        return self._handle_response(response)

    def add_asset_type_assignment_rule(self, workflow_definition_id: str,
                                        asset_type_id: str, rule_type: str = None):
        """
        Adds an asset type assignment rule to a workflow definition.
        :param workflow_definition_id: The UUID of the workflow definition.
        :param asset_type_id: The UUID of the asset type.
        :param rule_type: Optional rule type.
        :return: Created rule details.
        """
        if not workflow_definition_id or not asset_type_id:
            raise ValueError("workflow_definition_id and asset_type_id are required")

        for param_name, param_value in [("workflow_definition_id", workflow_definition_id),
                                         ("asset_type_id", asset_type_id)]:
            try:
                uuid.UUID(param_value)
            except ValueError as exc:
                raise ValueError(f"{param_name} must be a valid UUID") from exc

        data = {"assetTypeId": asset_type_id}
        if rule_type is not None:
            data["ruleType"] = rule_type

        response = self._post(
            url=f"{self.__base_api}/{workflow_definition_id}/assetTypeAssignmentRules",
            data=data
        )
        return self._handle_response(response)

    def add_domain_type_assignment_rule(self, workflow_definition_id: str,
                                         domain_type_id: str, rule_type: str = None):
        """
        Adds a domain type assignment rule to a workflow definition.
        :param workflow_definition_id: The UUID of the workflow definition.
        :param domain_type_id: The UUID of the domain type.
        :param rule_type: Optional rule type.
        :return: Created rule details.
        """
        if not workflow_definition_id or not domain_type_id:
            raise ValueError("workflow_definition_id and domain_type_id are required")

        for param_name, param_value in [("workflow_definition_id", workflow_definition_id),
                                         ("domain_type_id", domain_type_id)]:
            try:
                uuid.UUID(param_value)
            except ValueError as exc:
                raise ValueError(f"{param_name} must be a valid UUID") from exc

        data = {"domainTypeId": domain_type_id}
        if rule_type is not None:
            data["ruleType"] = rule_type

        response = self._post(
            url=f"{self.__base_api}/{workflow_definition_id}/domainTypeAssignmentRules",
            data=data
        )
        return self._handle_response(response)

    def remove_assignment_rule(self, workflow_definition_id: str, rule_id: str):
        """
        Removes an assignment rule from a workflow definition.
        :param workflow_definition_id: The UUID of the workflow definition.
        :param rule_id: The UUID of the rule to remove.
        :return: None
        """
        if not workflow_definition_id or not rule_id:
            raise ValueError("workflow_definition_id and rule_id are required")

        for param_name, param_value in [("workflow_definition_id", workflow_definition_id),
                                         ("rule_id", rule_id)]:
            try:
                uuid.UUID(param_value)
            except ValueError as exc:
                raise ValueError(f"{param_name} must be a valid UUID") from exc

        response = self._delete(
            url=f"{self.__base_api}/{workflow_definition_id}/assignmentRules/{rule_id}"
        )
        return self._handle_response(response)
