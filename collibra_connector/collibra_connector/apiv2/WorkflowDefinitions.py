import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class WorkflowDefinitions(BaseAPI):
    """API class for workflow definition operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/workflowDefinitions"

    def find_workflow_definitions(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        asset_id: List[str] = None,
        domain_id: List[str] = None,
        community_id: List[str] = None,
        enabled: bool = None,
        global_workflow: bool = None,
        definition_id_phrase: str = None,
        name: str = None,
        sort_order: str = "ASC",
        sort_field: str = "NAME",
        description: str = None,
        guardrails_validation_result: str = None,
        global_: bool = None
    ) -> Dict[str, Any]:
        """
        Finds the workflow definitions matching the criteria described in the request object.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "assetId": asset_id,
            "domainId": domain_id,
            "communityId": community_id,
            "enabled": enabled,
            "global": global_workflow,
            "definitionIdPhrase": definition_id_phrase,
            "name": name,
            "sortOrder": sort_order,
            "sortField": sort_field,
            "description": description,
            "guardrailsValidationResult": guardrails_validation_result,
            "global": global_
        }

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_asset_type_assignment_rule(
        self,
        workflow_definition_id: str,
        asset_type_id: str,
        domain_id: str = None,
        community_id: str = None,
        status_id: str = None
    ) -> Dict[str, Any]:
        """
        Adds an asset type assignment rule to the workflow definition with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        data = {
            "assetTypeId": asset_type_id,
            "domainId": domain_id,
            "communityId": community_id,
            "statusId": status_id
        }

        response = self._post(url=f"{self.__base_api}/{workflow_definition_id}/assetTypeAssignmentRules", data=data)
        return self._handle_response(response)

    def add_domain_type_assignment_rule(
        self,
        workflow_definition_id: str,
        domain_type_id: str,
        status_id: str = None,
        community_id: str = None
    ) -> Dict[str, Any]:
        """
        Adds a domain type assignment rule to the workflow definition with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        data = {
            "domainTypeId": domain_type_id,
            "statusId": status_id,
            "communityId": community_id
        }

        response = self._post(url=f"{self.__base_api}/{workflow_definition_id}/domainTypeAssignmentRules", data=data)
        return self._handle_response(response)

    def change_asset_type_assignment_rule(
        self,
        workflow_definition_id: str,
        rule_id: str,
        asset_type_id: str = None,
        domain_id: str = None,
        community_id: str = None,
        status_id: str = None
    ) -> Dict[str, Any]:
        """
        Modifies the asset type assignment rule with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id) or not self._uuid_validation(rule_id):
            raise ValueError("workflow_definition_id and rule_id must be valid UUIDs")

        data = {
            "assetTypeId": asset_type_id,
            "domainId": domain_id,
            "communityId": community_id,
            "statusId": status_id
        }

        response = self._patch(url=f"{self.__base_api}/{workflow_definition_id}/assetTypeAssignmentRules/{rule_id}", data=data)
        return self._handle_response(response)

    def change_domain_type_assignment_rule(
        self,
        workflow_definition_id: str,
        rule_id: str,
        domain_type_id: str = None,
        status_id: str = None,
        community_id: str = None
    ) -> Dict[str, Any]:
        """
        Modifies the domain type assignment rule with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id) or not self._uuid_validation(rule_id):
            raise ValueError("workflow_definition_id and rule_id must be valid UUIDs")

        data = {
            "domainTypeId": domain_type_id,
            "statusId": status_id,
            "communityId": community_id
        }

        response = self._patch(url=f"{self.__base_api}/{workflow_definition_id}/domainTypeAssignmentRules/{rule_id}", data=data)
        return self._handle_response(response)

    def get_workflow_definition(self, workflow_definition_id: str) -> Dict[str, Any]:
        """
        Returns the workflow definition with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{workflow_definition_id}")
        return self._handle_response(response)

    def remove_workflow_definition(self, workflow_definition_id: str) -> None:
        """
        Removes the workflow definition with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        response = self._delete(url=f"{self.__base_api}/{workflow_definition_id}")
        return self._handle_response(response)

    def change_workflow_definition(
        self,
        workflow_definition_id: str,
        configuration_variables: Dict[str, Any] = None,
        enable: bool = None,
        name: str = None,
        start_label: str = None,
        global_create: bool = None,
        candidate_user_check_enabled: bool = None,
        exclusivity: bool = None,
        start_role_ids: List[str] = None,
        start_events: List[str] = None,
        business_item_resource_type: str = None,
        candidate_user_check_disabled: bool = None,
        description: str = None,
        guest_user_accessible: bool = None,
        reassign_role_ids: List[str] = None,
        stop_role_ids: List[str] = None,
        registered_user_accessible: bool = None
    ) -> Dict[str, Any]:
        """
        Modifies the workflow definition with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        data = {
            "configurationVariables": configuration_variables,
            "enable": enable,
            "name": name,
            "startLabel": start_label,
            "globalCreate": global_create,
            "candidateUserCheckEnabled": candidate_user_check_enabled,
            "exclusivity": exclusivity,
            "startRoleIds": start_role_ids,
            "startEvents": start_events,
            "businessItemResourceType": business_item_resource_type,
            "candidateUserCheckDisabled": candidate_user_check_disabled,
            "description": description,
            "guestUserAccessible": guest_user_accessible,
            "reassignRoleIds": reassign_role_ids,
            "stopRoleIds": stop_role_ids,
            "registeredUserAccessible": registered_user_accessible
        }

        response = self._patch(url=f"{self.__base_api}/{workflow_definition_id}", json=data)
        return self._handle_response(response)

    def deploy_workflow_definition(self, file_path: str, file_name: str) -> Dict[str, Any]:
        """
        Deploys workflow definition using the specified file.
        """
        # This implementation requires multipart/form-data support in BaseAPI
        files = {
            "file": (file_name, open(file_path, "rb")),
            "fileName": (None, file_name)
        }
        response = self._post(url=self.__base_api, files=files)
        return self._handle_response(response)

    def get_configuration_start_form_data(self, workflow_definition_id: str, form_property_type: str = None) -> Dict[str, Any]:
        """
        Returns the task configuration start form data of the workflow definition.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        params = {"formPropertyType": form_property_type}
        response = self._get(url=f"{self.__base_api}/workflowDefinition/{workflow_definition_id}/configurationStartFormData", params=params)
        return self._handle_response(response)

    def get_possible_start_events(self) -> List[Dict[str, Any]]:
        """
        Returns all possible workflow start events.
        """
        response = self._get(url=f"{self.__base_api}/startEvents")
        return self._handle_response(response)

    def get_start_form_data(self, workflow_definition_id: str, form_property_type: str = None) -> Dict[str, Any]:
        """
        Returns the task start form data of the workflow definition.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        params = {"formPropertyType": form_property_type}
        response = self._get(url=f"{self.__base_api}/workflowDefinition/{workflow_definition_id}/startFormData", params=params)
        return self._handle_response(response)

    def get_workflow_definition_by_process_id(self, process_id: str) -> Dict[str, Any]:
        """
        Returns the workflow definition with the specified process ID.
        """
        response = self._get(url=f"{self.__base_api}/process/{process_id}")
        return self._handle_response(response)

    def get_workflow_definition_diagram(self, workflow_definition_id: str) -> Any:
        """
        Returns the process diagram of the workflow definition.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{workflow_definition_id}/diagram")
        return self._handle_response(response)

    def get_workflow_definition_xml(self, workflow_definition_id: str) -> Any:
        """
        Returns the XML source of the workflow definition.
        """
        if not self._uuid_validation(workflow_definition_id):
            raise ValueError("workflow_definition_id must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/{workflow_definition_id}/xml")
        return self._handle_response(response)

    def remove_assignment_rule(self, workflow_definition_id: str, rule_id: str) -> None:
        """
        Removes the assignment rule with the specified ID.
        """
        if not self._uuid_validation(workflow_definition_id) or not self._uuid_validation(rule_id):
            raise ValueError("workflow_definition_id and rule_id must be valid UUIDs")

        response = self._delete(url=f"{self.__base_api}/{workflow_definition_id}/assignmentRules/{rule_id}")
        return self._handle_response(response)

    def remove_workflow_definitions_in_job(self, workflow_definition_ids: List[str]) -> Dict[str, Any]:
        """
        Removes multiple workflow definitions asynchronously.
        """
        if not workflow_definition_ids or not isinstance(workflow_definition_ids, list):
            raise ValueError("workflow_definition_ids must be a non-empty list")

        response = self._post(url=f"{self.__base_api}/removalJobs", data=workflow_definition_ids)
        return self._handle_response(response)
