import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Assignments(BaseAPI):
    """API class for assignment operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/assignments"

    def add_assignment(
        self,
        asset_type_id: str,
        id: str = None,
        status_ids: List[str] = None,
        characteristic_types: List[Dict[str, Any]] = None,
        articulation_rules: List[Dict[str, Any]] = None,
        validation_rule_ids: List[str] = None,
        data_quality_rule_ids: List[str] = None,
        domain_type_ids: List[str] = None,
        default_status_id: str = None,
        scope_id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new assignment with the specified parameters.

        :param asset_type_id: The UUID of the asset type (required).
        :param id: The unique identifier of the assignment (optional).
        :param status_ids: A list of status IDs associated with the assignment (optional).
        :param characteristic_types: A list of characteristic types for the assignment (optional).
        :param articulation_rules: A list of articulation rules for the assignment (optional).
        :param validation_rule_ids: A list of validation rule IDs for the assignment (optional).
        :param data_quality_rule_ids: A list of data quality rule IDs for the assignment (optional).
        :param domain_type_ids: A list of domain type IDs for the assignment (optional).
        :param default_status_id: The default status ID for the assignment (optional).
        :param scope_id: The scope ID for the assignment (optional).
        :return: A dictionary containing the details of the created assignment.
        """
        if not asset_type_id:
            raise ValueError("asset_type_id is required")

        data = {
            "assetTypeId": asset_type_id,
            "id": id,
            "statusIds": status_ids,
            "characteristicTypes": characteristic_types,
            "articulationRules": articulation_rules,
            "validationRuleIds": validation_rule_ids,
            "dataQualityRuleIds": data_quality_rule_ids,
            "domainTypeIds": domain_type_ids,
            "defaultStatusId": default_status_id,
            "scopeId": scope_id
        }

        # UUID validation
        for param_name in ["assetTypeId", "id", "defaultStatusId", "scopeId"]:
            val = data.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def remove_assignment(self, assignment_id: str) -> None:
        """
        Deletes an assignment identified by its ID.

        :param assignment_id: The unique identifier of the assignment to delete.
        """
        if not self._uuid_validation(assignment_id):
            raise ValueError("assignment_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{assignment_id}")
        return self._handle_response(response)

    def change_assignment(
        self,
        assignment_id: str,
        status_ids: List[str] = None,
        characteristic_types: List[Dict[str, Any]] = None,
        articulation_rules: List[Dict[str, Any]] = None,
        validation_rule_ids: List[str] = None,
        data_quality_rule_ids: List[str] = None,
        domain_type_ids: List[str] = None,
        default_status_id: str = None,
        scope_id: str = None,
    ) -> Dict[str, Any]:
        """
        Updates the details of an assignment identified by its ID.

        :param assignment_id: The unique identifier of the assignment to update.
        :param status_ids: A list of new status IDs for the assignment (optional).
        :param characteristic_types: A list of new characteristic types for the assignment (optional).
        :param articulation_rules: A list of new articulation rules for the assignment (optional).
        :param validation_rule_ids: A list of new validation rule IDs for the assignment (optional).
        :param data_quality_rule_ids: A list of new data quality rule IDs for the assignment (optional).
        :param domain_type_ids: A list of new domain type IDs for the assignment (optional).
        :param default_status_id: The new default status ID for the assignment (optional).
        :param scope_id: The new scope ID for the assignment (optional).
        :return: A dictionary containing the updated details of the assignment.
        """
        if not self._uuid_validation(assignment_id):
            raise ValueError("assignment_id must be a valid UUID")

        data = {}
        if status_ids is not None:
            data["statusIds"] = status_ids
        if characteristic_types is not None:
            data["characteristicTypes"] = characteristic_types
        if articulation_rules is not None:
            data["articulationRules"] = articulation_rules
        if validation_rule_ids is not None:
            data["validationRuleIds"] = validation_rule_ids
        if data_quality_rule_ids is not None:
            data["dataQualityRuleIds"] = data_quality_rule_ids
        if domain_type_ids is not None:
            data["domainTypeIds"] = domain_type_ids
        if default_status_id is not None:
            data["defaultStatusId"] = default_status_id
        if scope_id is not None:
            data["scopeId"] = scope_id

        response = self._patch(url=f"{self.__base_api}/{assignment_id}", data=data)
        return self._handle_response(response)

    def find_assignments_for_resource(self, resource_id: str = None, resource_type: str = None, resource_discriminator: str = None) -> List[Dict[str, Any]]:
        """
        Finds assignments associated with a specific resource.

        :param resource_id: The UUID of the resource to filter by (optional).
        :param resource_type: The type of the resource to filter by (optional).
        :param resource_discriminator: The discriminator for the resource to filter by (optional).
        :return: A list of dictionaries containing the matching assignments.
        """
        params = {
            "resourceId": resource_id,
            "resourceType": resource_type,
            "resourceDiscriminator": resource_discriminator
        }
        
        if resource_id and not self._uuid_validation(resource_id):
            raise ValueError("resourceId must be a valid UUID")

        response = self._get(url=f"{self.__base_api}/forResource", params=params)
        return self._handle_response(response)

    def get_assignments_for_asset(self, asset_id: str) -> Dict[str, Any]:
        """
        Retrieves assignments associated with a specific asset.

        :param asset_id: The UUID of the asset to retrieve assignments for.
        :return: A dictionary containing the assignments for the asset.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/asset/{asset_id}")
        return self._handle_response(response)

    def get_assignments_for_asset_type(self, asset_type_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves assignments associated with a specific asset type.

        :param asset_type_id: The UUID of the asset type to retrieve assignments for.
        :return: A list of dictionaries containing the assignments for the asset type.
        """
        if not self._uuid_validation(asset_type_id):
            raise ValueError("asset_type_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/assetType/{asset_type_id}")
        return self._handle_response(response)

    def get_available_asset_types_for_domain(self, domain_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves available asset types for a specific domain.

        :param domain_id: The UUID of the domain to retrieve asset types for.
        :return: A list of dictionaries containing the available asset types for the domain.
        """
        if not self._uuid_validation(domain_id):
            raise ValueError("domain_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/domain/{domain_id}/assetTypes")
        return self._handle_response(response)

    def get_available_attribute_types_for_asset(self, asset_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves available attribute types for a specific asset.

        :param asset_id: The UUID of the asset to retrieve attribute types for.
        :return: A list of dictionaries containing the available attribute types for the asset.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/asset/{asset_id}/attributeTypes")
        return self._handle_response(response)

    def get_available_complex_relation_types_for_asset(self, asset_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves available complex relation types for a specific asset.

        :param asset_id: The UUID of the asset to retrieve complex relation types for.
        :return: A list of dictionaries containing the available complex relation types for the asset.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/asset/{asset_id}/complexRelationTypes")
        return self._handle_response(response)

    def get_available_relation_types_for_asset(self, asset_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves available relation types for a specific asset.

        :param asset_id: The UUID of the asset to retrieve relation types for.
        :return: A list of dictionaries containing the available relation types for the asset.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/asset/{asset_id}/relationTypes")
        return self._handle_response(response)
