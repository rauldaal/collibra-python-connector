import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class DataQualityRules(BaseAPI):
    """API class for data quality rule operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/dataQualityRules"

    def find_data_quality_rules(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        sort_field: str = "NAME",
        sort_order: str = "ASC"
    ) -> Dict[str, Any]:
        """
        Searches for data quality rules based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param name: The name of the data quality rule to search for (optional).
        :param name_match_mode: The mode for matching the name (default: "ANYWHERE").
        :param sort_field: The field to sort the results by (default: "NAME").
        :param sort_order: The order to sort the results in (default: "ASC").
        :return: A dictionary containing the matching data quality rules.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode,
            "sortField": sort_field,
            "sortOrder": sort_order
        }
        
        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_data_quality_rule(
        self,
        name: str,
        id: str = None,
        description: str = None,
        data_quality_rule_type: str = None,
        categorization_relation_type_id: str = None,
        data_quality_metrics: list = None,
        relation_trace_entries: list = None
    ) -> Dict[str, Any]:
        """
        Creates a new data quality rule with the specified parameters.

        :param name: The name of the data quality rule (required).
        :param id: The unique identifier of the data quality rule (optional).
        :param description: A description of the data quality rule (optional).
        :param data_quality_rule_type: The type of the data quality rule (optional).
        :param categorization_relation_type_id: The relation type ID for categorization (optional).
        :param data_quality_metrics: A list of metrics associated with the rule (optional).
        :param relation_trace_entries: A list of trace entries for relations (optional).
        :return: A dictionary containing the details of the created data quality rule.
        """
        if not name:
            raise ValueError("name is required")

        data = {
            "name": name,
            "id": id,
            "description": description,
            "dataQualityRuleType": data_quality_rule_type,
            "categorizationRelationTypeId": categorization_relation_type_id,
            "dataQualityMetrics": data_quality_metrics,
            "relationTraceEntries": relation_trace_entries
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_data_quality_rule(self, data_quality_rule_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a data quality rule by its ID.

        :param data_quality_rule_id: The unique identifier of the data quality rule to retrieve.
        :return: A dictionary containing the details of the data quality rule.
        """
        if not self._uuid_validation(data_quality_rule_id):
            raise ValueError("data_quality_rule_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{data_quality_rule_id}")
        return self._handle_response(response)

    def remove_data_quality_rule(self, data_quality_rule_id: str) -> None:
        """
        Deletes a data quality rule identified by its ID.

        :param data_quality_rule_id: The unique identifier of the data quality rule to delete.
        """
        if not self._uuid_validation(data_quality_rule_id):
            raise ValueError("data_quality_rule_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{data_quality_rule_id}")
        return self._handle_response(response)

    def change_data_quality_rule(
        self,
        data_quality_rule_id: str,
        name: str = None,
        description: str = None,
        data_quality_rule_type: str = None,
        categorization_relation_type_id: str = None,
        data_quality_metrics: list = None,
        relation_trace_entries: list = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a data quality rule identified by its ID.

        :param data_quality_rule_id: The unique identifier of the data quality rule to update.
        :param name: The new name of the data quality rule (optional).
        :param description: The new description of the data quality rule (optional).
        :param data_quality_rule_type: The new type of the data quality rule (optional).
        :param categorization_relation_type_id: The new relation type ID for categorization (optional).
        :param data_quality_metrics: The new list of metrics associated with the rule (optional).
        :param relation_trace_entries: The new list of trace entries for relations (optional).
        :return: A dictionary containing the updated details of the data quality rule.
        """
        if not self._uuid_validation(data_quality_rule_id):
            raise ValueError("data_quality_rule_id must be a valid UUID")

        data = {
            "name": name,
            "description": description,
            "dataQualityRuleType": data_quality_rule_type,
            "categorizationRelationTypeId": categorization_relation_type_id,
            "dataQualityMetrics": data_quality_metrics,
            "relationTraceEntries": relation_trace_entries
        }

        response = self._patch(url=f"{self.__base_api}/{data_quality_rule_id}", data=data)
        return self._handle_response(response)
