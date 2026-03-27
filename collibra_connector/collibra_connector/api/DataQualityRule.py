import uuid
from .Base import BaseAPI


class DataQualityRule(BaseAPI):
    """API class for data quality rule operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/dataQualityRules"

    def find_data_quality_rules(
        self,
        count_limit: int = -1,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
        sort_field: str = "NAME",
        sort_order: str = "ASC",
    ):
        """
        Returns data quality rules matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :return: List of data quality rules.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset
        if sort_field != "NAME":
            params["sortField"] = sort_field
        if sort_order != "ASC":
            params["sortOrder"] = sort_order

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_data_quality_rule(self, data_quality_rule_id: str):
        """
        Returns the DataQualityRule identified by the given UUID.
        :param data_quality_rule_id: The UUID of the data quality rule.
        :return: Data quality rule details.
        """
        if not data_quality_rule_id:
            raise ValueError("data_quality_rule_id is required")
        try:
            uuid.UUID(data_quality_rule_id)
        except ValueError as exc:
            raise ValueError("data_quality_rule_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{data_quality_rule_id}")
        return self._handle_response(response)

    def add_data_quality_rule(self, name: str, resource_id: str, description: str = None,
                               threshold: float = None, result_type: str = None):
        """
        Adds a new data quality rule.
        :param name: The name of the rule (required).
        :param resource_id: The UUID of the resource this rule applies to (required).
        :param description: Optional description.
        :param threshold: Optional threshold value.
        :param result_type: Optional result type.
        :return: Created data quality rule details.
        """
        if not name:
            raise ValueError("name is required")
        if not resource_id:
            raise ValueError("resource_id is required")
        try:
            uuid.UUID(resource_id)
        except ValueError as exc:
            raise ValueError("resource_id must be a valid UUID") from exc

        data = {"name": name, "resourceId": resource_id}
        if description is not None:
            data["description"] = description
        if threshold is not None:
            data["threshold"] = threshold
        if result_type is not None:
            data["resultType"] = result_type

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def change_data_quality_rule(self, data_quality_rule_id: str, name: str = None,
                                  description: str = None, threshold: float = None,
                                  result_type: str = None):
        """
        Changes the data quality rule with the given ID.
        :param data_quality_rule_id: The UUID of the data quality rule to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :param threshold: Optional new threshold.
        :param result_type: Optional new result type.
        :return: Updated data quality rule details.
        """
        if not data_quality_rule_id:
            raise ValueError("data_quality_rule_id is required")
        try:
            uuid.UUID(data_quality_rule_id)
        except ValueError as exc:
            raise ValueError("data_quality_rule_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if threshold is not None:
            data["threshold"] = threshold
        if result_type is not None:
            data["resultType"] = result_type

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{data_quality_rule_id}", data=data)
        return self._handle_response(response)

    def remove_data_quality_rule(self, data_quality_rule_id: str):
        """
        Removes the DataQualityRule identified by the given UUID.
        :param data_quality_rule_id: The UUID of the data quality rule.
        :return: None
        """
        if not data_quality_rule_id:
            raise ValueError("data_quality_rule_id is required")
        try:
            uuid.UUID(data_quality_rule_id)
        except ValueError as exc:
            raise ValueError("data_quality_rule_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{data_quality_rule_id}")
        return self._handle_response(response)
