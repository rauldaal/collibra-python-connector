import uuid
from .Base import BaseAPI


class Validations(BaseAPI):
    """API class for validation operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/validation"

    def find_validation_results(
        self,
        asset_id: str = None,
        count_limit: int = -1,
        job_id: str = None,
        limit: int = 0,
        most_recent_execution: bool = None,
        most_recent_job: bool = None,
        offset: int = 0,
        result: str = None,
        validation_rule_id: str = None,
    ):
        """
        Returns validation results matching the given search criteria.
        :param asset_id: UUID of the asset to filter by.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param job_id: UUID of the job to filter by.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param most_recent_execution: Whether to return only the most recent execution results.
        :param most_recent_job: Whether to return only results from the most recent job.
        :param offset: First result to retrieve.
        :param result: Validation result to filter by (e.g., SUCCESS, ERROR).
        :param validation_rule_id: UUID of the validation rule to filter by.
        :return: List of validation results.
        """
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if asset_id is not None:
            try:
                uuid.UUID(asset_id)
            except ValueError as exc:
                raise ValueError("asset_id must be a valid UUID") from exc

        if job_id is not None:
            try:
                uuid.UUID(job_id)
            except ValueError as exc:
                raise ValueError("job_id must be a valid UUID") from exc

        if validation_rule_id is not None:
            try:
                uuid.UUID(validation_rule_id)
            except ValueError as exc:
                raise ValueError("validation_rule_id must be a valid UUID") from exc

        params = {}
        if asset_id is not None:
            params["assetId"] = asset_id
        if count_limit != -1:
            params["countLimit"] = count_limit
        if job_id is not None:
            params["jobId"] = job_id
        if limit != 0:
            params["limit"] = limit
        if most_recent_execution is not None:
            params["mostRecentExecution"] = most_recent_execution
        if most_recent_job is not None:
            params["mostRecentJob"] = most_recent_job
        if offset != 0:
            params["offset"] = offset
        if result is not None:
            params["result"] = result
        if validation_rule_id is not None:
            params["validationRuleId"] = validation_rule_id

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def validate(self, asset_id: str):
        """
        Validates a single asset.
        :param asset_id: The UUID of the asset to validate.
        :return: Validation results for the asset.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        data = {"assetId": asset_id}
        response = self._post(url=f"{self.__base_api}/{asset_id}", data=data)
        return self._handle_response(response)

    def validate_in_job(self, asset_ids: list, send_notification: bool = False):
        """
        Validates multiple assets in a background job.
        :param asset_ids: List of asset UUIDs to validate.
        :param send_notification: Whether to send a notification upon job completion.
        :return: Job details.
        """
        if not asset_ids or not isinstance(asset_ids, list):
            raise ValueError("asset_ids must be a non-empty list")

        for asset_id in asset_ids:
            try:
                uuid.UUID(asset_id)
            except ValueError as exc:
                raise ValueError(f"'{asset_id}' is not a valid UUID") from exc

        data = {
            "assetIds": asset_ids,
            "sendNotification": send_notification
        }
        response = self._post(url=f"{self.__base_api}/bulk", data=data)
        return self._handle_response(response)
