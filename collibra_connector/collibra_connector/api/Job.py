import uuid
from .Base import BaseAPI


class Job(BaseAPI):
    """API class for job operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/jobs"

    def find_jobs(
        self,
        count_limit: int = -1,
        created_by: str = None,
        limit: int = 0,
        max_visibility: str = None,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
        results: list = None,
        sort_field: str = "CREATED_ON",
        sort_order: str = "DESC",
        states: list = None,
        types: list = None,
        visible: bool = None,
    ):
        """
        Returns jobs matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param created_by: UUID of the user who created the job.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param max_visibility: Maximum visibility level filter.
        :param name: Job name to search for.
        :param name_match_mode: Name matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :param results: List of job result statuses to filter by (e.g., SUCCESS, ERROR).
        :param sort_field: Field to sort by. Default: CREATED_ON.
        :param sort_order: Sort order (ASC or DESC). Default: DESC.
        :param states: List of job states to filter by (e.g., RUNNING, COMPLETED).
        :param types: List of job types to filter by.
        :param visible: Whether to filter by visibility.
        :return: List of jobs.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if created_by is not None:
            try:
                uuid.UUID(created_by)
            except ValueError as exc:
                raise ValueError("created_by must be a valid UUID") from exc

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if created_by is not None:
            params["createdBy"] = created_by
        if limit != 0:
            params["limit"] = limit
        if max_visibility is not None:
            params["maxVisibility"] = max_visibility
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset
        if results is not None:
            params["results"] = results
        if sort_field != "CREATED_ON":
            params["sortField"] = sort_field
        if sort_order != "DESC":
            params["sortOrder"] = sort_order
        if states is not None:
            params["states"] = states
        if types is not None:
            params["types"] = types
        if visible is not None:
            params["visible"] = visible

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_job(self, job_id: str):
        """
        Returns the Job identified by the given UUID.
        :param job_id: The UUID of the job.
        :return: Job details.
        """
        if not job_id:
            raise ValueError("job_id is required")
        try:
            uuid.UUID(job_id)
        except ValueError as exc:
            raise ValueError("job_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{job_id}")
        return self._handle_response(response)

    def cancel_job(self, job_id: str):
        """
        Cancels the job identified by the given UUID.
        :param job_id: The UUID of the job to cancel.
        :return: Cancelled job details.
        """
        if not job_id:
            raise ValueError("job_id is required")
        try:
            uuid.UUID(job_id)
        except ValueError as exc:
            raise ValueError("job_id must be a valid UUID") from exc

        data = {"jobId": job_id}
        response = self._post(url=f"{self.__base_api}/{job_id}/canceled", data=data)
        return self._handle_response(response)

    def wait_for_job(self, job_id: str, poll_interval: float = 2.0, timeout: float = 300.0):
        """
        Polls the job until it completes or times out.
        :param job_id: The UUID of the job to wait for.
        :param poll_interval: Seconds between each poll. Default: 2.0.
        :param timeout: Maximum seconds to wait. Default: 300.0.
        :return: Final job details.
        :raises TimeoutError: If the job doesn't complete within the timeout.
        """
        import time

        if not job_id:
            raise ValueError("job_id is required")

        terminal_states = {"COMPLETED", "ERROR", "FAILED", "CANCELLED", "CANCELED"}
        elapsed = 0.0

        while elapsed < timeout:
            job = self.get_job(job_id)
            state = job.get("state", "")
            if state.upper() in terminal_states:
                return job
            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")
