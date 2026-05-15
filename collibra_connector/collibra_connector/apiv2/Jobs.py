import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Jobs(BaseAPI):
    """API class for job operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/jobs"

    def find_jobs(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        sort_field: str = "CREATED_ON",
        sort_order: str = "DESC",
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        created_by: str = None,
        states: List[str] = None,
        types: List[str] = None,
        results: List[str] = None,
        visible: bool = None,
        max_visibility: str = None
    ) -> Dict[str, Any]:
        """
        Searches for jobs based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param sort_field: The field to sort the results by (default: "CREATED_ON").
        :param sort_order: The order to sort the results in (default: "DESC").
        :param name: The name of the job to filter by (optional).
        :param name_match_mode: The matching mode for the name (default: "ANYWHERE").
        :param created_by: The ID of the user who created the job (optional).
        :param states: A list of states to filter jobs by (optional).
        :param types: A list of types to filter jobs by (optional).
        :param results: A list of results to filter jobs by (optional).
        :param visible: Whether to include only visible jobs (optional).
        :param max_visibility: The maximum visibility level of the jobs (optional).
        :return: A dictionary containing the matching jobs.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "sortField": sort_field,
            "sortOrder": sort_order,
            "name": name,
            "nameMatchMode": name_match_mode,
            "createdBy": created_by,
            "states": states,
            "types": types,
            "results": results,
            "visible": visible,
            "maxVisibility": max_visibility
        }
        
        if created_by and not self._uuid_validation(created_by):
            raise ValueError("createdBy must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a job by its ID.

        :param job_id: The unique identifier of the job to retrieve.
        :return: A dictionary containing the details of the job.
        """
        if not self._uuid_validation(job_id):
            raise ValueError("job_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{job_id}")
        return self._handle_response(response)

    def cancel_job(self, job_id: str, message: str = None) -> None:
        """
        Cancels a job by its ID.

        :param job_id: The unique identifier of the job to cancel.
        :param message: An optional message explaining the reason for cancellation.
        """
        if not job_id:
            raise ValueError("job_id is required")

        data = {"jobId": job_id}
        if message:
            data["message"] = message

        self._post(url=f"{self.__base_api}/jobs/cancel", data=data)

    def wait_for_job(self, job_id: str, poll_interval: float = 2.0, timeout: float = 300.0) -> Dict[str, Any]:
        """
        Waits for a job to complete by polling its status.

        :param job_id: The unique identifier of the job to wait for.
        :param poll_interval: The interval (in seconds) between status checks (default: 2.0).
        :param timeout: The maximum time (in seconds) to wait for the job to complete (default: 300.0).
        :return: A dictionary containing the details of the completed job.
        :raises TimeoutError: If the job does not complete within the specified timeout.
        """
        import time
        
        elapsed = 0.0
        terminal_states = {"COMPLETED", "ERROR", "FAILED", "CANCELLED", "CANCELED"}
        
        while elapsed < timeout:
            job = self.get_job(job_id)
            state = job.get("state", "")
            if state.upper() in terminal_states:
                return job
            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")
