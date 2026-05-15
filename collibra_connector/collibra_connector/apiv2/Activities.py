import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Activities(BaseAPI):
    """API class for activity operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/activities"

    def get_activities(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        activity_type: str = None,
        call_id: str = None,
        categories: List[str] = None,
        context_id: str = None,
        start_date: int = None,
        end_date: int = None,
        involved_people_ids: List[str] = None,
        involved_role_ids: List[str] = None,
        performed_by_user_id: str = None,
        performed_by_role_ids: List[str] = None,
        resource_discriminators: List[str] = None,
        resource_types: List[str] = None,
        task_id: str = None,
        call_count_enabled: bool = False
    ) -> Dict[str, Any]:
        """
        Retrieves activities based on the specified search criteria.

        :param offset: The starting point for the results (default is 0).
        :param limit: The maximum number of results to return (default is 0, which means no limit).
        :param count_limit: The maximum number of activities to count (default is -1, which means no limit).
        :param activity_type: The type of activity to filter by (optional).
        :param call_id: The unique identifier for the call (optional).
        :param categories: A list of categories to filter activities by (optional).
        :param context_id: The UUID of the context to filter activities by (optional).
        :param start_date: The start date (in milliseconds since epoch) to filter activities (optional).
        :param end_date: The end date (in milliseconds since epoch) to filter activities (optional).
        :param involved_people_ids: A list of UUIDs of people involved in the activities (optional).
        :param involved_role_ids: A list of UUIDs of roles involved in the activities (optional).
        :param performed_by_user_id: The UUID of the user who performed the activities (optional).
        :param performed_by_role_ids: A list of UUIDs of roles that performed the activities (optional).
        :param resource_discriminators: A list of resource discriminators to filter activities (optional).
        :param resource_types: A list of resource types to filter activities (optional).
        :param task_id: The UUID of the task associated with the activities (optional).
        :param call_count_enabled: Whether to enable call count in the response (default is False).
        :return: A dictionary containing the activities that match the search criteria.
        :raises ValueError: If any provided UUID is invalid.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "activityType": activity_type,
            "callId": call_id,
            "categories": categories,
            "contextId": context_id,
            "startDate": start_date,
            "endDate": end_date,
            "involvedPeopleIds": involved_people_ids,
            "involvedRoleIds": involved_role_ids,
            "performedByUserId": performed_by_user_id,
            "performedByRoleIds": performed_by_role_ids,
            "resourceDiscriminators": resource_discriminators,
            "resourceTypes": resource_types,
            "taskId": task_id,
            "callCountEnabled": call_count_enabled
        }
        
        # UUID validation for single ID params
        if context_id and not self._uuid_validation(context_id):
            raise ValueError("contextId must be a valid UUID")
        if performed_by_user_id and not self._uuid_validation(performed_by_user_id):
            raise ValueError("performedByUserId must be a valid UUID")
        if task_id and not self._uuid_validation(task_id):
            raise ValueError("taskId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)
