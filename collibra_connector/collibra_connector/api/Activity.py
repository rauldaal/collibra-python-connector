import uuid
from typing import List, Optional
from .Base import BaseAPI


class Activity(BaseAPI):
    """
    Activity API implementation.
    Wraps the /activities endpoint of Collibra DGC.
    """

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/activities"

    def find_activities(
        self,
        activity_type: Optional[str] = None,
        call_count_enabled: bool = False,
        call_id: Optional[str] = None,
        categories: Optional[List[str]] = None,
        context_id: Optional[str] = None,
        count_limit: int = -1,
        end_date: Optional[int] = None,
        involved_people_ids: Optional[List[str]] = None,
        involved_role_ids: Optional[List[str]] = None,
        limit: int = 0,
        offset: int = 0,
        performed_by_role_ids: Optional[List[str]] = None,
        performed_by_user_id: Optional[str] = None,
        resource_discriminators: Optional[List[str]] = None,
        resource_types: Optional[List[str]] = None,
        start_date: Optional[int] = None,
        task_id: Optional[str] = None,
    ):
        """
        Returns activities matching the given search criteria.
        Only parameters that are specified in this request and have not null values are used for filtering.
        All other parameters are ignored. The returned activities satisfy all constraints that are specified
        in this search criteria. By default a result containing 100 activities is returned.

        :param activity_type: The type of the activity. Allowed values: ADD, UPDATE, DELETE
        :param call_count_enabled: Flag to indicate if the number of calls standing behind the activity
                                   should be returned or not. Default: False
        :param call_id: The ID of the call.
        :param categories: The set of the categories of activities that should be searched.
                          Allowed values: ATTRIBUTE, ATTACHMENT, RELATION, COMMENT, RATING, STATUS,
                          WORKFLOW, RESPONSIBILITY, USER, USER_GROUP, ROLE, TAGS, OTHERS, USER_PASSWORD,
                          VIEW_PERMISSION
        :param context_id: The ID of the context which the activities should be searched for.
        :param count_limit: Parameter not used in the context of activities, for performance reasons
                           they are not counted. Default: -1
        :param end_date: The end date of the searched activities, expressed as a Unix timestamp in milliseconds.
        :param involved_people_ids: The list of IDs of the people that should be involved in searched activities.
        :param involved_role_ids: The list of IDs of the roles that should be involved in searched activities.
        :param limit: The maximum number of results to retrieve. If not set (limit = 0), the default limit
                     will be used. The maximum allowed limit is 1000. Default: 0
        :param offset: The first result to retrieve. If not set (offset = 0), results will be retrieved
                      starting from row 0. Default: 0
        :param performed_by_role_ids: The list of IDs of the roles assigned to users who performed
                                     searched activities.
        :param performed_by_user_id: The ID of the user who performed searched activities.
        :param resource_discriminators: The set of the resource discriminators that searched activities
                                       refer to, i.e. [Community, Asset, Domain, Attribute, Relation,
                                       WorkflowInstance].
        :param resource_types: The set of the resource types that searched activities refer to.
                              (deprecated - use resource_discriminators instead)
        :param start_date: The start date of the searched activities, expressed as a Unix timestamp
                          in milliseconds.
        :param task_id: The ID of the task which contains the basic find activities request.
        :return: The paged response with found Activity information.
        """
        # Validate activity_type if provided
        if activity_type is not None:
            if not isinstance(activity_type, str):
                raise ValueError("activity_type must be a string")
            if activity_type not in ["ADD", "UPDATE", "DELETE"]:
                raise ValueError("activity_type must be one of: ADD, UPDATE, DELETE")

        # Validate call_count_enabled
        if not isinstance(call_count_enabled, bool):
            raise ValueError("call_count_enabled must be a boolean")

        # Validate categories if provided
        if categories is not None:
            if not isinstance(categories, list):
                raise ValueError("categories must be a list")
            valid_categories = [
                "ATTRIBUTE", "ATTACHMENT", "RELATION", "COMMENT", "RATING", "STATUS",
                "WORKFLOW", "RESPONSIBILITY", "USER", "USER_GROUP", "ROLE", "TAGS",
                "OTHERS", "USER_PASSWORD", "VIEW_PERMISSION"
            ]
            for category in categories:
                if category not in valid_categories:
                    raise ValueError(f"Invalid category: {category}. Must be one of: {', '.join(valid_categories)}")

        # Validate context_id if provided
        if context_id is not None:
            if not isinstance(context_id, str):
                raise ValueError("context_id must be a string")
            try:
                uuid.UUID(context_id)
            except ValueError as exc:
                raise ValueError("context_id must be a valid UUID") from exc

        # Validate dates if provided
        if end_date is not None:
            if not isinstance(end_date, int):
                raise ValueError("end_date must be an integer (Unix timestamp in milliseconds)")
            if end_date < 0:
                raise ValueError("end_date must be a positive integer")

        if start_date is not None:
            if not isinstance(start_date, int):
                raise ValueError("start_date must be an integer (Unix timestamp in milliseconds)")
            if start_date < 0:
                raise ValueError("start_date must be a positive integer")

        # Validate involved_people_ids if provided
        if involved_people_ids is not None:
            if not isinstance(involved_people_ids, list):
                raise ValueError("involved_people_ids must be a list")
            for i, people_id in enumerate(involved_people_ids):
                if not isinstance(people_id, str):
                    raise ValueError(f"involved_people_ids[{i}] must be a string")
                try:
                    uuid.UUID(people_id)
                except ValueError as exc:
                    raise ValueError(f"involved_people_ids[{i}] must be a valid UUID") from exc

        # Validate involved_role_ids if provided
        if involved_role_ids is not None:
            if not isinstance(involved_role_ids, list):
                raise ValueError("involved_role_ids must be a list")
            for i, role_id in enumerate(involved_role_ids):
                if not isinstance(role_id, str):
                    raise ValueError(f"involved_role_ids[{i}] must be a string")
                try:
                    uuid.UUID(role_id)
                except ValueError as exc:
                    raise ValueError(f"involved_role_ids[{i}] must be a valid UUID") from exc

        # Validate limit
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        # Validate offset
        if offset < 0:
            raise ValueError("offset must be a non-negative integer")

        # Validate performed_by_role_ids if provided
        if performed_by_role_ids is not None:
            if not isinstance(performed_by_role_ids, list):
                raise ValueError("performed_by_role_ids must be a list")
            for i, role_id in enumerate(performed_by_role_ids):
                if not isinstance(role_id, str):
                    raise ValueError(f"performed_by_role_ids[{i}] must be a string")
                try:
                    uuid.UUID(role_id)
                except ValueError as exc:
                    raise ValueError(f"performed_by_role_ids[{i}] must be a valid UUID") from exc

        # Validate performed_by_user_id if provided
        if performed_by_user_id is not None:
            if not isinstance(performed_by_user_id, str):
                raise ValueError("performed_by_user_id must be a string")
            try:
                uuid.UUID(performed_by_user_id)
            except ValueError as exc:
                raise ValueError("performed_by_user_id must be a valid UUID") from exc

        # Validate resource_discriminators if provided
        if resource_discriminators is not None:
            if not isinstance(resource_discriminators, list):
                raise ValueError("resource_discriminators must be a list")

        # Validate resource_types if provided
        if resource_types is not None:
            if not isinstance(resource_types, list):
                raise ValueError("resource_types must be a list")

        # Validate task_id if provided
        if task_id is not None:
            if not isinstance(task_id, str):
                raise ValueError("task_id must be a string")
            try:
                uuid.UUID(task_id)
            except ValueError as exc:
                raise ValueError("task_id must be a valid UUID") from exc

        # Build parameters - only include non-None and non-default values
        params = {}

        if activity_type is not None:
            params["activityType"] = activity_type
        if call_count_enabled is not False:  # Only add if True
            params["callCountEnabled"] = call_count_enabled
        if call_id is not None:
            params["callId"] = call_id
        if categories is not None:
            params["categories"] = categories
        if context_id is not None:
            params["contextId"] = context_id
        if count_limit != -1:  # Only add if different from default
            params["countLimit"] = count_limit
        if end_date is not None:
            params["endDate"] = end_date
        if involved_people_ids is not None:
            params["involvedPeopleIds"] = involved_people_ids
        if involved_role_ids is not None:
            params["involvedRoleIds"] = involved_role_ids
        if limit != 0:  # Only add if different from default
            params["limit"] = limit
        if offset != 0:  # Only add if different from default
            params["offset"] = offset
        if performed_by_role_ids is not None:
            params["performedByRoleIds"] = performed_by_role_ids
        if performed_by_user_id is not None:
            params["performedByUserId"] = performed_by_user_id
        if resource_discriminators is not None:
            params["resourceDiscriminators"] = resource_discriminators
        if resource_types is not None:
            params["resourceTypes"] = resource_types
        if start_date is not None:
            params["startDate"] = start_date
        if task_id is not None:
            params["taskId"] = task_id

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)
