import uuid
from .Base import BaseAPI


class Mapping(BaseAPI):
    """API class for mapping operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/mappings"

    def find_mappings(
        self,
        count_limit: int = -1,
        external_entity_id: str = None,
        external_system_id: str = None,
        limit: int = 0,
        mapped_resource_id: str = None,
        mapped_resource_type: str = None,
        offset: int = 0,
        sync_action: str = None,
    ):
        """
        Returns mappings matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param external_entity_id: External entity ID to filter by.
        :param external_system_id: External system ID to filter by.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param mapped_resource_id: UUID of the mapped resource to filter by.
        :param mapped_resource_type: Type of the mapped resource to filter by.
        :param offset: First result to retrieve.
        :param sync_action: Sync action to filter by.
        :return: List of mappings.
        """
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if mapped_resource_id is not None:
            try:
                uuid.UUID(mapped_resource_id)
            except ValueError as exc:
                raise ValueError("mapped_resource_id must be a valid UUID") from exc

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if external_entity_id is not None:
            params["externalEntityId"] = external_entity_id
        if external_system_id is not None:
            params["externalSystemId"] = external_system_id
        if limit != 0:
            params["limit"] = limit
        if mapped_resource_id is not None:
            params["mappedResourceId"] = mapped_resource_id
        if mapped_resource_type is not None:
            params["mappedResourceType"] = mapped_resource_type
        if offset != 0:
            params["offset"] = offset
        if sync_action is not None:
            params["syncAction"] = sync_action

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_mapping(self, mapping_id: str):
        """
        Returns the mapping identified by the given UUID.
        :param mapping_id: The UUID of the mapping.
        :return: Mapping details.
        """
        if not mapping_id:
            raise ValueError("mapping_id is required")
        try:
            uuid.UUID(mapping_id)
        except ValueError as exc:
            raise ValueError("mapping_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{mapping_id}")
        return self._handle_response(response)

    def get_mapping_by_external_entity(self, external_system_id: str, external_entity_id: str):
        """
        Returns the mapping identified by its external IDs.
        :param external_system_id: The external system ID.
        :param external_entity_id: The external entity ID.
        :return: Mapping details.
        """
        if not external_system_id or not external_entity_id:
            raise ValueError("external_system_id and external_entity_id are required")

        response = self._get(
            url=f"{self.__base_api}/externalSystem/{external_system_id}/externalEntity/{external_entity_id}"
        )
        return self._handle_response(response)

    def get_mapping_by_mapped_resource(self, external_system_id: str, mapped_resource_id: str):
        """
        Returns the mapping identified by external system ID and mapped resource ID.
        :param external_system_id: The external system ID.
        :param mapped_resource_id: The UUID of the mapped resource.
        :return: Mapping details.
        """
        if not external_system_id:
            raise ValueError("external_system_id is required")
        if not mapped_resource_id:
            raise ValueError("mapped_resource_id is required")
        try:
            uuid.UUID(mapped_resource_id)
        except ValueError as exc:
            raise ValueError("mapped_resource_id must be a valid UUID") from exc

        response = self._get(
            url=f"{self.__base_api}/externalSystem/{external_system_id}/mappedResource/{mapped_resource_id}"
        )
        return self._handle_response(response)

    def add_mapping(self, external_system_id: str, external_entity_id: str,
                    mapped_resource_id: str, mapped_resource_type: str,
                    sync_action: str = None, external_entity_url: str = None):
        """
        Adds a new mapping.
        :param external_system_id: The external system ID (required).
        :param external_entity_id: The external entity ID (required).
        :param mapped_resource_id: The UUID of the mapped Collibra resource (required).
        :param mapped_resource_type: The type of the mapped resource (required).
        :param sync_action: Optional sync action.
        :param external_entity_url: Optional URL of the external entity.
        :return: Created mapping details.
        """
        if not all([external_system_id, external_entity_id, mapped_resource_id, mapped_resource_type]):
            raise ValueError("external_system_id, external_entity_id, mapped_resource_id, "
                             "and mapped_resource_type are required")
        try:
            uuid.UUID(mapped_resource_id)
        except ValueError as exc:
            raise ValueError("mapped_resource_id must be a valid UUID") from exc

        data = {
            "externalSystemId": external_system_id,
            "externalEntityId": external_entity_id,
            "mappedResourceId": mapped_resource_id,
            "mappedResourceType": mapped_resource_type,
        }
        if sync_action is not None:
            data["syncAction"] = sync_action
        if external_entity_url is not None:
            data["externalEntityUrl"] = external_entity_url

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_mappings(self, mappings: list):
        """
        Adds multiple mappings in one go.
        :param mappings: List of mapping objects.
        :return: Created mappings.
        """
        if not mappings or not isinstance(mappings, list):
            raise ValueError("mappings must be a non-empty list")
        response = self._post(url=f"{self.__base_api}/bulk", data={"mappings": mappings})
        return self._handle_response(response)

    def change_mapping(self, mapping_id: str, external_entity_id: str = None,
                       sync_action: str = None, external_entity_url: str = None):
        """
        Changes the mapping identified by its UUID.
        :param mapping_id: The UUID of the mapping.
        :param external_entity_id: Optional new external entity ID.
        :param sync_action: Optional new sync action.
        :param external_entity_url: Optional new external entity URL.
        :return: Updated mapping details.
        """
        if not mapping_id:
            raise ValueError("mapping_id is required")
        try:
            uuid.UUID(mapping_id)
        except ValueError as exc:
            raise ValueError("mapping_id must be a valid UUID") from exc

        data = {}
        if external_entity_id is not None:
            data["externalEntityId"] = external_entity_id
        if sync_action is not None:
            data["syncAction"] = sync_action
        if external_entity_url is not None:
            data["externalEntityUrl"] = external_entity_url

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{mapping_id}", data=data)
        return self._handle_response(response)

    def change_mappings(self, mappings: list):
        """
        Changes multiple mappings identified by their IDs.
        :param mappings: List of mapping change objects (must include id).
        :return: Updated mappings.
        """
        if not mappings or not isinstance(mappings, list):
            raise ValueError("mappings must be a non-empty list")
        response = self._patch(url=f"{self.__base_api}/bulk", data={"mappings": mappings})
        return self._handle_response(response)

    def change_mapping_by_external_entity(self, external_system_id: str, external_entity_id: str,
                                           sync_action: str = None, external_entity_url: str = None):
        """
        Changes the mapping identified by its external IDs.
        :param external_system_id: The external system ID.
        :param external_entity_id: The external entity ID.
        :param sync_action: Optional new sync action.
        :param external_entity_url: Optional new external entity URL.
        :return: Updated mapping details.
        """
        if not external_system_id or not external_entity_id:
            raise ValueError("external_system_id and external_entity_id are required")

        data = {}
        if sync_action is not None:
            data["syncAction"] = sync_action
        if external_entity_url is not None:
            data["externalEntityUrl"] = external_entity_url

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(
            url=f"{self.__base_api}/externalSystem/{external_system_id}/externalEntity/{external_entity_id}",
            data=data
        )
        return self._handle_response(response)

    def change_mappings_by_external_entities(self, mappings: list):
        """
        Changes mappings identified by their external IDs.
        :param mappings: List of mapping objects with externalSystemId and externalEntityId.
        :return: Updated mappings.
        """
        if not mappings or not isinstance(mappings, list):
            raise ValueError("mappings must be a non-empty list")
        response = self._patch(
            url=f"{self.__base_api}/externalSystem/externalEntity/bulk",
            data={"mappings": mappings}
        )
        return self._handle_response(response)

    def change_mappings_by_mapped_resources(self, mappings: list):
        """
        Changes mappings identified by their external system IDs and mapped resource IDs.
        :param mappings: List of mapping objects with externalSystemId and mappedResourceId.
        :return: Updated mappings.
        """
        if not mappings or not isinstance(mappings, list):
            raise ValueError("mappings must be a non-empty list")
        response = self._patch(
            url=f"{self.__base_api}/externalSystem/mappedResource/bulk",
            data={"mappings": mappings}
        )
        return self._handle_response(response)

    def remove_mapping(self, mapping_id: str):
        """
        Removes the mapping identified by its UUID.
        :param mapping_id: The UUID of the mapping.
        :return: None
        """
        if not mapping_id:
            raise ValueError("mapping_id is required")
        try:
            uuid.UUID(mapping_id)
        except ValueError as exc:
            raise ValueError("mapping_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{mapping_id}")
        return self._handle_response(response)

    def remove_mapping_by_external_entity(self, external_system_id: str, external_entity_id: str):
        """
        Removes the mapping identified by its external IDs.
        :param external_system_id: The external system ID.
        :param external_entity_id: The external entity ID.
        :return: None
        """
        if not external_system_id or not external_entity_id:
            raise ValueError("external_system_id and external_entity_id are required")

        response = self._delete(
            url=f"{self.__base_api}/externalSystem/{external_system_id}/externalEntity/{external_entity_id}"
        )
        return self._handle_response(response)

    def remove_mapping_by_mapped_resource(self, external_system_id: str, mapped_resource_id: str):
        """
        Removes the mapping identified by external system ID and mapped resource ID.
        :param external_system_id: The external system ID.
        :param mapped_resource_id: The UUID of the mapped resource.
        :return: None
        """
        if not external_system_id or not mapped_resource_id:
            raise ValueError("external_system_id and mapped_resource_id are required")

        response = self._delete(
            url=f"{self.__base_api}/externalSystem/{external_system_id}/mappedResource/{mapped_resource_id}"
        )
        return self._handle_response(response)

    def remove_mappings_in_job(self, mapping_ids: list):
        """
        Removes multiple mappings in a background job.
        :param mapping_ids: List of mapping UUIDs to remove.
        :return: Job details.
        """
        if not mapping_ids or not isinstance(mapping_ids, list):
            raise ValueError("mapping_ids must be a non-empty list")
        response = self._post(
            url=f"{self.__base_api}/removalJobs",
            data={"mappingIds": mapping_ids}
        )
        return self._handle_response(response)

    def remove_mappings_by_external_system_in_job(self, external_system_id: str):
        """
        Removes all mappings for the given external system ID in a background job.
        :param external_system_id: The external system ID.
        :return: Job details.
        """
        if not external_system_id:
            raise ValueError("external_system_id is required")

        data = {"externalSystemId": external_system_id}
        response = self._post(
            url=f"{self.__base_api}/externalSystem/{external_system_id}/removalJobs",
            data=data
        )
        return self._handle_response(response)
