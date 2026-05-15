from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Licenses(BaseAPI):
    """API class for license operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/license"

    def has_capabilities(self, names: List[str] = None, name: str = None) -> bool:
        """
        Checks if the license has specific capabilities.
        :param names: A list of capability names to check.
        :param name: A single capability name to check.
        :return: True if the license has the specified capabilities, False otherwise.
        """
        if not names and not name:
            raise ValueError("Either 'names' or 'name' must be provided")

        params = {}
        if names:
            params["names"] = names
        if name:
            params["name"] = name

        response = self._get(url=f"{self.__base_api}/licenses/hasCapabilities", params=params)
        return response.get("hasCapabilities", False)

    def change_mapping_by_external_entity(
        self,
        external_system_id: str,
        external_entity_id: str,
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Changes a mapping by external entity.
        :param external_system_id: The ID of the external system.
        :param external_entity_id: The ID of the external entity.
        :param updates: The updates to apply to the mapping.
        :return: Details of the updated mapping.
        """
        if not external_system_id or not external_entity_id:
            raise ValueError("Both external_system_id and external_entity_id are required")

        response = self._patch(
            url=f"{self.__base_api}/mappings/externalSystem/{external_system_id}/externalEntity/{external_entity_id}",
            data=updates
        )
        return self._handle_response(response)

    def get_mapping_by_mapped_resource(
        self,
        external_system_id: str,
        mapped_resource_id: str
    ) -> Dict[str, Any]:
        """
        Retrieves a mapping by mapped resource.
        :param external_system_id: The ID of the external system.
        :param mapped_resource_id: The ID of the mapped resource.
        :return: Details of the mapping.
        """
        if not external_system_id or not mapped_resource_id:
            raise ValueError("Both external_system_id and mapped_resource_id are required")

        response = self._get(
            url=f"{self.__base_api}/mappings/externalSystem/{external_system_id}/mappedResource/{mapped_resource_id}"
        )
        return self._handle_response(response)

    def remove_mapping_by_mapped_resource(
        self,
        external_system_id: str,
        mapped_resource_id: str
    ) -> None:
        """
        Removes a mapping by mapped resource.
        :param external_system_id: The ID of the external system.
        :param mapped_resource_id: The ID of the mapped resource.
        :return: None.
        """
        if not external_system_id or not mapped_resource_id:
            raise ValueError("Both external_system_id and mapped_resource_id are required")

        self._delete(
            url=f"{self.__base_api}/mappings/externalSystem/{external_system_id}/mappedResource/{mapped_resource_id}"
        )

    def change_mapping_by_mapped_resource(
        self,
        external_system_id: str,
        mapped_resource_id: str,
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Changes a mapping by mapped resource.
        :param external_system_id: The ID of the external system.
        :param mapped_resource_id: The ID of the mapped resource.
        :param updates: The updates to apply to the mapping.
        :return: Details of the updated mapping.
        """
        if not external_system_id or not mapped_resource_id:
            raise ValueError("Both external_system_id and mapped_resource_id are required")

        response = self._patch(
            url=f"{self.__base_api}/mappings/externalSystem/{external_system_id}/mappedResource/{mapped_resource_id}",
            data=updates
        )
        return self._handle_response(response)

    def change_mappings_by_external_entities(
        self,
        updates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Changes mappings by external entities in bulk.
        :param updates: A list of updates to apply to the mappings.
        :return: Details of the updated mappings.
        """
        if not updates or not isinstance(updates, list):
            raise ValueError("updates must be a non-empty list")

        response = self._patch(
            url=f"{self.__base_api}/mappings/externalSystem/externalEntity/bulk",
            data=updates
        )
        return self._handle_response(response)

    def change_mappings_by_mapped_resources(
        self,
        updates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Changes mappings by mapped resources in bulk.
        :param updates: A list of updates to apply to the mappings.
        :return: Details of the updated mappings.
        """
        if not updates or not isinstance(updates, list):
            raise ValueError("updates must be a non-empty list")

        response = self._patch(
            url=f"{self.__base_api}/mappings/externalSystem/mappedResource/bulk",
            data=updates
        )
        return self._handle_response(response)

    def remove_mappings_by_external_system_in_job(
        self,
        external_system_id: str,
        job_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Removes mappings by external system in a job.
        :param external_system_id: The ID of the external system.
        :param job_details: Details of the removal job.
        :return: Details of the job.
        """
        if not external_system_id:
            raise ValueError("external_system_id is required")

        response = self._post(
            url=f"{self.__base_api}/mappings/externalSystem/{external_system_id}/removalJobs",
            data=job_details
        )
        return self._handle_response(response)

    def add_mapping(
        self,
        external_system_id: str,
        external_entity_id: str,
        mapped_resource_id: str,
        mapped_resource_type: str,
        sync_action: str,
        external_entity_url: str = None,
        id: str = None,
        last_sync_date: str = None,
        description: str = None
    ) -> Dict[str, Any]:
        """
        Adds a new mapping.
        :param external_system_id: The ID of the external system.
        :param external_entity_id: The ID of the external entity.
        :param mapped_resource_id: The ID of the mapped resource.
        :param mapped_resource_type: The type of the mapped resource.
        :param sync_action: The synchronization action.
        :param external_entity_url: The URL of the external entity.
        :param id: The ID of the mapping.
        :param last_sync_date: The last synchronization date.
        :param description: A description of the mapping.
        :return: Details of the added mapping.
        """
        data = {
            "externalSystemId": external_system_id,
            "externalEntityId": external_entity_id,
            "mappedResourceId": mapped_resource_id,
            "mappedResourceType": mapped_resource_type,
            "syncAction": sync_action
        }

        if external_entity_url:
            data["externalEntityUrl"] = external_entity_url
        if id:
            data["id"] = id
        if last_sync_date:
            data["lastSyncDate"] = last_sync_date
        if description:
            data["description"] = description

        response = self._post(url=f"{self.__base_api}/mappings", data=data)
        return self._handle_response(response)

    def change_mapping(
        self,
        mapping_id: str,
        external_entity_id: str = None,
        sync_action: str = None,
        external_entity_url: str = None,
        id: str = None,
        last_sync_date: str = None,
        external_system_id: str = None,
        description: str = None,
        mapped_resource_id: str = None
    ) -> Dict[str, Any]:
        """
        Changes an existing mapping.
        :param mapping_id: The ID of the mapping to change.
        :param external_entity_id: The ID of the external entity.
        :param sync_action: The synchronization action.
        :param external_entity_url: The URL of the external entity.
        :param id: The ID of the mapping.
        :param last_sync_date: The last synchronization date.
        :param external_system_id: The ID of the external system.
        :param description: A description of the mapping.
        :param mapped_resource_id: The ID of the mapped resource.
        :return: Details of the updated mapping.
        """
        data = {}

        if external_entity_id:
            data["externalEntityId"] = external_entity_id
        if sync_action:
            data["syncAction"] = sync_action
        if external_entity_url:
            data["externalEntityUrl"] = external_entity_url
        if id:
            data["id"] = id
        if last_sync_date:
            data["lastSyncDate"] = last_sync_date
        if external_system_id:
            data["externalSystemId"] = external_system_id
        if description:
            data["description"] = description
        if mapped_resource_id:
            data["mappedResourceId"] = mapped_resource_id

        response = self._patch(url=f"{self.__base_api}/mappings/{mapping_id}", data=data)
        return self._handle_response(response)
