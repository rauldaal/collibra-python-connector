import uuid
from .Base import BaseAPI


class Assignment(BaseAPI):
    """API class for assignment operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/assignments"

    def add_assignment(self, asset_type_id: str = None, domain_type_id: str = None,
                       attribute_types: list = None, relation_types: list = None,
                       default_status_id: str = None, status_ids: list = None):
        """
        Adds a new Assignment.
        :param asset_type_id: The UUID of the asset type.
        :param domain_type_id: The UUID of the domain type.
        :param attribute_types: List of attribute type assignments.
        :param relation_types: List of relation type assignments.
        :param default_status_id: The UUID of the default status.
        :param status_ids: List of status UUIDs.
        :return: Created assignment details.
        """
        data = {}
        if asset_type_id is not None:
            try:
                uuid.UUID(asset_type_id)
            except ValueError as exc:
                raise ValueError("asset_type_id must be a valid UUID") from exc
            data["assetTypeId"] = asset_type_id
        if domain_type_id is not None:
            try:
                uuid.UUID(domain_type_id)
            except ValueError as exc:
                raise ValueError("domain_type_id must be a valid UUID") from exc
            data["domainTypeId"] = domain_type_id
        if attribute_types is not None:
            data["attributeTypes"] = attribute_types
        if relation_types is not None:
            data["relationTypes"] = relation_types
        if default_status_id is not None:
            try:
                uuid.UUID(default_status_id)
            except ValueError as exc:
                raise ValueError("default_status_id must be a valid UUID") from exc
            data["defaultStatusId"] = default_status_id
        if status_ids is not None:
            data["statusIds"] = status_ids

        if not data:
            raise ValueError("At least one field must be provided")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_assignments_for_asset(self, asset_id: str):
        """
        Returns the Assignment identified by the given Asset.
        :param asset_id: The UUID of the asset.
        :return: Assignment details.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/asset/{asset_id}")
        return self._handle_response(response)

    def get_assignments_for_asset_type(self, asset_type_id: str):
        """
        Returns Assignments for given asset type id.
        :param asset_type_id: The UUID of the asset type.
        :return: Assignment details.
        """
        if not asset_type_id:
            raise ValueError("asset_type_id is required")
        try:
            uuid.UUID(asset_type_id)
        except ValueError as exc:
            raise ValueError("asset_type_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/assetType/{asset_type_id}")
        return self._handle_response(response)

    def get_available_asset_types_for_domain(self, domain_id: str):
        """
        Returns available asset types for domain identified by given id.
        :param domain_id: The UUID of the domain.
        :return: List of available asset types.
        """
        if not domain_id:
            raise ValueError("domain_id is required")
        try:
            uuid.UUID(domain_id)
        except ValueError as exc:
            raise ValueError("domain_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/domain/{domain_id}/assetTypes")
        return self._handle_response(response)

    def get_available_attribute_types_for_asset(self, asset_id: str):
        """
        Returns available attribute types for asset identified by given id.
        :param asset_id: The UUID of the asset.
        :return: List of available attribute types.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/asset/{asset_id}/attributeTypes")
        return self._handle_response(response)

    def get_available_complex_relation_types_for_asset(self, asset_id: str):
        """
        Returns the available ComplexRelationTypes for the Asset identified by the given id.
        :param asset_id: The UUID of the asset.
        :return: List of available complex relation types.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/asset/{asset_id}/complexRelationTypes")
        return self._handle_response(response)

    def get_available_relation_types_for_asset(self, asset_id: str):
        """
        Returns the available RelationTypes for the Asset identified by the given id.
        :param asset_id: The UUID of the asset.
        :return: List of available relation types.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/asset/{asset_id}/relationTypes")
        return self._handle_response(response)

    def find_assignments_for_resource(self, resource_id: str = None, resource_type: str = None,
                                      resource_discriminator: str = None):
        """
        Find the assignments where a given resource is assigned.
        :param resource_id: The UUID of the resource.
        :param resource_type: The type of the resource (deprecated, use resource_discriminator).
        :param resource_discriminator: The discriminator of the resource.
        :return: List of assignments.
        """
        params = {}
        if resource_id is not None:
            try:
                uuid.UUID(resource_id)
            except ValueError as exc:
                raise ValueError("resource_id must be a valid UUID") from exc
            params["resourceId"] = resource_id
        if resource_type is not None:
            params["resourceType"] = resource_type
        if resource_discriminator is not None:
            params["resourceDiscriminator"] = resource_discriminator

        response = self._get(url=f"{self.__base_api}/forResource", params=params or None)
        return self._handle_response(response)

    def change_assignment(self, assignment_id: str, attribute_types: list = None,
                          relation_types: list = None, default_status_id: str = None,
                          status_ids: list = None):
        """
        Changes the assignment with the information provided.
        :param assignment_id: The UUID of the assignment to change.
        :param attribute_types: New list of attribute type assignments.
        :param relation_types: New list of relation type assignments.
        :param default_status_id: New default status UUID.
        :param status_ids: New list of status UUIDs.
        :return: Updated assignment details.
        """
        if not assignment_id:
            raise ValueError("assignment_id is required")
        try:
            uuid.UUID(assignment_id)
        except ValueError as exc:
            raise ValueError("assignment_id must be a valid UUID") from exc

        data = {}
        if attribute_types is not None:
            data["attributeTypes"] = attribute_types
        if relation_types is not None:
            data["relationTypes"] = relation_types
        if default_status_id is not None:
            try:
                uuid.UUID(default_status_id)
            except ValueError as exc:
                raise ValueError("default_status_id must be a valid UUID") from exc
            data["defaultStatusId"] = default_status_id
        if status_ids is not None:
            data["statusIds"] = status_ids

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{assignment_id}", data=data)
        return self._handle_response(response)

    def remove_assignment(self, assignment_id: str):
        """
        Removes the Assignment identified by the given id.
        :param assignment_id: The UUID of the assignment.
        :return: None
        """
        if not assignment_id:
            raise ValueError("assignment_id is required")
        try:
            uuid.UUID(assignment_id)
        except ValueError as exc:
            raise ValueError("assignment_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{assignment_id}")
        return self._handle_response(response)
