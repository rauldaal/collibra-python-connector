import uuid
from .Base import BaseAPI


class Scope(BaseAPI):
    """API class for scope operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/scopes"

    def get_all_scopes(self):
        """
        Returns all scopes.
        :return: List of all scopes.
        """
        response = self._get(url=self.__base_api)
        return self._handle_response(response)

    def get_scope(self, scope_id: str):
        """
        Returns the scope identified by the given UUID.
        :param scope_id: The UUID of the scope.
        :return: Scope details.
        """
        if not scope_id:
            raise ValueError("scope_id is required")
        try:
            uuid.UUID(scope_id)
        except ValueError as exc:
            raise ValueError("scope_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{scope_id}")
        return self._handle_response(response)

    def get_scope_by_public_id(self, public_id: str):
        """
        Returns the scope identified by the given public ID.
        :param public_id: The public ID of the scope.
        :return: Scope details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)

    def add_scope(self, name: str, description: str = None, community_ids: list = None,
                   domain_ids: list = None, asset_type_ids: list = None):
        """
        Adds a new scope.
        :param name: The name of the scope (required).
        :param description: Optional description.
        :param community_ids: Optional list of community UUIDs to include in the scope.
        :param domain_ids: Optional list of domain UUIDs to include in the scope.
        :param asset_type_ids: Optional list of asset type UUIDs to include in the scope.
        :return: Created scope details.
        """
        if not name:
            raise ValueError("name is required")

        data = {"name": name}
        if description is not None:
            data["description"] = description
        if community_ids is not None:
            data["communityIds"] = community_ids
        if domain_ids is not None:
            data["domainIds"] = domain_ids
        if asset_type_ids is not None:
            data["assetTypeIds"] = asset_type_ids

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def change_scope(self, scope_id: str, name: str = None, description: str = None,
                      community_ids: list = None, domain_ids: list = None,
                      asset_type_ids: list = None):
        """
        Changes the scope with the given ID.
        :param scope_id: The UUID of the scope to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :param community_ids: Optional new list of community UUIDs.
        :param domain_ids: Optional new list of domain UUIDs.
        :param asset_type_ids: Optional new list of asset type UUIDs.
        :return: Updated scope details.
        """
        if not scope_id:
            raise ValueError("scope_id is required")
        try:
            uuid.UUID(scope_id)
        except ValueError as exc:
            raise ValueError("scope_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if community_ids is not None:
            data["communityIds"] = community_ids
        if domain_ids is not None:
            data["domainIds"] = domain_ids
        if asset_type_ids is not None:
            data["assetTypeIds"] = asset_type_ids

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{scope_id}", data=data)
        return self._handle_response(response)

    def remove_scope(self, scope_id: str):
        """
        Removes the scope identified by the given UUID.
        :param scope_id: The UUID of the scope.
        :return: None
        """
        if not scope_id:
            raise ValueError("scope_id is required")
        try:
            uuid.UUID(scope_id)
        except ValueError as exc:
            raise ValueError("scope_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{scope_id}")
        return self._handle_response(response)
