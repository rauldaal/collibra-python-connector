import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Scopes(BaseAPI):
    """API class for scope operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/scopes"

    def get_all_scopes(self) -> Dict[str, Any]:
        """
        Returns all scopes.
        """
        response = self._get(url=self.__base_api)
        return self._handle_response(response)

    def add_scope(
        self,
        name: str,
        description: str = None,
        id: str = None,
        public_id: str = None,
        community_ids: List[str] = None,
        domain_ids: List[str] = None,
        asset_type_ids: List[str] = None
    ) -> Dict[str, Any]:
        """
        Adds a new scope.
        """
        if not name:
            raise ValueError("name is required")

        data = {
            "name": name,
            "description": description,
            "id": id,
            "publicId": public_id,
            "communityIds": community_ids,
            "domainIds": domain_ids,
            "assetTypeIds": asset_type_ids
        }

        if id and not self._uuid_validation(id):
            raise ValueError("id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_scope(self, scope_id: str) -> Dict[str, Any]:
        """
        Returns scope identified by given id.
        """
        if not self._uuid_validation(scope_id):
            raise ValueError("scope_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{scope_id}")
        return self._handle_response(response)

    def remove_scope(self, scope_id: str) -> None:
        """
        Removes scope identified by given id.
        """
        if not self._uuid_validation(scope_id):
            raise ValueError("scope_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{scope_id}")
        return self._handle_response(response)

    def change_scope(
        self,
        scope_id: str,
        name: str = None,
        description: str = None,
        public_id: str = None,
        community_ids: List[str] = None,
        domain_ids: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Changes the scope with the information that is present in the request.
        Only properties that are specified and are not None are updated.
        """
        if not self._uuid_validation(scope_id):
            raise ValueError("scope_id must be a valid UUID")

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if public_id is not None:
            data["publicId"] = public_id
        if community_ids is not None:
            data["communityIds"] = community_ids
        if domain_ids is not None:
            data["domainIds"] = domain_ids

        response = self._patch(url=f"{self.__base_api}/{scope_id}", data=data)
        return self._handle_response(response)

    def get_scope_by_public_id(self, public_id: str) -> Dict[str, Any]:
        """
        Returns the scope identified by the given public id.
        """
        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)
