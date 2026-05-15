import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class AssetTypes(BaseAPI):
    """API class for asset type operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/assetTypes"

    def find_asset_types(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        parent_id: str = None,
        exclude_meta: bool = True,
        exclude_final: bool = False,
        exclude_unlicensed_products: bool = False,
        top_level: bool = False,
        display_name_enabled: bool = None
    ) -> Dict[str, Any]:
        """
        Searches for asset types based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param name: The name of the asset type to search for (optional).
        :param name_match_mode: The matching mode for the name (default: "ANYWHERE").
        :param parent_id: The ID of the parent asset type (optional).
        :param exclude_meta: Whether to exclude meta asset types (default: True).
        :param exclude_final: Whether to exclude final asset types (default: False).
        :param exclude_unlicensed_products: Whether to exclude unlicensed products (default: False).
        :param top_level: Whether to include only top-level asset types (default: False).
        :param display_name_enabled: Whether to include asset types with display names enabled (optional).
        :return: A dictionary containing the matching asset types.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "name": name,
            "nameMatchMode": name_match_mode,
            "parentId": parent_id,
            "excludeMeta": exclude_meta,
            "excludeFinal": exclude_final,
            "excludeUnlicensedProducts": exclude_unlicensed_products,
            "topLevel": top_level,
            "displayNameEnabled": display_name_enabled
        }
        
        if parent_id and not self._uuid_validation(parent_id):
            raise ValueError("parentId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_asset_type(
        self,
        name: str,
        display_name_enabled: bool,
        rating_enabled: bool,
        symbol_type: str,
        id: str = None,
        public_id: str = None,
        description: str = None,
        color: str = None,
        icon_code: str = None,
        acronym_code: str = None,
        parent_id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new asset type with the specified parameters.

        :param name: The name of the asset type (required).
        :param display_name_enabled: Whether the asset type has display names enabled (required).
        :param rating_enabled: Whether the asset type has ratings enabled (required).
        :param symbol_type: The symbol type of the asset type (required).
        :param id: The unique identifier for the asset type (optional).
        :param public_id: The public identifier for the asset type (optional).
        :param description: A description of the asset type (optional).
        :param color: The color associated with the asset type (optional).
        :param icon_code: The icon code for the asset type (optional).
        :param acronym_code: The acronym code for the asset type (optional).
        :param parent_id: The ID of the parent asset type (optional).
        :return: A dictionary containing the details of the created asset type.
        """
        if name is None or display_name_enabled is None or rating_enabled is None or symbol_type is None:
            raise ValueError("name, display_name_enabled, rating_enabled, and symbol_type are required")

        data = {
            "name": name,
            "displayNameEnabled": display_name_enabled,
            "ratingEnabled": rating_enabled,
            "symbolType": symbol_type,
            "id": id,
            "publicId": public_id,
            "description": description,
            "color": color,
            "iconCode": icon_code,
            "acronymCode": acronym_code,
            "parentId": parent_id
        }

        # UUID validation
        for param_name in ["id", "parentId"]:
            val = data.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_asset_types(self, asset_types: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adds multiple asset types in bulk.

        :param asset_types: A list of dictionaries, each representing an asset type to be added.
        :return: A list of dictionaries containing the details of the added asset types.
        """
        if not asset_types or not isinstance(asset_types, list):
            raise ValueError("asset_types must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=asset_types)
        return self._handle_response(response)

    def remove_asset_types(self, asset_type_ids: List[str]) -> None:
        """
        Deletes multiple asset types identified by their IDs.

        :param asset_type_ids: A list of asset type IDs to be removed.
        """
        if not asset_type_ids or not isinstance(asset_type_ids, list):
            raise ValueError("asset_type_ids must be a non-empty list")
        
        response = self._delete(url=f"{self.__base_api}/bulk", data=asset_type_ids)
        return self._handle_response(response)

    def change_asset_types(self, asset_types: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Updates multiple asset types with the provided parameters.

        :param asset_types: A list of dictionaries, each containing the updated parameters for an asset type.
        :return: A list of dictionaries containing the details of the updated asset types.
        """
        if not asset_types or not isinstance(asset_types, list):
            raise ValueError("asset_types must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=asset_types)
        return self._handle_response(response)

    def get_asset_type(self, asset_type_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of an asset type by its ID.

        :param asset_type_id: The unique identifier of the asset type to retrieve.
        :return: A dictionary containing the details of the asset type.
        """
        if not self._uuid_validation(asset_type_id):
            raise ValueError("asset_type_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{asset_type_id}")
        return self._handle_response(response)

    def remove_asset_type(self, asset_type_id: str) -> None:
        """
        Deletes an asset type identified by its ID.

        :param asset_type_id: The unique identifier of the asset type to remove.
        """
        if not self._uuid_validation(asset_type_id):
            raise ValueError("asset_type_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{asset_type_id}")
        return self._handle_response(response)

    def change_asset_type(
        self,
        asset_type_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parent_id: Optional[str] = None,
        color: Optional[str] = None,
        symbol_type: Optional[str] = None,
        icon_code: Optional[str] = None,
        acronym_code: Optional[str] = None,
        display_name_enabled: Optional[bool] = None,
        rating_enabled: Optional[bool] = None,
        public_id: Optional[str] = None,
        id: Optional[str] = None,
        trait_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Updates the details of an asset type identified by its ID.

        :param asset_type_id: The unique identifier of the asset type to update.
        :param name: The new name of the asset type (optional).
        :param description: The new description of the asset type (optional).
        :param parent_id: The ID of the new parent asset type (optional).
        :param color: The new color associated with the asset type (optional).
        :param symbol_type: The new symbol type of the asset type (optional).
        :param icon_code: The new icon code for the asset type (optional).
        :param acronym_code: The new acronym code for the asset type (optional).
        :param display_name_enabled: Whether display names are enabled for the asset type (optional).
        :param rating_enabled: Whether ratings are enabled for the asset type (optional).
        :param public_id: The new public identifier for the asset type (optional).
        :param id: The new unique identifier for the asset type (optional).
        :param trait_ids: A list of trait IDs associated with the asset type (optional).
        :return: A dictionary containing the updated details of the asset type.
        """
        if not self._uuid_validation(asset_type_id):
            raise ValueError("asset_type_id must be a valid UUID")

        data = {
            "name": name,
            "description": description,
            "parentId": parent_id,
            "color": color,
            "symbolType": symbol_type,
            "iconCode": icon_code,
            "acronymCode": acronym_code,
            "displayNameEnabled": display_name_enabled,
            "ratingEnabled": rating_enabled,
            "publicId": public_id,
            "id": id,
            "traitIds": trait_ids
        }

        # Remove keys with None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._patch(url=f"{self.__base_api}/{asset_type_id}", data=data)
        return self._handle_response(response)

    def find_parent_types(self, asset_type_id: str) -> List[Dict[str, Any]]:
        """
        Finds all the parent asset types of the asset with the given ID.

        :param asset_type_id: The unique identifier of the asset to find parent types for.
        :return: A list of dictionaries containing the details of the parent asset types.
        """
        if not self._uuid_validation(asset_type_id):
            raise ValueError("asset_type_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{asset_type_id}/parents")
        return self._handle_response(response)

    def find_sub_asset_types(self, asset_type_id: str, include_parent: bool = None, direct_sub_types_only: bool = None) -> Dict[str, Any]:
        """
        Finds all asset subtypes of an asset type, as specified by the request parameters.

        :param asset_type_id: The unique identifier of the asset type to find subtypes for.
        :param include_parent: Whether to include parent asset types in the result (optional).
        :param direct_sub_types_only: Whether to include only direct subtypes (optional).
        :return: A dictionary containing the details of the subtypes.
        """
        if not self._uuid_validation(asset_type_id):
            raise ValueError("asset_type_id must be a valid UUID")
        
        params = {
            "includeParent": include_parent,
            "directSubTypesOnly": direct_sub_types_only
        }
        response = self._get(url=f"{self.__base_api}/{asset_type_id}/subTypes", params=params)
        return self._handle_response(response)

    def get_asset_type_by_public_id(self, public_id: str) -> Dict[str, Any]:
        """
        Returns the asset type identified by the given public id.

        :param public_id: The public identifier of the asset type.
        :return: A dictionary containing the details of the asset type.
        """
        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)
