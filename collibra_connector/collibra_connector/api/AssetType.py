import uuid
from .Base import BaseAPI


class AssetType(BaseAPI):
    """API class for asset type operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/assetTypes"

    def find_asset_types(
        self,
        count_limit: int = -1,
        display_name_enabled: bool = None,
        exclude_final: bool = None,
        exclude_meta: bool = None,
        exclude_unlicensed_products: bool = None,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
        parent_id: str = None,
        top_level: bool = None,
    ):
        """
        Returns asset types matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param display_name_enabled: Whether display name is enabled for the asset type.
        :param exclude_final: Whether to exclude final asset types.
        :param exclude_meta: Whether to exclude meta asset types.
        :param exclude_unlicensed_products: Whether to exclude asset types from unlicensed products.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :param parent_id: UUID of parent asset type to filter by.
        :param top_level: Whether to only return top-level asset types.
        :return: List of asset types matching criteria.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if parent_id is not None:
            try:
                uuid.UUID(parent_id)
            except ValueError as exc:
                raise ValueError("parent_id must be a valid UUID") from exc

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if display_name_enabled is not None:
            params["displayNameEnabled"] = display_name_enabled
        if exclude_final is not None:
            params["excludeFinal"] = exclude_final
        if exclude_meta is not None:
            params["excludeMeta"] = exclude_meta
        if exclude_unlicensed_products is not None:
            params["excludeUnlicensedProducts"] = exclude_unlicensed_products
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset
        if parent_id is not None:
            params["parentId"] = parent_id
        if top_level is not None:
            params["topLevel"] = top_level

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def get_asset_type(self, asset_type_id: str):
        """
        Returns the asset type identified by the given UUID.
        :param asset_type_id: The UUID of the asset type.
        :return: Asset type details.
        """
        if not asset_type_id:
            raise ValueError("asset_type_id is required")
        try:
            uuid.UUID(asset_type_id)
        except ValueError as exc:
            raise ValueError("asset_type_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{asset_type_id}")
        return self._handle_response(response)

    def get_asset_type_by_public_id(self, public_id: str):
        """
        Returns the asset type identified by the given public ID.
        :param public_id: The public ID of the asset type.
        :return: Asset type details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)

    def add_asset_type(self, name: str, description: str = None, parent_id: str = None,
                       color: str = None, symbol_type: str = None, icon_code: str = None,
                       acronym_code: str = None, display_name_enabled: bool = None,
                       rating_enabled: bool = None):
        """
        Adds a new asset type.
        :param name: The name of the asset type (required).
        :param description: Optional description.
        :param parent_id: Optional UUID of the parent asset type.
        :param color: Optional color for the asset type.
        :param symbol_type: Optional symbol type.
        :param icon_code: Optional icon code.
        :param acronym_code: Optional acronym code.
        :param display_name_enabled: Whether display name is enabled.
        :param rating_enabled: Whether rating is enabled.
        :return: Created asset type details.
        """
        if not name:
            raise ValueError("name is required")

        if parent_id is not None:
            try:
                uuid.UUID(parent_id)
            except ValueError as exc:
                raise ValueError("parent_id must be a valid UUID") from exc

        data = {"name": name}
        if description is not None:
            data["description"] = description
        if parent_id is not None:
            data["parentId"] = parent_id
        if color is not None:
            data["color"] = color
        if symbol_type is not None:
            data["symbolType"] = symbol_type
        if icon_code is not None:
            data["iconCode"] = icon_code
        if acronym_code is not None:
            data["acronymCode"] = acronym_code
        if display_name_enabled is not None:
            data["displayNameEnabled"] = display_name_enabled
        if rating_enabled is not None:
            data["ratingEnabled"] = rating_enabled

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_asset_types(self, asset_types: list):
        """
        Adds multiple asset types in one go.
        :param asset_types: List of asset type objects.
        :return: Created asset types.
        """
        if not asset_types or not isinstance(asset_types, list):
            raise ValueError("asset_types must be a non-empty list")
        response = self._post(url=f"{self.__base_api}/bulk", data={"assetTypes": asset_types})
        return self._handle_response(response)

    def change_asset_type(self, asset_type_id: str, name: str = None, description: str = None,
                          parent_id: str = None, color: str = None, symbol_type: str = None,
                          icon_code: str = None, acronym_code: str = None,
                          display_name_enabled: bool = None, rating_enabled: bool = None):
        """
        Changes the asset type with the given ID.
        :param asset_type_id: The UUID of the asset type to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :param parent_id: Optional new parent UUID.
        :param color: Optional new color.
        :param symbol_type: Optional new symbol type.
        :param icon_code: Optional new icon code.
        :param acronym_code: Optional new acronym code.
        :param display_name_enabled: Optional display name enabled setting.
        :param rating_enabled: Optional rating enabled setting.
        :return: Updated asset type details.
        """
        if not asset_type_id:
            raise ValueError("asset_type_id is required")
        try:
            uuid.UUID(asset_type_id)
        except ValueError as exc:
            raise ValueError("asset_type_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if parent_id is not None:
            try:
                uuid.UUID(parent_id)
            except ValueError as exc:
                raise ValueError("parent_id must be a valid UUID") from exc
            data["parentId"] = parent_id
        if color is not None:
            data["color"] = color
        if symbol_type is not None:
            data["symbolType"] = symbol_type
        if icon_code is not None:
            data["iconCode"] = icon_code
        if acronym_code is not None:
            data["acronymCode"] = acronym_code
        if display_name_enabled is not None:
            data["displayNameEnabled"] = display_name_enabled
        if rating_enabled is not None:
            data["ratingEnabled"] = rating_enabled

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{asset_type_id}", data=data)
        return self._handle_response(response)

    def change_asset_types(self, asset_types: list):
        """
        Changes multiple asset types in one go.
        :param asset_types: List of asset type change objects (must include id).
        :return: Updated asset types.
        """
        if not asset_types or not isinstance(asset_types, list):
            raise ValueError("asset_types must be a non-empty list")
        response = self._patch(url=f"{self.__base_api}/bulk", data={"assetTypes": asset_types})
        return self._handle_response(response)

    def remove_asset_type(self, asset_type_id: str):
        """
        Removes the asset type identified by the given UUID.
        :param asset_type_id: The UUID of the asset type.
        :return: None
        """
        if not asset_type_id:
            raise ValueError("asset_type_id is required")
        try:
            uuid.UUID(asset_type_id)
        except ValueError as exc:
            raise ValueError("asset_type_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{asset_type_id}")
        return self._handle_response(response)

    def remove_asset_types(self, asset_type_ids: list):
        """
        Removes multiple asset types.
        :param asset_type_ids: List of asset type UUIDs to remove.
        :return: None
        """
        if not asset_type_ids or not isinstance(asset_type_ids, list):
            raise ValueError("asset_type_ids must be a non-empty list")
        response = self._delete(url=f"{self.__base_api}/bulk")
        return self._handle_response(response)

    def find_parent_types(self, asset_type_id: str):
        """
        Returns the parent types of the asset type with the given ID.
        :param asset_type_id: The UUID of the asset type.
        :return: List of parent asset types.
        """
        if not asset_type_id:
            raise ValueError("asset_type_id is required")
        try:
            uuid.UUID(asset_type_id)
        except ValueError as exc:
            raise ValueError("asset_type_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{asset_type_id}/parents")
        return self._handle_response(response)

    def find_sub_asset_types(self, asset_type_id: str, include_parent: bool = None,
                             direct_sub_types_only: bool = None):
        """
        Returns the sub types of the asset type with the given ID.
        :param asset_type_id: The UUID of the asset type.
        :param include_parent: Whether to include the parent type in the results.
        :param direct_sub_types_only: Whether to include only direct subtypes.
        :return: List of sub asset types.
        """
        if not asset_type_id:
            raise ValueError("asset_type_id is required")
        try:
            uuid.UUID(asset_type_id)
        except ValueError as exc:
            raise ValueError("asset_type_id must be a valid UUID") from exc

        params = {}
        if include_parent is not None:
            params["includeParent"] = include_parent
        if direct_sub_types_only is not None:
            params["directSubTypesOnly"] = direct_sub_types_only

        response = self._get(url=f"{self.__base_api}/{asset_type_id}/subTypes", params=params or None)
        return self._handle_response(response)
