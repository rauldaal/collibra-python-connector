import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Assets(BaseAPI):
    """API class for asset operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/assets"

    def find_assets(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        cursor: str = None,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        domain_id: str = None,
        community_id: str = None,
        type_ids: List[str] = None,
        type_public_ids: List[str] = None,
        type_id: List[str] = None,
        status_ids: List[str] = None,
        status_id: List[str] = None,
        tag_names: List[str] = None,
        type_inheritance: bool = True,
        exclude_meta: bool = True,
        sort_field: str = "NAME",
        sort_order: str = "ASC"
    ) -> Dict[str, Any]:
        """
        Find assets matching the given search criteria.

        :param offset: The starting offset for pagination.
        :param limit: The maximum number of results to return.
        :param count_limit: The maximum number of items to count.
        :param cursor: The cursor for pagination.
        :param name: The name of the asset to search for.
        :param name_match_mode: The matching mode for the name ('ANYWHERE', 'START', 'END', 'EXACT').
        :param domain_id: The ID of the domain to search within.
        :param community_id: The ID of the community to search within.
        :param type_ids: A list of asset type IDs.
        :param type_public_ids: A list of asset type public IDs.
        :param type_id: A list of asset type IDs (legacy).
        :param status_ids: A list of status IDs.
        :param status_id: A list of status IDs (legacy).
        :param tag_names: A list of tag names to filter by.
        :param type_inheritance: Whether to include inherited types.
        :param exclude_meta: Whether to exclude metadata from the results.
        :param sort_field: The field to sort by.
        :param sort_order: The order to sort by ('ASC', 'DESC').
        :return: A dictionary containing the search results.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "cursor": cursor,
            "name": name,
            "nameMatchMode": name_match_mode,
            "domainId": domain_id,
            "communityId": community_id,
            "typeIds": type_ids,
            "typePublicIds": type_public_ids,
            "typeId": type_id,
            "statusIds": status_ids,
            "statusId": status_id,
            "tagNames": tag_names,
            "typeInheritance": type_inheritance,
            "excludeMeta": exclude_meta,
            "sortField": sort_field,
            "sortOrder": sort_order
        }
        
        # UUID validation for single ID params
        for param_name in ["domainId", "communityId"]:
            val = params.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_asset(
        self,
        name: str,
        domain_id: str,
        display_name: str = None,
        type_id: str = None,
        id: str = None,
        status_id: str = None,
        excluded_from_auto_hyperlinking: bool = None,
        type_public_id: str = None
    ) -> Dict[str, Any]:
        """
        Adds a new asset to a domain.

        :param name: The name of the asset.
        :param domain_id: The ID of the domain.
        :param display_name: The display name of the asset.
        :param type_id: The ID of the asset type.
        :param id: The unique ID for the asset (optional).
        :param status_id: The ID of the status.
        :param excluded_from_auto_hyperlinking: Whether to exclude from auto-hyperlinking.
        :param type_public_id: The public ID of the asset type.
        :return: A dictionary representing the created asset.
        """
        if not name or not domain_id:
            raise ValueError("name and domain_id are required")

        data = {
            "name": name,
            "domainId": domain_id,
            "displayName": display_name,
            "typeId": type_id,
            "id": id,
            "statusId": status_id,
            "excludedFromAutoHyperlinking": excluded_from_auto_hyperlinking,
            "typePublicId": type_public_id
        }

        # UUID validation
        for param_name in ["domainId", "typeId", "id", "statusId"]:
            val = data.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_assets(self, assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adds multiple assets in one go.

        :param assets: A list of dictionaries, each representing an asset to add.
        :return: A list of dictionaries representing the created assets.
        """
        if not assets or not isinstance(assets, list):
            raise ValueError("assets must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=assets)
        return self._handle_response(response)

    def remove_assets(self, asset_ids: List[str]) -> None:
        """
        Removes multiple assets by their IDs.

        :param asset_ids: A list of asset IDs to remove.
        :return: None
        """
        if not asset_ids or not isinstance(asset_ids, list):
            raise ValueError("asset_ids must be a non-empty list")
        
        response = self._delete(url=f"{self.__base_api}/bulk", data=asset_ids)
        return self._handle_response(response)

    def change_assets(self, assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Changes multiple assets in one go.

        :param assets: A list of dictionaries, each representing an asset update.
        :return: A list of dictionaries representing the updated assets.
        """
        if not assets or not isinstance(assets, list):
            raise ValueError("assets must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=assets)
        return self._handle_response(response)

    def get_asset_tags(self, asset_id: str) -> List[Dict[str, Any]]:
        """
        Returns all tags of the asset with the given ID.

        :param asset_id: The ID of the asset.
        :return: A list of dictionaries representing the asset's tags.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{asset_id}/tags")
        return self._handle_response(response)

    def set_tags_for_asset(self, asset_id: str, tag_names: List[str]) -> List[Dict[str, Any]]:
        """
        Sets tags for the asset with the given ID.

        :param asset_id: The ID of the asset.
        :param tag_names: A list of tag names to set.
        :return: A list of dictionaries representing the set tags.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        data = {"tagNames": tag_names}
        response = self._put(url=f"{self.__base_api}/{asset_id}/tags", data=data)
        return self._handle_response(response)

    def add_tags_to_asset(self, asset_id: str, tag_names: List[str]) -> List[Dict[str, Any]]:
        """
        Adds tags to the asset with the given ID.

        :param asset_id: The ID of the asset.
        :param tag_names: A list of tag names to add.
        :return: A list of dictionaries representing the added tags.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        data = {"tagNames": tag_names}
        response = self._post(url=f"{self.__base_api}/{asset_id}/tags", data=data)
        return self._handle_response(response)

    def remove_tags_from_asset(self, asset_id: str, tag_names: List[str]) -> None:
        """
        Removes tags from the asset with the given ID.

        :param asset_id: The ID of the asset.
        :param tag_names: A list of tag names to remove.
        :return: None
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        data = {"tagNames": tag_names}
        # Standard _delete often doesn't take data, but Collibra bulk delete usually does
        response = self._delete(url=f"{self.__base_api}/{asset_id}/tags", data=data)
        return self._handle_response(response)

    def get_asset(self, asset_id: str) -> Dict[str, Any]:
        """
        Returns the asset having the given ID.

        :param asset_id: The ID of the asset.
        :return: A dictionary representing the asset.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{asset_id}")
        return self._handle_response(response)

    def remove_asset(self, asset_id: str) -> None:
        """
        Removes an asset identified by the given ID.

        :param asset_id: The ID of the asset.
        :return: None
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{asset_id}")
        return self._handle_response(response)

    def change_asset(
        self,
        asset_id: str,
        id: str = None,
        name: str = None,
        display_name: str = None,
        type_id: str = None,
        status_id: str = None,
        domain_id: str = None,
        excluded_from_auto_hyperlinking: bool = None,
        type_public_id: str = None,
    ) -> Dict[str, Any]:
        """
        Changes the asset with the given ID.

        :param asset_id: The ID of the asset to change.
        :param id: The new ID of the asset.
        :param name: The new name of the asset.
        :param display_name: The new display name of the asset.
        :param type_id: The new ID of the asset type.
        :param status_id: The new ID of the status.
        :param domain_id: The new ID of the domain.
        :param excluded_from_auto_hyperlinking: New exclusion status for auto-hyperlinking.
        :param type_public_id: The new public ID of the asset type.
        :return: A dictionary representing the updated asset.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")

        data = {}
        if id is not None:
            data["id"] = id
        if name is not None:
            data["name"] = name
        if display_name is not None:
            data["displayName"] = display_name
        if type_id is not None:
            data["typeId"] = type_id
        if status_id is not None:
            data["statusId"] = status_id
        if domain_id is not None:
            data["domainId"] = domain_id
        if excluded_from_auto_hyperlinking is not None:
            data["excludedFromAutoHyperlinking"] = excluded_from_auto_hyperlinking
        if type_public_id is not None:
            data["typePublicId"] = type_public_id

        response = self._patch(url=f"{self.__base_api}/{asset_id}", data=data)
        return self._handle_response(response)

    def get_asset_breadcrumb(self, asset_id: str) -> List[Dict[str, Any]]:
        """
        Returns the list of resources that lead to the asset identified by the given ID.

        :param asset_id: The ID of the asset.
        :return: A list of dictionaries representing the breadcrumb trail.
        """
        response = self._get(url=f"{self.__base_api}/{asset_id}/breadcrumb")
        return self._handle_response(response)

    def set_asset_attributes(self, asset_id: str, type_id: str = None, type_public_id: str = None, values: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Replaces all attributes of the asset with the given ID.

        :param asset_id: The ID of the asset.
        :param type_id: The ID of the asset type.
        :param type_public_id: The public ID of the asset type.
        :param values: A list of attribute values.
        :return: A list of dictionaries representing the set attributes.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        data = {
            "typeId": type_id,
            "typePublicId": type_public_id,
            "values": values
        }
        response = self._put(url=f"{self.__base_api}/{asset_id}/attributes", data=data)
        return self._handle_response(response)

    def set_asset_relations(self, asset_id: str, type_id: str = None, type_public_id: str = None, related_asset_ids: List[str] = None, relation_direction: str = None) -> List[Dict[str, Any]]:
        """
        Sets relations for the asset with the given ID.

        :param asset_id: The ID of the asset.
        :param type_id: The ID of the asset type.
        :param type_public_id: The public ID of the asset type.
        :param related_asset_ids: A list of IDs of related assets.
        :param relation_direction: The direction of the relation.
        :return: A list of dictionaries representing the set relations.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        data = {
            "typeId": type_id,
            "typePublicId": type_public_id,
            "relatedAssetIds": related_asset_ids,
            "relationDirection": relation_direction
        }
        response = self._put(url=f"{self.__base_api}/{asset_id}/relations", data=data)
        return self._handle_response(response)

    def set_asset_responsibilities(self, asset_id: str, role_id: str = None, owner_ids: List[str] = None) -> List[Dict[str, Any]]:
        """
        Sets responsibilities for the asset with the given ID.

        :param asset_id: The ID of the asset.
        :param role_id: The ID of the role.
        :param owner_ids: A list of owner IDs.
        :return: A list of dictionaries representing the set responsibilities.
        """
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")
        
        data = {
            "roleId": role_id,
            "ownerIds": owner_ids
        }
        response = self._put(url=f"{self.__base_api}/{asset_id}/responsibilities", data=data)
        return self._handle_response(response)

    # --- Convenience and Legacy Methods ---

    def get_full_profile(
        self,
        asset_id: str,
        include_attributes: bool = True,
        include_relations: bool = True,
        include_responsibilities: bool = True,
        include_comments: bool = False,
        include_activities: bool = False,
        include_tags: bool = True,
        include_attachments: bool = True
    ):
        """
        Get a complete profile of an asset including all related information.

        :param asset_id: The ID of the asset.
        :param include_attributes: Whether to include asset attributes.
        :param include_relations: Whether to include asset relations.
        :param include_responsibilities: Whether to include asset responsibilities.
        :param include_comments: Whether to include asset comments.
        :param include_activities: Whether to include asset activities.
        :param include_tags: Whether to include asset tags.
        :param include_attachments: Whether to include asset attachments.
        :return: An AssetProfileModel object representing the asset profile.
        """
        # (Preserving complex logic here, ensuring it uses updated methods)
        if not self._uuid_validation(asset_id):
            raise ValueError("asset_id must be a valid UUID")

        connector = self._BaseAPI__connector
        from ..models import AssetProfileModel, AssetModel, RelationsGrouped, ResponsibilitySummary, CommentModel

        asset_raw = self.get_asset(asset_id)
        asset_data = AssetModel(
            id=asset_raw.get('id'),
            name=asset_raw.get('name'),
            display_name=asset_raw.get('displayName'),
            type_name=asset_raw.get('type', {}).get('name'),
            type_id=asset_raw.get('type', {}).get('id'),
            status_name=asset_raw.get('status', {}).get('name'),
            status_id=asset_raw.get('status', {}).get('id'),
            domain_name=asset_raw.get('domain', {}).get('name'),
            domain_id=asset_raw.get('domain', {}).get('id'),
            type=asset_raw.get('type'),
            status=asset_raw.get('status'),
            domain=asset_raw.get('domain')
        )

        attributes_dict = {}
        relations_data = {"outgoing": {}, "incoming": {}, "outgoing_count": 0, "incoming_count": 0}
        responsibilities_list = []
        comments_list = []
        activities_list = []
        tags_list = []
        attachments_list = []

        if include_attributes:
            try:
                attributes_dict = connector.attributes.get_attributes_as_dict(asset_id)
            except Exception: pass

        if include_relations:
            try:
                relations_data = connector.relations.get_asset_relations(asset_id, include_type_details=True)
            except Exception: pass

        if include_responsibilities:
            try:
                resp_result = connector.responsibilities.get_asset_responsibilities(asset_id)
                for resp in resp_result.get('results', []):
                    role = resp.get('role', {}).get('name', 'Unknown')
                    owner = resp.get('owner', {})
                    owner_name = f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip() or owner.get('name', 'Unknown')
                    responsibilities_list.append(ResponsibilitySummary(role=role, owner=owner_name, owner_id=owner.get('id')))
            except Exception: pass

        if include_comments:
            try:
                comments_result = connector.comments.find_comments(base_resource_id=asset_id)
                for comment_data in comments_result.get('results', []):
                    comments_list.append(CommentModel(
                        id=comment_data.get('id'),
                        content=comment_data.get('content'),
                        created_by=comment_data.get('createdBy', {}).get('name'),
                        created_date=comment_data.get('createdDate'),
                        resolved=comment_data.get('resolved', False)
                    ))
            except Exception: pass

        if include_activities:
            try:
                activities_list = self.get_asset_activities(asset_id)
            except Exception: pass

        if include_tags:
            try:
                tags_res = self.get_asset_tags(asset_id)
                tags_list = [tag.get('name', '') for tag in tags_res]
            except Exception: pass

        if include_attachments:
            try:
                attachments_list = self.get_attachments(asset_id)
            except Exception: pass

        return AssetProfileModel(
            asset=asset_data,
            attributes=attributes_dict,
            relations=RelationsGrouped(**relations_data),
            responsibilities=responsibilities_list,
            comments=comments_list,
            activities=activities_list,
            tags=tags_list,
            attachments=attachments_list
        )

    def get_asset_activities(self, asset_id: str, limit: int = 50):
        """
        Legacy helper for asset activities.

        :param asset_id: The ID of the asset.
        :param limit: The maximum number of activities to return.
        :return: A list of activity dictionaries.
        """
        params = {"contextId": asset_id, "resourceTypes": ["Asset"], "limit": limit}
        response = self._get(url=f"{self.__base_api}/activities", params=params)
        return self._handle_response(response).get("results", [])

    def get_attachments(self, asset_id: str):
        """
        Legacy helper for asset attachments.

        :param asset_id: The ID of the asset.
        :return: A list of attachment dictionaries.
        """
        url = f"{self._BaseAPI__connector.api}/attachments"
        params = {"resourceId": asset_id, "resourceType": "Asset"}
        response = self._get(url=url, params=params)
        return self._handle_response(response).get("results", [])
