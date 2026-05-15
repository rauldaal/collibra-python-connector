import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class Domains(BaseAPI):
    """API class for domain operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/domains"

    def find_domains(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        cursor: str = None,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        exclude_meta: bool = True,
        community_id: str = None,
        type_id: str = None,
        type_public_id: str = None,
        include_sub_communities: bool = False
    ) -> Dict[str, Any]:
        """
        Searches for domains based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param cursor: The cursor for paginated results (optional).
        :param name: The name of the domain to search for (optional).
        :param name_match_mode: The mode for matching the name (default: "ANYWHERE").
        :param exclude_meta: Whether to exclude metadata domains (default: True).
        :param community_id: The UUID of the community to search within (optional).
        :param type_id: The UUID of the domain type to filter by (optional).
        :param type_public_id: The public ID of the domain type to filter by (optional).
        :param include_sub_communities: Whether to include sub-communities in the search (default: False).
        :return: A dictionary containing the matching domains.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "cursor": cursor,
            "name": name,
            "nameMatchMode": name_match_mode,
            "excludeMeta": exclude_meta,
            "communityId": community_id,
            "typeId": type_id,
            "typePublicId": type_public_id,
            "includeSubCommunities": include_sub_communities
        }
        
        # UUID validation
        for param_name in ["communityId", "typeId"]:
            val = params.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_domain(
        self,
        name: str,
        community_id: str,
        type_id: str = None,
        description: str = None,
        id: str = None,
        excluded_from_auto_hyperlinking: bool = None,
        type_public_id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new domain within a specified community.

        :param name: The name of the domain (required).
        :param community_id: The UUID of the community to add the domain to (required).
        :param type_id: The UUID of the domain type (optional).
        :param description: A description of the domain (optional).
        :param id: The unique identifier of the domain (optional).
        :param excluded_from_auto_hyperlinking: Whether the domain is excluded from auto-hyperlinking (optional).
        :param type_public_id: The public ID of the domain type (optional).
        :return: A dictionary containing the details of the created domain.
        """
        if not name or not community_id:
            raise ValueError("name and community_id are required")

        data = {
            "name": name,
            "communityId": community_id,
            "typeId": type_id,
            "description": description,
            "id": id,
            "excludedFromAutoHyperlinking": excluded_from_auto_hyperlinking,
            "typePublicId": type_public_id
        }

        # UUID validation
        for param_name in ["communityId", "typeId", "id"]:
            val = data.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_domains(self, domains: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Creates multiple domains in a single operation.

        :param domains: A list of dictionaries, each containing the details of a domain to create.
        :return: A list of dictionaries containing the details of the created domains.
        """
        if not domains or not isinstance(domains, list):
            raise ValueError("domains must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/bulk", data=domains)
        return self._handle_response(response)

    def remove_domains(self, domain_ids: List[str]) -> None:
        """
        Deletes multiple domains identified by their IDs. (Deprecated)

        :param domain_ids: A list of UUIDs for the domains to delete.
        """
        if not domain_ids or not isinstance(domain_ids, list):
            raise ValueError("domain_ids must be a non-empty list")
        
        response = self._delete(url=f"{self.__base_api}/bulk", data=domain_ids)
        return self._handle_response(response)

    def change_domains(self, domains: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Updates multiple domains with the specified details.

        :param domains: A list of dictionaries, each containing the updated details of a domain.
        :return: A list of dictionaries containing the updated domains.
        """
        if not domains or not isinstance(domains, list):
            raise ValueError("domains must be a non-empty list")
        
        response = self._patch(url=f"{self.__base_api}/bulk", data=domains)
        return self._handle_response(response)

    def get_domain(self, domain_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a domain by its ID.

        :param domain_id: The UUID of the domain to retrieve.
        :return: A dictionary containing the details of the domain.
        """
        if not self._uuid_validation(domain_id):
            raise ValueError("domain_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{domain_id}")
        return self._handle_response(response)

    def remove_domain(self, domain_id: str) -> None:
        """
        Deletes a domain identified by its ID. (Deprecated)

        :param domain_id: The UUID of the domain to delete.
        """
        if not self._uuid_validation(domain_id):
            raise ValueError("domain_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{domain_id}")
        return self._handle_response(response)

    def change_domain(
        self,
        domain_id: str,
        name: str = None,
        description: str = None,
        type_id: str = None,
        type_public_id: str = None,
        remove_scope_overlap_on_move: bool = None,
        community_id: str = None,
        excluded_from_auto_hyperlinking: bool = None,
        id: str = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a domain identified by its ID.

        :param domain_id: The UUID of the domain to update.
        :param name: The new name of the domain (optional).
        :param description: The new description of the domain (optional).
        :param type_id: The UUID of the new domain type (optional).
        :param type_public_id: The new public ID of the domain type (optional).
        :param remove_scope_overlap_on_move: Whether to remove scope overlap on move (optional).
        :param community_id: The UUID of the new community (optional).
        :param excluded_from_auto_hyperlinking: Whether the domain is excluded from auto-hyperlinking (optional).
        :param id: The new unique identifier of the domain (optional).
        :return: A dictionary containing the updated details of the domain.
        """
        if not self._uuid_validation(domain_id):
            raise ValueError("domain_id must be a valid UUID")
        
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if type_id is not None:
            data["typeId"] = type_id
        if type_public_id is not None:
            data["typePublicId"] = type_public_id
        if remove_scope_overlap_on_move is not None:
            data["removeScopeOverlapOnMove"] = remove_scope_overlap_on_move
        if community_id is not None:
            data["communityId"] = community_id
        if excluded_from_auto_hyperlinking is not None:
            data["excludedFromAutoHyperlinking"] = excluded_from_auto_hyperlinking
        if id is not None:
            data["id"] = id

        response = self._patch(url=f"{self.__base_api}/{domain_id}", data=data)
        return self._handle_response(response)

    def get_domain_breadcrumb(self, domain_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves the breadcrumb trail for a domain by its ID.

        :param domain_id: The UUID of the domain to retrieve the breadcrumb for.
        :return: A list of dictionaries representing the breadcrumb trail.
        """
        response = self._get(url=f"{self.__base_api}/{domain_id}/breadcrumb")
        return self._handle_response(response)

    def remove_domains_in_job(self, domain_ids: List[str]) -> Dict[str, Any]:
        """
        Deletes multiple domains in a single job.

        :param domain_ids: A list of UUIDs for the domains to delete.
        :return: A dictionary containing the details of the removal job.
        """
        if not domain_ids or not isinstance(domain_ids, list):
            raise ValueError("domain_ids must be a non-empty list")
        
        response = self._post(url=f"{self.__base_api}/removalJobs", data=domain_ids)
        return self._handle_response(response)
