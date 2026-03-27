import uuid
from .Base import BaseAPI


class Trait(BaseAPI):
    """API class for trait operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/traits"

    def find_traits(
        self,
        count_limit: int = -1,
        limit: int = 0,
        name: str = None,
        name_match_mode: str = "ANYWHERE",
        offset: int = 0,
    ):
        """
        Returns traits matching the given search criteria.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param name: Name to search for.
        :param name_match_mode: Matching mode. Options: START, END, ANYWHERE, EXACT
        :param offset: First result to retrieve.
        :return: List of traits.
        """
        valid_match_modes = ["START", "END", "ANYWHERE", "EXACT"]
        if name_match_mode not in valid_match_modes:
            raise ValueError(f"name_match_mode must be one of: {', '.join(valid_match_modes)}")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        params = {}
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if name is not None:
            params["name"] = name
        if name_match_mode != "ANYWHERE":
            params["nameMatchMode"] = name_match_mode
        if offset != 0:
            params["offset"] = offset

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_trait(self, trait_id: str):
        """
        Returns the trait identified by the given UUID.
        :param trait_id: The UUID of the trait.
        :return: Trait details.
        """
        if not trait_id:
            raise ValueError("trait_id is required")
        try:
            uuid.UUID(trait_id)
        except ValueError as exc:
            raise ValueError("trait_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{trait_id}")
        return self._handle_response(response)

    def get_trait_by_public_id(self, public_id: str):
        """
        Returns the trait identified by the given public ID.
        :param public_id: The public ID of the trait.
        :return: Trait details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)

    def change_trait(self, trait_id: str, name: str = None, description: str = None):
        """
        Changes the trait with the given ID.
        :param trait_id: The UUID of the trait to change.
        :param name: Optional new name.
        :param description: Optional new description.
        :return: Updated trait details.
        """
        if not trait_id:
            raise ValueError("trait_id is required")
        try:
            uuid.UUID(trait_id)
        except ValueError as exc:
            raise ValueError("trait_id must be a valid UUID") from exc

        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{trait_id}", data=data)
        return self._handle_response(response)
