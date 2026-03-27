import uuid
from .Base import BaseAPI


class RelationType(BaseAPI):
    """API class for relation type operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/relationTypes"

    def find_relation_types(
        self,
        co_role: str = None,
        count_limit: int = -1,
        limit: int = 0,
        offset: int = 0,
        role: str = None,
        role_co_role_logical_operator: str = None,
        sort_field: str = "ROLE",
        sort_order: str = "ASC",
        source_type_id: str = None,
        source_type_name: str = None,
        target_type_id: str = None,
        target_type_name: str = None,
    ):
        """
        Finds all relation types matching the given criteria.
        :param co_role: The coRole of the relation type.
        :param count_limit: Limit elements counted. -1 counts all, 0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param role: The role of the relation type.
        :param role_co_role_logical_operator: Logical operator for role/coRole filter (AND or OR).
        :param sort_field: Field to sort by.
        :param sort_order: Sort order (ASC or DESC).
        :param source_type_id: UUID of the source asset type to filter by.
        :param source_type_name: Name of the source asset type to filter by.
        :param target_type_id: UUID of the target asset type to filter by.
        :param target_type_name: Name of the target asset type to filter by.
        :return: List of relation types.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if source_type_id is not None:
            try:
                uuid.UUID(source_type_id)
            except ValueError as exc:
                raise ValueError("source_type_id must be a valid UUID") from exc

        if target_type_id is not None:
            try:
                uuid.UUID(target_type_id)
            except ValueError as exc:
                raise ValueError("target_type_id must be a valid UUID") from exc

        params = {}
        if co_role is not None:
            params["coRole"] = co_role
        if count_limit != -1:
            params["countLimit"] = count_limit
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if role is not None:
            params["role"] = role
        if role_co_role_logical_operator is not None:
            params["roleCoRoleLogicalOperator"] = role_co_role_logical_operator
        if sort_field != "ROLE":
            params["sortField"] = sort_field
        if sort_order != "ASC":
            params["sortOrder"] = sort_order
        if source_type_id is not None:
            params["sourceTypeId"] = source_type_id
        if source_type_name is not None:
            params["sourceTypeName"] = source_type_name
        if target_type_id is not None:
            params["targetTypeId"] = target_type_id
        if target_type_name is not None:
            params["targetTypeName"] = target_type_name

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_relation_type(self, relation_type_id: str):
        """
        Returns the relation type identified by the given UUID.
        :param relation_type_id: The UUID of the relation type.
        :return: Relation type details.
        """
        if not relation_type_id:
            raise ValueError("relation_type_id is required")
        try:
            uuid.UUID(relation_type_id)
        except ValueError as exc:
            raise ValueError("relation_type_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{relation_type_id}")
        return self._handle_response(response)

    def get_relation_type_by_public_id(self, public_id: str):
        """
        Returns the relation type identified by the given public ID.
        :param public_id: The public ID of the relation type.
        :return: Relation type details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/publicId/{public_id}")
        return self._handle_response(response)

    def add_relation_type(self, role: str, source_type_id: str, target_type_id: str,
                           co_role: str = None):
        """
        Adds a new relation type.
        :param role: The role label from source to target (required).
        :param source_type_id: The UUID of the source asset type (required).
        :param target_type_id: The UUID of the target asset type (required).
        :param co_role: The coRole label from target to source.
        :return: Created relation type details.
        """
        if not role or not source_type_id or not target_type_id:
            raise ValueError("role, source_type_id, and target_type_id are required")

        for param_name, param_value in [("source_type_id", source_type_id),
                                         ("target_type_id", target_type_id)]:
            try:
                uuid.UUID(param_value)
            except ValueError as exc:
                raise ValueError(f"{param_name} must be a valid UUID") from exc

        data = {
            "role": role,
            "sourceTypeId": source_type_id,
            "targetTypeId": target_type_id,
        }
        if co_role is not None:
            data["coRole"] = co_role

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_relation_types(self, relation_types: list):
        """
        Adds multiple new relation types in one go.
        :param relation_types: List of relation type objects.
        :return: Created relation types.
        """
        if not relation_types or not isinstance(relation_types, list):
            raise ValueError("relation_types must be a non-empty list")
        response = self._post(url=f"{self.__base_api}/bulk", data={"relationTypes": relation_types})
        return self._handle_response(response)

    def change_relation_type(self, relation_type_id: str, role: str = None, co_role: str = None,
                              source_type_id: str = None, target_type_id: str = None):
        """
        Changes the relation type with the given ID.
        :param relation_type_id: The UUID of the relation type to change.
        :param role: Optional new role label.
        :param co_role: Optional new coRole label.
        :param source_type_id: Optional new source asset type UUID.
        :param target_type_id: Optional new target asset type UUID.
        :return: Updated relation type details.
        """
        if not relation_type_id:
            raise ValueError("relation_type_id is required")
        try:
            uuid.UUID(relation_type_id)
        except ValueError as exc:
            raise ValueError("relation_type_id must be a valid UUID") from exc

        data = {}
        if role is not None:
            data["role"] = role
        if co_role is not None:
            data["coRole"] = co_role
        if source_type_id is not None:
            try:
                uuid.UUID(source_type_id)
            except ValueError as exc:
                raise ValueError("source_type_id must be a valid UUID") from exc
            data["sourceTypeId"] = source_type_id
        if target_type_id is not None:
            try:
                uuid.UUID(target_type_id)
            except ValueError as exc:
                raise ValueError("target_type_id must be a valid UUID") from exc
            data["targetTypeId"] = target_type_id

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{relation_type_id}", data=data)
        return self._handle_response(response)

    def change_relation_types(self, relation_types: list):
        """
        Changes multiple relation types in one go.
        :param relation_types: List of relation type change objects (must include id).
        :return: Updated relation types.
        """
        if not relation_types or not isinstance(relation_types, list):
            raise ValueError("relation_types must be a non-empty list")
        response = self._patch(url=f"{self.__base_api}/bulk", data={"relationTypes": relation_types})
        return self._handle_response(response)

    def remove_relation_type(self, relation_type_id: str):
        """
        Removes the relation type identified by the given UUID.
        :param relation_type_id: The UUID of the relation type.
        :return: None
        """
        if not relation_type_id:
            raise ValueError("relation_type_id is required")
        try:
            uuid.UUID(relation_type_id)
        except ValueError as exc:
            raise ValueError("relation_type_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{relation_type_id}")
        return self._handle_response(response)

    def remove_relation_types(self, relation_type_ids: list):
        """
        Removes multiple relation types.
        :param relation_type_ids: List of relation type UUIDs to remove.
        :return: None
        """
        if not relation_type_ids or not isinstance(relation_type_ids, list):
            raise ValueError("relation_type_ids must be a non-empty list")
        response = self._delete(url=f"{self.__base_api}/bulk")
        return self._handle_response(response)
