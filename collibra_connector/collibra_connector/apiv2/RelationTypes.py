import uuid
from .Base import BaseAPI


class RelationTypes(BaseAPI):
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
        :param co_role: The name of the role that the target plays.
        :param count_limit: Limit elements counted. -1 counts all,
            0 skips count.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param role: The name of the role that the source plays.
        :param role_co_role_logical_operator: Logical operator for
            role/coRole filter. Options: AND, OR
        :param sort_field: Field to sort by. Options: ROLE, CO_ROLE
        :param sort_order: Sort order (ASC or DESC).
        :param source_type_id: UUID of the source type to filter by.
        :param source_type_name: Name of the source type to filter by.
        :param target_type_id: UUID of the target type to filter by.
        :param target_type_name: Name of the target type to filter by.
        :return: List of relation types.
        """
        if sort_order not in ["ASC", "DESC"]:
            raise ValueError("sort_order must be 'ASC' or 'DESC'")
        valid_sort_fields = ["ROLE", "CO_ROLE"]
        if sort_field not in valid_sort_fields:
            raise ValueError(
                f"sort_field must be one of: {', '.join(valid_sort_fields)}"
            )
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if role_co_role_logical_operator is not None:
            valid_operators = ["AND", "OR"]
            if role_co_role_logical_operator not in valid_operators:
                raise ValueError(
                    "role_co_role_logical_operator must be one of: "
                    f"{', '.join(valid_operators)}"
                )

        if source_type_id is not None:
            try:
                uuid.UUID(source_type_id)
            except ValueError as exc:
                raise ValueError(
                    "source_type_id must be a valid UUID"
                ) from exc

        if target_type_id is not None:
            try:
                uuid.UUID(target_type_id)
            except ValueError as exc:
                raise ValueError(
                    "target_type_id must be a valid UUID"
                ) from exc

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
            params["roleCoRoleLogicalOperator"] = (
                role_co_role_logical_operator
            )
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
            raise ValueError(
                "relation_type_id must be a valid UUID"
            ) from exc

        response = self._get(
            url=f"{self.__base_api}/{relation_type_id}"
        )
        return self._handle_response(response)

    def get_relation_type_by_public_id(self, public_id: str):
        """
        Returns the relation type identified by the given public ID.
        :param public_id: The public ID of the relation type.
        :return: Relation type details.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(
            url=f"{self.__base_api}/publicId/{public_id}"
        )
        return self._handle_response(response)

    def add_relation_type(
        self,
        role: str,
        co_role: str,
        source_type_id: str,
        target_type_id: str,
        relation_type_id: str = None,
        public_id: str = None,
        description: str = None,
        id: str = None
    ):
        """
        Adds a new relation type.
        :param role: The name of the role the source plays (required).
        :param co_role: The name of the role the target plays (required).
        :param source_type_id: UUID of the source asset type (required).
        :param target_type_id: UUID of the target asset type (required).
        :param relation_type_id: Optional UUID for the new relation type.
        :param public_id: Optional public ID for the new relation type.
        :param description: Optional description.
        :param id: Optional ID for the new relation type.
        :return: Created relation type details.
        """
        if not role:
            raise ValueError("role is required")
        if not co_role:
            raise ValueError("co_role is required")
        if not source_type_id:
            raise ValueError("source_type_id is required")
        if not target_type_id:
            raise ValueError("target_type_id is required")

        for param_name, param_value in [
            ("source_type_id", source_type_id),
            ("target_type_id", target_type_id),
        ]:
            try:
                uuid.UUID(param_value)
            except ValueError as exc:
                raise ValueError(
                    f"{param_name} must be a valid UUID"
                ) from exc

        if relation_type_id is not None:
            try:
                uuid.UUID(relation_type_id)
            except ValueError as exc:
                raise ValueError(
                    "relation_type_id must be a valid UUID"
                ) from exc

        data = {
            "role": role,
            "coRole": co_role,
            "sourceTypeId": source_type_id,
            "targetTypeId": target_type_id,
            "id": id
        }
        if relation_type_id is not None:
            data["relationTypeId"] = relation_type_id
        if public_id is not None:
            data["publicId"] = public_id
        if description is not None:
            data["description"] = description

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def add_relation_types(self, relation_types: list):
        """
        Adds multiple new relation types in one go.
        :param relation_types: List of AddRelationTypeRequest objects.
        :return: Created relation types.
        """
        if not relation_types or not isinstance(relation_types, list):
            raise ValueError("relation_types must be a non-empty list")
        response = self._post(
            url=f"{self.__base_api}/bulk", data=relation_types
        )
        return self._handle_response(response)

    def change_relation_type(
        self,
        relation_type_id: str,
        role: str = None,
        co_role: str = None,
        source_type_id: str = None,
        target_type_id: str = None,
        public_id: str = None,
        description: str = None,
        id: str = None
    ):
        """
        Changes the relation type with the given ID.
        :param relation_type_id: The UUID of the relation type to change.
        :param role: Optional new role label.
        :param co_role: Optional new coRole label.
        :param source_type_id: Optional new source asset type UUID.
        :param target_type_id: Optional new target asset type UUID.
        :param public_id: Optional new public ID.
        :param description: Optional new description.
        :param id: Optional new ID for the relation type.
        :return: Updated relation type details.
        """
        if not relation_type_id:
            raise ValueError("relation_type_id is required")
        try:
            uuid.UUID(relation_type_id)
        except ValueError as exc:
            raise ValueError(
                "relation_type_id must be a valid UUID"
            ) from exc

        data = {}
        if role is not None:
            data["role"] = role
        if co_role is not None:
            data["coRole"] = co_role
        if source_type_id is not None:
            try:
                uuid.UUID(source_type_id)
            except ValueError as exc:
                raise ValueError(
                    "source_type_id must be a valid UUID"
                ) from exc
            data["sourceTypeId"] = source_type_id
        if target_type_id is not None:
            try:
                uuid.UUID(target_type_id)
            except ValueError as exc:
                raise ValueError(
                    "target_type_id must be a valid UUID"
                ) from exc
            data["targetTypeId"] = target_type_id
        if public_id is not None:
            data["publicId"] = public_id
        if description is not None:
            data["description"] = description
        if id is not None:
            data["id"] = id

        if not data:
            raise ValueError(
                "At least one field to change must be provided"
            )

        response = self._patch(
            url=f"{self.__base_api}/{relation_type_id}", data=data
        )
        return self._handle_response(response)

    def change_relation_types(self, relation_types: list):
        """
        Changes multiple relation types in one go.
        :param relation_types: List of ChangeRelationTypeRequest objects
            (each must include id).
        :return: Updated relation types.
        """
        if not relation_types or not isinstance(relation_types, list):
            raise ValueError("relation_types must be a non-empty list")
        response = self._patch(
            url=f"{self.__base_api}/bulk", data=relation_types
        )
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
            raise ValueError(
                "relation_type_id must be a valid UUID"
            ) from exc

        response = self._delete(
            url=f"{self.__base_api}/{relation_type_id}"
        )
        return self._handle_response(response)

    def remove_relation_types(self, relation_type_ids: list):
        """
        Removes multiple relation types.
        :param relation_type_ids: List of relation type UUIDs to remove.
        :return: None
        """
        if not relation_type_ids or not isinstance(
            relation_type_ids, list
        ):
            raise ValueError(
                "relation_type_ids must be a non-empty list"
            )

        for rid in relation_type_ids:
            try:
                uuid.UUID(rid)
            except ValueError as exc:
                raise ValueError(
                    f"{rid} is not a valid UUID"
                ) from exc

        response = self._delete(
            url=f"{self.__base_api}/bulk", data=relation_type_ids
        )
        return self._handle_response(response)
