import uuid
from .Base import BaseAPI


class ComplexRelation(BaseAPI):
    """API class for complex relation operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/complexRelations"

    def find_complex_relations(
        self,
        asset_id: str = None,
        count_limit: int = -1,
        cursor: str = None,
        limit: int = 0,
        offset: int = 0,
        type_id: str = None,
        type_public_ids: list = None,
    ):
        """
        Returns complex relations matching the given search criteria.
        :param asset_id: UUID of the asset to filter by.
        :param count_limit: Limit elements counted.
        :param cursor: Cursor for pagination.
        :param limit: Maximum results to retrieve (0 = default, max 1000).
        :param offset: First result to retrieve.
        :param type_id: UUID of the complex relation type to filter by.
        :param type_public_ids: List of complex relation type public IDs to filter by.
        :return: List of complex relations.
        """
        if limit < 0 or limit > 1000:
            raise ValueError("limit must be between 0 and 1000")

        if asset_id is not None:
            try:
                uuid.UUID(asset_id)
            except ValueError as exc:
                raise ValueError("asset_id must be a valid UUID") from exc

        if type_id is not None:
            try:
                uuid.UUID(type_id)
            except ValueError as exc:
                raise ValueError("type_id must be a valid UUID") from exc

        params = {}
        if asset_id is not None:
            params["assetId"] = asset_id
        if count_limit != -1:
            params["countLimit"] = count_limit
        if cursor is not None:
            params["cursor"] = cursor
        if limit != 0:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if type_id is not None:
            params["typeId"] = type_id
        if type_public_ids is not None:
            params["typePublicIds"] = type_public_ids

        response = self._get(url=self.__base_api, params=params or None)
        return self._handle_response(response)

    def get_complex_relation(self, complex_relation_id: str):
        """
        Returns the complex relation identified by the given UUID.
        :param complex_relation_id: The UUID of the complex relation.
        :return: Complex relation details.
        """
        if not complex_relation_id:
            raise ValueError("complex_relation_id is required")
        try:
            uuid.UUID(complex_relation_id)
        except ValueError as exc:
            raise ValueError("complex_relation_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/{complex_relation_id}")
        return self._handle_response(response)

    def add_complex_relation(self, type_id: str, legs: list, attributes: list = None):
        """
        Adds a new complex relation.
        :param type_id: The UUID of the complex relation type (required).
        :param legs: List of leg objects defining assets in the complex relation (required).
        :param attributes: Optional list of attribute objects.
        :return: Created complex relation details.
        """
        if not type_id:
            raise ValueError("type_id is required")
        try:
            uuid.UUID(type_id)
        except ValueError as exc:
            raise ValueError("type_id must be a valid UUID") from exc

        if not legs or not isinstance(legs, list):
            raise ValueError("legs must be a non-empty list")

        data = {"typeId": type_id, "legs": legs}
        if attributes is not None:
            data["attributes"] = attributes

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def change_complex_relation(self, complex_relation_id: str, legs: list = None,
                                 attributes: list = None):
        """
        Changes the complex relation with the given ID.
        :param complex_relation_id: The UUID of the complex relation to change.
        :param legs: Optional new list of leg objects.
        :param attributes: Optional new list of attribute objects.
        :return: Updated complex relation details.
        """
        if not complex_relation_id:
            raise ValueError("complex_relation_id is required")
        try:
            uuid.UUID(complex_relation_id)
        except ValueError as exc:
            raise ValueError("complex_relation_id must be a valid UUID") from exc

        data = {}
        if legs is not None:
            data["legs"] = legs
        if attributes is not None:
            data["attributes"] = attributes

        if not data:
            raise ValueError("At least one field to change must be provided")

        response = self._patch(url=f"{self.__base_api}/{complex_relation_id}", data=data)
        return self._handle_response(response)

    def remove_complex_relation(self, complex_relation_id: str):
        """
        Removes the complex relation identified by the given UUID.
        :param complex_relation_id: The UUID of the complex relation.
        :return: None
        """
        if not complex_relation_id:
            raise ValueError("complex_relation_id is required")
        try:
            uuid.UUID(complex_relation_id)
        except ValueError as exc:
            raise ValueError("complex_relation_id must be a valid UUID") from exc

        response = self._delete(url=f"{self.__base_api}/{complex_relation_id}")
        return self._handle_response(response)

    def export_to_csv(self, type_id: str, **kwargs):
        """
        Export complex relations of the given type to CSV (returns async job).
        :param type_id: The UUID of the complex relation type.
        :return: Job details.
        """
        if not type_id:
            raise ValueError("type_id is required")
        try:
            uuid.UUID(type_id)
        except ValueError as exc:
            raise ValueError("type_id must be a valid UUID") from exc

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/csv-job", data=data)
        return self._handle_response(response)

    def export_to_csv_file(self, type_id: str, **kwargs):
        """
        Export all complex relations of the given type to a CSV file.
        :param type_id: The UUID of the complex relation type.
        :return: File details.
        """
        if not type_id:
            raise ValueError("type_id is required")
        try:
            uuid.UUID(type_id)
        except ValueError as exc:
            raise ValueError("type_id must be a valid UUID") from exc

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/csv-file", data=data)
        return self._handle_response(response)

    def export_to_excel(self, type_id: str, **kwargs):
        """
        Export complex relations of the given type to Excel (returns async job).
        :param type_id: The UUID of the complex relation type.
        :return: Job details.
        """
        if not type_id:
            raise ValueError("type_id is required")
        try:
            uuid.UUID(type_id)
        except ValueError as exc:
            raise ValueError("type_id must be a valid UUID") from exc

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/excel-job", data=data)
        return self._handle_response(response)

    def export_to_excel_file(self, type_id: str, **kwargs):
        """
        Export all complex relations of the given type to an Excel file.
        :param type_id: The UUID of the complex relation type.
        :return: File details.
        """
        if not type_id:
            raise ValueError("type_id is required")
        try:
            uuid.UUID(type_id)
        except ValueError as exc:
            raise ValueError("type_id must be a valid UUID") from exc

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/excel-file", data=data)
        return self._handle_response(response)
