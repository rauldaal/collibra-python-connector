import uuid
from typing import Any, List, Optional, Dict
from .Base import BaseAPI


class ComplexRelations(BaseAPI):
    """API class for complex relation operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/complexRelations"

    def find_complex_relations(
        self,
        offset: int = 0,
        limit: int = 0,
        count_limit: int = -1,
        cursor: str = None,
        asset_id: str = None,
        type_id: str = None,
        type_public_ids: List[str] = None
    ) -> Dict[str, Any]:
        """
        Searches for complex relations based on the provided criteria.

        :param offset: The starting point for the search results (default: 0).
        :param limit: The maximum number of results to return (default: 0, meaning no limit).
        :param count_limit: The maximum number of results to count (default: -1, meaning no limit).
        :param cursor: The cursor for paginated results (optional).
        :param asset_id: The UUID of the asset to filter by (optional).
        :param type_id: The UUID of the complex relation type to filter by (optional).
        :param type_public_ids: A list of public IDs for the complex relation types to filter by (optional).
        :return: A dictionary containing the matching complex relations.
        """
        params = {
            "offset": offset,
            "limit": limit,
            "countLimit": count_limit,
            "cursor": cursor,
            "assetId": asset_id,
            "typeId": type_id,
            "typePublicIds": type_public_ids
        }
        
        if asset_id and not self._uuid_validation(asset_id):
            raise ValueError("assetId must be a valid UUID")
        if type_id and not self._uuid_validation(type_id):
            raise ValueError("typeId must be a valid UUID")

        response = self._get(url=self.__base_api, params=params)
        return self._handle_response(response)

    def add_complex_relation(
        self,
        complex_relation_type_id: str,
        legs: List[Dict[str, Any]],
        attributes: List[Dict[str, Any]] = None,
        attributes_for_type_public_id: str = None,
        complex_relation_type_public_id: str = None,
        id: str = None
    ) -> Dict[str, Any]:
        """
        Creates a new complex relation with the specified parameters.

        :param complex_relation_type_id: The UUID of the complex relation type (required).
        :param legs: A list of dictionaries representing the legs of the complex relation (required).
        :param attributes: A list of dictionaries representing the attributes of the complex relation (optional).
        :param attributes_for_type_public_id: The public ID for the attributes of the complex relation type (optional).
        :param complex_relation_type_public_id: The public ID of the complex relation type (optional).
        :param id: The unique identifier of the complex relation (optional).
        :return: A dictionary containing the details of the created complex relation.
        """
        if not complex_relation_type_id or not legs:
            raise ValueError("complex_relation_type_id and legs are required")

        data = {
            "typeId": complex_relation_type_id,
            "legs": legs,
            "attributes": attributes,
            "attributesForTypePublicId": attributes_for_type_public_id,
            "complexRelationTypePublicId": complex_relation_type_public_id,
            "id": id
        }

        if not self._uuid_validation(complex_relation_type_id):
            raise ValueError("complex_relation_type_id must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)

    def get_complex_relation(self, complex_relation_id: str) -> Dict[str, Any]:
        """
        Retrieves the details of a complex relation by its ID.

        :param complex_relation_id: The unique identifier of the complex relation to retrieve.
        :return: A dictionary containing the details of the complex relation.
        """
        if not self._uuid_validation(complex_relation_id):
            raise ValueError("complex_relation_id must be a valid UUID")
        
        response = self._get(url=f"{self.__base_api}/{complex_relation_id}")
        return self._handle_response(response)

    def remove_complex_relation(self, complex_relation_id: str) -> None:
        """
        Deletes a complex relation identified by its ID.

        :param complex_relation_id: The unique identifier of the complex relation to delete.
        """
        if not self._uuid_validation(complex_relation_id):
            raise ValueError("complex_relation_id must be a valid UUID")
        
        response = self._delete(url=f"{self.__base_api}/{complex_relation_id}")
        return self._handle_response(response)

    def change_complex_relation(
        self,
        complex_relation_id: str,
        legs: List[Dict[str, Any]] = None,
        attributes: List[Dict[str, Any]] = None,
        relations: List[Dict[str, Any]] = None,
        attributes_for_type_public_id: str = None,
        partial_attributes_update: bool = None
    ) -> Dict[str, Any]:
        """
        Updates the details of a complex relation identified by its ID.

        :param complex_relation_id: The unique identifier of the complex relation to update.
        :param legs: The new list of legs for the complex relation (optional).
        :param attributes: The new list of attributes for the complex relation (optional).
        :param relations: The new list of relations for the complex relation (optional).
        :param attributes_for_type_public_id: The public ID for the attributes of the complex relation type (optional).
        :param partial_attributes_update: Whether to perform a partial update of attributes (optional).
        :return: A dictionary containing the updated details of the complex relation.
        """
        if not self._uuid_validation(complex_relation_id):
            raise ValueError("complex_relation_id must be a valid UUID")
        
        data = {}
        if legs is not None:
            data["legs"] = legs
        if attributes is not None:
            data["attributes"] = attributes
        if relations is not None:
            data["relations"] = relations
        if attributes_for_type_public_id is not None:
            data["attributesForTypePublicId"] = attributes_for_type_public_id
        if partial_attributes_update is not None:
            data["partialAttributesUpdate"] = partial_attributes_update

        response = self._patch(url=f"{self.__base_api}/{complex_relation_id}", data=data)
        return self._handle_response(response)

    def export_complex_relations_to_csv(self, type_id: str, **kwargs) -> Dict[str, Any]:
        """
        Initiates a job to export complex relations of the given type to a CSV file.

        :param type_id: The UUID of the complex relation type to export.
        :param kwargs: Additional parameters for the export job.
        :return: A dictionary containing the details of the export job.
        """
        if not self._uuid_validation(type_id):
            raise ValueError("type_id must be a valid UUID")

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/csv-job", data=data)
        return self._handle_response(response)

    def export_complex_relations_to_csv_as_string(self, type_id: str, **kwargs) -> str:
        """
        Exports all complex relations of the given type to a CSV string.

        :param type_id: The UUID of the complex relation type to export.
        :param kwargs: Additional parameters for the export.
        :return: A string containing the CSV data.
        """
        if not self._uuid_validation(type_id):
            raise ValueError("type_id must be a valid UUID")

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/csv", data=data)
        return response.text

    def export_complex_relations_to_csv_file(self, type_id: str, **kwargs) -> Dict[str, Any]:
        """
        Exports all complex relations of the given type to a CSV file.

        :param type_id: The UUID of the complex relation type to export.
        :param kwargs: Additional parameters for the export.
        :return: A dictionary containing the details of the export job.
        """
        if not self._uuid_validation(type_id):
            raise ValueError("type_id must be a valid UUID")

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/csv-file", data=data)
        return self._handle_response(response)

    def export_complex_relations_to_excel(self, type_id: str, **kwargs) -> Dict[str, Any]:
        """
        Initiates a job to export complex relations of the given type to an Excel file.

        :param type_id: The UUID of the complex relation type to export.
        :param kwargs: Additional parameters for the export job.
        :return: A dictionary containing the details of the export job.
        """
        if not self._uuid_validation(type_id):
            raise ValueError("type_id must be a valid UUID")

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/excel-job", data=data)
        return self._handle_response(response)

    def export_complex_relations_to_excel_file(self, type_id: str, **kwargs) -> Dict[str, Any]:
        """
        Exports all complex relations of the given type to an Excel file.

        :param type_id: The UUID of the complex relation type to export.
        :param kwargs: Additional parameters for the export.
        :return: A dictionary containing the details of the export job.
        """
        if not self._uuid_validation(type_id):
            raise ValueError("type_id must be a valid UUID")

        data = {"typeId": type_id, **kwargs}
        response = self._post(url=f"{self.__base_api}/export/excel-file", data=data)
        return self._handle_response(response)

    def export_to_csv(
        self,
        complex_relation_type_id: str,
        include_header_row: bool = True,
        separator: str = ",",
        domain_id: str = None,
        store_as_attachment: bool = False,
        quote: str = '"',
        file_name: str = None,
        support_roundtrip: bool = False,
        escape: str = "\\",
        remove_formatting: bool = False
    ) -> Dict[str, Any]:
        """
        Initiates a job to export complex relations to a CSV file.

        :param complex_relation_type_id: The UUID of the complex relation type to export.
        :param include_header_row: Whether to include a header row in the CSV file (default: True).
        :param separator: The separator character for the CSV file (default: ",").
        :param domain_id: The UUID of the domain to filter by (optional).
        :param store_as_attachment: Whether to store the CSV file as an attachment (default: False).
        :param quote: The quote character for the CSV file (default: '"').
        :param file_name: The name of the CSV file (optional).
        :param support_roundtrip: Whether to support roundtrip export (default: False).
        :param escape: The escape character for the CSV file (default: "\\").
        :param remove_formatting: Whether to remove formatting from the CSV data (default: False).
        :return: A dictionary containing the details of the export job.
        """
        data = {
            "complexRelationTypeId": complex_relation_type_id,
            "includeHeaderRow": include_header_row,
            "separator": separator,
            "domainId": domain_id,
            "storeAsAttachment": store_as_attachment,
            "quote": quote,
            "fileName": file_name,
            "supportRoundtrip": support_roundtrip,
            "escape": escape,
            "removeFormatting": remove_formatting
        }
        response = self._post(url=f"{self.__base_api}/export/csv-job", data=data)
        return self._handle_response(response)

    def export_to_csv_as_string(
        self,
        complex_relation_type_id: str,
        include_header_row: bool = True,
        separator: str = ",",
        domain_id: str = None,
        store_as_attachment: bool = False,
        quote: str = '"',
        file_name: str = None,
        support_roundtrip: bool = False,
        escape: str = "\\",
        remove_formatting: bool = False
    ) -> str:
        """
        Exports complex relations to a CSV string.

        :param complex_relation_type_id: The UUID of the complex relation type to export.
        :param include_header_row: Whether to include a header row in the CSV string (default: True).
        :param separator: The separator character for the CSV string (default: ",").
        :param domain_id: The UUID of the domain to filter by (optional).
        :param store_as_attachment: Whether to store the CSV string as an attachment (default: False).
        :param quote: The quote character for the CSV string (default: '"').
        :param file_name: The name of the CSV string (optional).
        :param support_roundtrip: Whether to support roundtrip export (default: False).
        :param escape: The escape character for the CSV string (default: "\\").
        :param remove_formatting: Whether to remove formatting from the CSV data (default: False).
        :return: A string containing the CSV data.
        """
        data = {
            "complexRelationTypeId": complex_relation_type_id,
            "includeHeaderRow": include_header_row,
            "separator": separator,
            "domainId": domain_id,
            "storeAsAttachment": store_as_attachment,
            "quote": quote,
            "fileName": file_name,
            "supportRoundtrip": support_roundtrip,
            "escape": escape,
            "removeFormatting": remove_formatting
        }
        response = self._post(url=f"{self.__base_api}/export/csv", data=data)
        return response.text

    def export_to_csv_without_job(
        self,
        complex_relation_type_id: str,
        include_header_row: bool = True,
        separator: str = ",",
        domain_id: str = None,
        store_as_attachment: bool = False,
        quote: str = '"',
        file_name: str = None,
        support_roundtrip: bool = False,
        escape: str = "\\",
        remove_formatting: bool = False
    ) -> bytes:
        """
        Exports complex relations to a CSV file directly without a job.

        :param complex_relation_type_id: The UUID of the complex relation type to export.
        :param include_header_row: Whether to include a header row in the CSV file (default: True).
        :param separator: The separator character for the CSV file (default: ",").
        :param domain_id: The UUID of the domain to filter by (optional).
        :param store_as_attachment: Whether to store the CSV file as an attachment (default: False).
        :param quote: The quote character for the CSV file (default: '"').
        :param file_name: The name of the CSV file (optional).
        :param support_roundtrip: Whether to support roundtrip export (default: False).
        :param escape: The escape character for the CSV file (default: "\\").
        :param remove_formatting: Whether to remove formatting from the CSV data (default: False).
        :return: A bytes object containing the CSV data.
        """
        data = {
            "complexRelationTypeId": complex_relation_type_id,
            "includeHeaderRow": include_header_row,
            "separator": separator,
            "domainId": domain_id,
            "storeAsAttachment": store_as_attachment,
            "quote": quote,
            "fileName": file_name,
            "supportRoundtrip": support_roundtrip,
            "escape": escape,
            "removeFormatting": remove_formatting
        }
        response = self._post(url=f"{self.__base_api}/export/csv-file", data=data)
        return response.content

    def export_to_excel(
        self,
        complex_relation_type_id: str,
        include_header_row: bool = True,
        sheet_name: str = "Sheet1",
        domain_id: str = None,
        store_as_attachment: bool = False,
        file_name: str = None,
        support_roundtrip: bool = False,
        xlsx: bool = True,
        remove_formatting: bool = False
    ) -> Dict[str, Any]:
        """
        Initiates a job to export complex relations to an Excel file.

        :param complex_relation_type_id: The UUID of the complex relation type to export.
        :param include_header_row: Whether to include a header row in the Excel file (default: True).
        :param sheet_name: The name of the sheet to export to (default: "Sheet1").
        :param domain_id: The UUID of the domain to filter by (optional).
        :param store_as_attachment: Whether to store the Excel file as an attachment (default: False).
        :param file_name: The name of the Excel file (optional).
        :param support_roundtrip: Whether to support roundtrip export (default: False).
        :param xlsx: Whether to export in XLSX format (default: True).
        :param remove_formatting: Whether to remove formatting from the Excel data (default: False).
        :return: A dictionary containing the details of the export job.
        """
        data = {
            "complexRelationTypeId": complex_relation_type_id,
            "includeHeaderRow": include_header_row,
            "sheetName": sheet_name,
            "domainId": domain_id,
            "storeAsAttachment": store_as_attachment,
            "fileName": file_name,
            "supportRoundtrip": support_roundtrip,
            "xlsx": xlsx,
            "removeFormatting": remove_formatting
        }
        response = self._post(url=f"{self.__base_api}/export/excel-job", data=data)
        return self._handle_response(response)

    def export_to_excel_without_job(
        self,
        complex_relation_type_id: str,
        include_header_row: bool = True,
        sheet_name: str = "Sheet1",
        domain_id: str = None,
        store_as_attachment: bool = False,
        file_name: str = None,
        support_roundtrip: bool = False,
        xlsx: bool = True,
        remove_formatting: bool = False
    ) -> bytes:
        """
        Exports complex relations to an Excel file directly without a job.

        :param complex_relation_type_id: The UUID of the complex relation type to export.
        :param include_header_row: Whether to include a header row in the Excel file (default: True).
        :param sheet_name: The name of the sheet to export to (default: "Sheet1").
        :param domain_id: The UUID of the domain to filter by (optional).
        :param store_as_attachment: Whether to store the Excel file as an attachment (default: False).
        :param file_name: The name of the Excel file (optional).
        :param support_roundtrip: Whether to support roundtrip export (default: False).
        :param xlsx: Whether to export in XLSX format (default: True).
        :param remove_formatting: Whether to remove formatting from the Excel data (default: False).
        :return: A bytes object containing the Excel data.
        """
        data = {
            "complexRelationTypeId": complex_relation_type_id,
            "includeHeaderRow": include_header_row,
            "sheetName": sheet_name,
            "domainId": domain_id,
            "storeAsAttachment": store_as_attachment,
            "fileName": file_name,
            "supportRoundtrip": support_roundtrip,
            "xlsx": xlsx,
            "removeFormatting": remove_formatting
        }
        response = self._post(url=f"{self.__base_api}/export/excel-file", data=data)
        return response.content
