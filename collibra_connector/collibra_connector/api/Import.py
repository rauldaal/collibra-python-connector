from typing import Optional, Dict, Any, List
import requests
from .Base import BaseAPI


class Import(BaseAPI):
    """
    Import API module for Collibra.

    Handles large volume import operations and synchronization of data.
    Provides endpoints for CSV, Excel, and JSON imports with both full
    and batch synchronization capabilities.
    """

    def __init__(self, connector):
        super().__init__(connector)
        self.__connector = connector
        self.__base_api = connector.api + "/import"
        self.__header = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def _post_multipart(
        self,
        url: str,
        data: Dict[str, Any] = None,
        files: Dict[str, Any] = None,
        params: Dict[str, Any] = None
    ) -> requests.Response:
        """
        Makes a POST request with multipart/form-data support for file uploads.
        """
        return requests.post(
            url,
            auth=self.__connector.auth,
            data=data,
            files=files,
            params=params,
            timeout=self.__connector.timeout
        )

    # ========== IMPORT ENDPOINTS ==========

    def import_csv_in_job(
        self,
        template: str,
        escape: str,
        quote: str,
        separator: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None,
        strict_quotes: Optional[bool] = None,
        ignore_leading_whitespace: Optional[bool] = None,
        header_row: Optional[bool] = None
    ) -> requests.Response:
        """
        Starts import from the CSV file in job (asynchronously).

        Request can either accept id of the uploaded file that contains CSV input
        which should be used for import - or the file itself.

        Args:
            template: The template that should be used for parsing and importing the CSV file. (required)
            escape: The delimiter character used to escape separator or quote character. (required)
            quote: The delimiter character used for quoted entries. (required)
            separator: The delimiter character used to separate entries. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains CSV input which should be used for import.
            file: The CSV file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file from the Collibra Platform if the import job is successful.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.
            strict_quotes: Whether characters outside quotes should be ignored. Defaults to false.
            ignore_leading_whitespace: Whether whitespace characters before quotes should be ignored. Defaults to false.
            header_row: Whether the first row of the imported CSV file is the header. Defaults to false.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/csv-job"
        data = {
            "template": template,
            "escape": escape,
            "quote": quote,
            "separator": separator
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action
        if strict_quotes is not None:
            data["strictQuotes"] = strict_quotes
        if ignore_leading_whitespace is not None:
            data["ignoreLeadingWhitespace"] = ignore_leading_whitespace
        if header_row is not None:
            data["headerRow"] = header_row

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    def import_excel_in_job(
        self,
        template: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None,
        sheet_name: Optional[str] = None,
        sheet_index: Optional[int] = None,
        header_row: Optional[bool] = None
    ) -> requests.Response:
        """
        Starts import from the Excel file in job (asynchronously).

        Request can either accept id of the uploaded file that contains Excel input
        which should be used for import - or the file itself.

        Args:
            template: The template that should be used for parsing and importing the
                      contents of the Excel file. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: Deprecated. The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains Excel input which should be used for import.
            file: The Excel file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file from the Collibra Platform if the import job is successful.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply (ADD_OR_IGNORE or REPLACE).
            attributes_action: The attributes action to apply (ADD_OR_IGNORE or REPLACE).
            sheet_name: The name of the Excel sheet. If null and sheet_index is null, the first sheet
                        is used. If null and sheet_index is not null, the sheet at that index is used.
            sheet_index: The index of the Excel sheet. If null and sheet_name is null, the first sheet
                         is used. If null and sheet_name is not null, the sheet with that name is used.
            header_row: Whether the first row of the imported Excel sheet is the header. Defaults to false.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/excel-job"
        data = {"template": template}

        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action
        if sheet_name is not None:
            data["sheetName"] = sheet_name
        if sheet_index is not None:
            data["sheetIndex"] = sheet_index
        if header_row is not None:
            data["headerRow"] = header_row

        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    # ========== IMPORT RESULTS ==========

    def find_import_errors(
        self,
        job_id: str,
        offset: int = 0,
        limit: int = 1000,
        count_limit: Optional[int] = None
    ) -> requests.Response:
        """
        List the errors of a finished import job.

        Returns a list of errors of a finished import job with the specified ID.
        By default the maximum number of results is limited to 1000.

        Args:
            job_id: The ID of the job.
            offset: The first result to retrieve. If not set (offset = 0), results
                    will be retrieved starting from row 0. Defaults to 0.
            limit: The maximum number of results to retrieve. If not set (limit = 0),
                   the default limit will be used. The maximum allowed limit is 1000.
                   Defaults to 1000.
            count_limit: Allows to limit the number of elements that will be counted.
                        -1 will count everything and 0 will cause the count to be skipped.
                        Optional.

        Returns:
            requests.Response: The response from the API containing import errors.
        """
        url = f"{self.__base_api}/results/{job_id}/errors"
        params = {
            "offset": offset,
            "limit": limit
        }
        if count_limit is not None:
            params["countLimit"] = count_limit
        return requests.get(
            url,
            auth=self.__connector.auth,
            params=params,
            timeout=self.__connector.timeout
        )

    def get_import_job_summary(self, job_id: str) -> requests.Response:
        """
        Retrieve a summary of a finished import job.

        Returns details about a finished import job with the specified ID such as
        the total number of resources and types of resources that were added,
        removed or updated and the number of errors.

        Args:
            job_id: The ID of the job.

        Returns:
            requests.Response: The response from the API containing the import job summary.
        """
        url = f"{self.__base_api}/results/{job_id}/summary"
        return requests.get(
            url,
            auth=self.__connector.auth,
            timeout=self.__connector.timeout
        )

    def synchronize_batch_csv_in_job(
        self,
        synchronization_id: str,
        template: str,
        escape: str,
        quote: str,
        separator: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None,
        strict_quotes: Optional[bool] = None,
        ignore_leading_whitespace: Optional[bool] = None,
        header_row: Optional[bool] = None
    ) -> requests.Response:
        """
        Starts batch synchronization from the CSV file in job (asynchronously).

        Request can either accept id of the uploaded file that contains CSV input
        which should be used for import or the file itself. The input file is treated as
        a part (batch) of synchronization process. After last batch, finalization (cleanup)
        process should be called.

        Args:
            synchronization_id: The synchronization id used to distinguish different synchronizations. (required)
            template: The template that should be used for parsing and synchronizing the CSV file. (required)
            escape: The delimiter character used to escape separator or quote character. (required)
            quote: The delimiter character used for quoted entries. (required)
            separator: The delimiter character used to separate entries. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains CSV input which should be used for synchronization.
            file: The CSV file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file after synchronization job is finished, regardless of the result.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.
            strict_quotes: Whether characters outside quotes should be ignored. Defaults to false.
            ignore_leading_whitespace: Whether whitespace characters before quotes should be ignored. Defaults to false.
            header_row: Whether the first row of the synchronized CSV file is the header. Defaults to false.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/batch/csv-job"
        data = {
            "synchronizationId": synchronization_id,
            "template": template,
            "escape": escape,
            "quote": quote,
            "separator": separator
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action
        if strict_quotes is not None:
            data["strictQuotes"] = strict_quotes
        if ignore_leading_whitespace is not None:
            data["ignoreLeadingWhitespace"] = ignore_leading_whitespace
        if header_row is not None:
            data["headerRow"] = header_row

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    def import_json_in_job(
        self,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None
    ) -> requests.Response:
        """
        Starts import from the JSON file in job (asynchronously).

        Request can either accept id of the uploaded file that contains JSON input
        which should be used for import - or the file itself.

        Args:
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains JSON input which should be used for import.
            file: The JSON file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file from the Collibra Platform if the import job is successful.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/json-job"
        data = {}

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    def find_synchronization_infos(
        self,
        offset: int = 0,
        limit: int = 1000,
        count_limit: Optional[int] = None
    ) -> requests.Response:
        """
        Returns synchronization information matching the given search criteria.

        Only parameters that are specified in this request and have not null values
        are used for filtering. All other parameters are ignored. The returned
        synchronization information satisfies all constraints that are specified in
        this search criteria. By default a result containing 1000 synchronization
        infos is returned.

        Args:
            offset: The first result to retrieve. If not set (offset = 0), results
                    will be retrieved starting from row 0. Defaults to 0.
            limit: The maximum number of results to retrieve. If not set (limit = 0),
                   the default limit will be used. The maximum allowed limit is 1000.
                   Defaults to 1000.
            count_limit: Allows to limit the number of elements that will be counted.
                        -1 will count everything and 0 will cause the count to be skipped.
                        Optional.

        Returns:
            requests.Response: The response from the API containing synchronization information.
        """
        url = f"{self.__base_api}/synchronize"
        params = {
            "offset": offset,
            "limit": limit
        }
        if count_limit is not None:
            params["countLimit"] = count_limit
        return requests.get(
            url,
            auth=self.__connector.auth,
            params=params,
            timeout=self.__connector.timeout
        )

    def exists(self, synchronization_id: str) -> requests.Response:
        """
        Checks if the synchronization with the provided id exists.

        Args:
            synchronization_id: The synchronization id of the operation.

        Returns:
            requests.Response: The response from the API (200 OK on success).
        """
        url = f"{self.__base_api}/synchronize/exists/{synchronization_id}"
        return requests.get(
            url,
            auth=self.__connector.auth,
            timeout=self.__connector.timeout
        )

    def remove_synchronization(self, synchronization_id: str) -> requests.Response:
        """
        Removes all information about synchronization process corresponding to provided synchronization id.

        This operation stops tracking of synchronization identified by provided synchronization id.
        The next synchronization process specified with this id will not be able to detect resources
        that should be removed.

        Args:
            synchronization_id: The synchronization id of the operation.

        Returns:
            requests.Response: The response from the API (204 No Content on success).
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}"
        return requests.delete(
            url,
            auth=self.__connector.auth,
            timeout=self.__connector.timeout
        )

    # ========== BATCH SYNCHRONIZATION ==========

    def synchronize_batch_excel_in_job(
        self,
        synchronization_id: str,
        template: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None,
        sheet_name: Optional[str] = None,
        sheet_index: Optional[int] = None,
        header_row: Optional[bool] = None
    ) -> requests.Response:
        """
        Starts batch synchronization from the Excel file in job (asynchronously).

        Request can either accept id of the uploaded file that contains Excel input
        which should be used for import or the file itself. The input file is treated as
        a part (batch) of synchronization process. After last batch, finalization (cleanup)
        process should be called.

        Args:
            synchronization_id: The synchronization id used to distinguish different synchronizations. (required)
            template: The template that should be used for parsing and synchronizing the Excel file. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains Excel input which should be used for synchronization.
            file: The Excel file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file after synchronization job is finished, regardless of the result.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.
            sheet_name: The name of the Excel sheet. If null and sheet_index is null, the first sheet is used.
            sheet_index: The index of the Excel sheet. If null and sheet_name is null, the first sheet is used.
            header_row: Whether the first row of the synchronized Excel sheet is the header. Defaults to false.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/batch/excel-job"
        data = {
            "synchronizationId": synchronization_id,
            "template": template
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action
        if sheet_name is not None:
            data["sheetName"] = sheet_name
        if sheet_index is not None:
            data["sheetIndex"] = sheet_index
        if header_row is not None:
            data["headerRow"] = header_row

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    def synchronize_batch_json_in_job(
        self,
        synchronization_id: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None
    ) -> requests.Response:
        """
        Starts batch synchronization from the JSON file in job (asynchronously).

        Request can either accept id of the uploaded file that contains JSON input
        which should be used for import or the file itself. The input file is treated as
        a part (batch) of synchronization process. After last batch, finalization (cleanup)
        process should be called.

        Args:
            synchronization_id: The synchronization id used to distinguish different synchronizations. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains JSON input which should be used for synchronization.
            file: The JSON file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file after synchronization job is finished, regardless of the result.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/batch/json-job"
        data = {
            "synchronizationId": synchronization_id
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    # ========== FULL SYNCHRONIZATION ==========

    def synchronize_excel_in_job(
        self,
        synchronization_id: str,
        template: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None,
        sheet_name: Optional[str] = None,
        sheet_index: Optional[int] = None,
        header_row: Optional[bool] = None,
        finalization_strategy: Optional[str] = None,
        missing_asset_status_id: Optional[str] = None
    ) -> requests.Response:
        """
        Starts full synchronization from the Excel file in job (asynchronously).

        Request can either accept id of the uploaded file that contains Excel input
        which should be used for import or the file itself. The input file is treated as
        a full input of synchronization process.

        Args:
            synchronization_id: The synchronization id used to distinguish different synchronizations. (required)
            template: The template that should be used for parsing and synchronizing the Excel file. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains Excel input which should be used for synchronization.
            file: The Excel file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file after synchronization job is finished, regardless of the result.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.
            sheet_name: The name of the Excel sheet. If null and sheet_index is null, the first sheet is used.
            sheet_index: The index of the Excel sheet. If null and sheet_name is null, the first sheet is used.
            header_row: Whether the first row of the synchronized Excel sheet is the header. Defaults to false.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/excel-job"
        data = {
            "synchronizationId": synchronization_id,
            "template": template
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action
        if sheet_name is not None:
            data["sheetName"] = sheet_name
        if sheet_index is not None:
            data["sheetIndex"] = sheet_index
        if header_row is not None:
            data["headerRow"] = header_row

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    def synchronize_csv_in_job(
        self,
        synchronization_id: str,
        template: str,
        escape: str,
        quote: str,
        separator: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None,
        strict_quotes: Optional[bool] = None,
        ignore_leading_whitespace: Optional[bool] = None,
        header_row: Optional[bool] = None,
        finalization_strategy: Optional[str] = None,
        missing_asset_status_id: Optional[str] = None
    ) -> requests.Response:
        """
        Starts full synchronization from the CSV file in job (asynchronously).

        Request can either accept id of the uploaded file that contains CSV input
        which should be used for import or the file itself. The input file is treated as
        a full input of synchronization process.

        Args:
            synchronization_id: The synchronization id used to distinguish different synchronizations. (required)
            template: The template that should be used for parsing and synchronizing the CSV file. (required)
            escape: The delimiter character used to escape separator or quote character. (required)
            quote: The delimiter character used for quoted entries. (required)
            separator: The delimiter character used to separate entries. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains CSV input which should be used for synchronization.
            file: The CSV file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file after synchronization job is finished, regardless of the result.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.
            strict_quotes: Whether characters outside quotes should be ignored. Defaults to false.
            ignore_leading_whitespace: Whether whitespace characters before quotes should be ignored. Defaults to false.
            header_row: Whether the first row of the synchronized CSV file is the header. Defaults to false.
            finalization_strategy: The synchronization finalization strategy (REMOVE_RESOURCES, CHANGE_STATUS, or IGNORE).
            missing_asset_status_id: The status id to use when finalization_strategy is CHANGE_STATUS.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/csv-job"
        data = {
            "synchronizationId": synchronization_id,
            "template": template,
            "escape": escape,
            "quote": quote,
            "separator": separator
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action
        if strict_quotes is not None:
            data["strictQuotes"] = strict_quotes
        if ignore_leading_whitespace is not None:
            data["ignoreLeadingWhitespace"] = ignore_leading_whitespace
        if header_row is not None:
            data["headerRow"] = header_row
        if finalization_strategy is not None:
            data["finalizationStrategy"] = finalization_strategy
        if missing_asset_status_id is not None:
            data["missingAssetStatusId"] = missing_asset_status_id

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    def synchronize_json_in_job(
        self,
        synchronization_id: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        file_id: Optional[str] = None,
        file: Optional[bytes] = None,
        file_name: Optional[str] = None,
        delete_file: Optional[bool] = None,
        continue_on_error: Optional[bool] = None,
        relations_action: Optional[str] = None,
        attributes_action: Optional[str] = None,
        finalization_strategy: Optional[str] = None,
        missing_asset_status_id: Optional[str] = None
    ) -> requests.Response:
        """
        Starts full synchronization from the JSON file in job (asynchronously).

        Request can either accept id of the uploaded file that contains JSON input
        which should be used for import or the file itself. The input file is treated as
        a full input of synchronization process.

        Args:
            synchronization_id: The synchronization id used to distinguish different synchronizations. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated parameter.
            file_id: The id of the uploaded file that contains JSON input which should be used for synchronization.
            file: The JSON file to upload.
            file_name: The name of the file to upload. If set, then also file should be provided.
            delete_file: Delete the file after synchronization job is finished, regardless of the result.
            continue_on_error: Whether the import should continue if some commands are invalid or failed.
            relations_action: The relations action to apply.
            attributes_action: The attributes action to apply.
            finalization_strategy: The synchronization finalization strategy (REMOVE_RESOURCES, CHANGE_STATUS, or IGNORE).
            missing_asset_status_id: The status id to use when finalization_strategy is CHANGE_STATUS.

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/json-job"
        data = {
            "synchronizationId": synchronization_id
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if file_id is not None:
            data["fileId"] = file_id
        if file_name is not None:
            data["fileName"] = file_name
        if delete_file is not None:
            data["deleteFile"] = delete_file
        if continue_on_error is not None:
            data["continueOnError"] = continue_on_error
        if relations_action is not None:
            data["relationsAction"] = relations_action
        if attributes_action is not None:
            data["attributesAction"] = attributes_action

        # Handle file upload
        files = None
        if file is not None:
            files = {"file": file}

        return self._post_multipart(url, data=data, files=files)

    def synchronize_finalize_in_job(
        self,
        synchronization_id: str,
        send_notification: Optional[bool] = None,
        batch_size: Optional[int] = None,
        simulation: Optional[bool] = None,
        save_result: Optional[bool] = None,
        finalization_strategy: Optional[str] = None,
        missing_asset_status_id: Optional[str] = None,
        finalization_parameters: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Starts synchronization finalization in job (asynchronously).

        Note that while the only mandatory parameter is synchronization_id, requests that
        omit all optional parameters fail because the multipart-based setup requires at least
        one part to be present in the body. To meet this requirement and still use all the
        default values, you can pass a dummy part, for example -F 'foo=bar' if using curl.

        Args:
            synchronization_id: The synchronization id used to distinguish different synchronizations. (required)
            send_notification: Whether job status notification should be sent. Defaults to false.
            batch_size: Deprecated. The batch size for processing. Defaults to 1000.
            simulation: Whether the import should be triggered as a simulation. Defaults to false.
            save_result: Deprecated. Whether the import result should be persisted or forgotten. Defaults to false.
            finalization_strategy: The synchronization finalization strategy (REMOVE_RESOURCES, CHANGE_STATUS, or IGNORE). Defaults to REMOVE_RESOURCES.
            missing_asset_status_id: The status ID for assets that no longer exist when finalizationStrategy is CHANGE_STATUS.
            finalization_parameters: Deprecated. Additional finalization parameters (contains STATUS_ID).

        Returns:
            requests.Response: The response from the API containing the job details.
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/finalize/job"
        data = {
            "synchronizationId": synchronization_id
        }

        # Add optional parameters
        if send_notification is not None:
            data["sendNotification"] = send_notification
        if batch_size is not None:
            data["batchSize"] = batch_size
        if simulation is not None:
            data["simulation"] = simulation
        if save_result is not None:
            data["saveResult"] = save_result
        if finalization_strategy is not None:
            data["finalizationStrategy"] = finalization_strategy
        if missing_asset_status_id is not None:
            data["missingAssetStatusId"] = missing_asset_status_id
        if finalization_parameters is not None:
            data["finalizationParameters"] = finalization_parameters

        return self._post_multipart(url, data=data)

    # ========== SYNCHRONIZATION CACHE ==========

    def evict_synchronization_cache(self, synchronization_id: str) -> requests.Response:
        """
        Removes all cache entries corresponding to the provided synchronization id.

        The synchronization component is optimized to only execute commands that differ
        from cycle to cycle. Call this method to clear the command cache and force the
        execution of all commands in this cycle.

        Note: this operation does not stop the tracking of the resources identified by
        the provided synchronization id. The next synchronization process using the same
        id will still be able to detect resources that should be removed.

        Args:
            synchronization_id: The synchronization id of the operation.

        Returns:
            requests.Response: The response from the API (204 No Content on success).
        """
        url = f"{self.__base_api}/synchronize/{synchronization_id}/evict"
        return requests.delete(
            url,
            auth=self.__connector.auth,
            timeout=self.__connector.timeout
        )
