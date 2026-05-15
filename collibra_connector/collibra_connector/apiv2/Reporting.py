from .Base import BaseAPI


class Reporting(BaseAPI):
    """API class for reporting operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/reporting"

    def get_insights_zip(self, format: str = None, snapshot_date: str = None):
        """
        Downloads a reporting insights ZIP file with optional parameters.
        :param format: The format of the insights ZIP file (optional).
        :param snapshot_date: The snapshot date for the insights (optional).
        :return: Insights ZIP file content.
        """
        params = {}
        if format:
            params["format"] = format
        if snapshot_date:
            params["snapshotDate"] = snapshot_date

        response = self._get(url=f"{self.__base_api}/insights/download", params=params)
        return self._handle_response(response)

    def get_pre_signed_insights_zip(self, snapshot_date: str = None, format: str = None):
        """
        Retrieves a pre-signed URL for downloading the insights ZIP file directly from cloud storage.
        :param snapshot_date: The snapshot date for the insights (optional).
        :param format: The format of the insights ZIP file (optional).
        :return: Pre-signed URL or direct content.
        """
        params = {}
        if snapshot_date:
            params["snapshotDate"] = snapshot_date
        if format:
            params["format"] = format

        response = self._get(url=f"{self.__base_api}/insights/directDownload", params=params)
        return self._handle_response(response)
