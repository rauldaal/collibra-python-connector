from .Base import BaseAPI


class Reporting(BaseAPI):
    """API class for reporting operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/reporting"

    def get_insights_zip(self):
        """
        Downloads a reporting insights ZIP file.
        :return: Insights ZIP file content.
        """
        response = self._get(url=f"{self.__base_api}/insights/download")
        return self._handle_response(response)

    def get_insights_zip_direct(self):
        """
        Downloads a reporting insights ZIP file directly from cloud storage.
        :return: Pre-signed URL or direct content.
        """
        response = self._get(url=f"{self.__base_api}/insights/directDownload")
        return self._handle_response(response)
