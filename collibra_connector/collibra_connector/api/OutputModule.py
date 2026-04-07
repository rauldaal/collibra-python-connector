import json as _json
from typing import Any, Dict, Optional, Union

from .Base import BaseAPI


class OutputModule(BaseAPI):
    """
    Output Module API endpoints for Collibra DGC.

    All export methods accept the query body in any of three forms:
    - A plain ``dict`` (ViewConfig / TableViewConfig already constructed).
    - A JSON ``str`` (will be decoded automatically).
    - An :class:`~collibra_connector.query_builder.OutputModuleQueryBuilder`
      instance (``build()`` is called automatically).

    The convenience ``*_query()`` methods accept a builder directly and are
    the preferred high-level interface::

        query = OutputModuleQueryBuilder().asset(
            ResourceBuilder("MyAsset").signifier("Name").id("Id")
        )
        data  = conn.output_module.export_json_query(query)
        rows  = conn.output_module.export_csv_query(query)
    """

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/outputModule"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_body(body: Any) -> dict:
        """
        Normalise the query body to a plain dict expected by ``_post``.

        Accepts:
        - ``dict``                          → returned as-is
        - ``str``                           → JSON-decoded
        - ``OutputModuleQueryBuilder``       → ``.build()`` called
        """
        # Import lazily to avoid circular imports at module load time
        from collibra_connector.query_builder import OutputModuleQueryBuilder
        if isinstance(body, OutputModuleQueryBuilder):
            return body.build()
        if isinstance(body, str):
            try:
                return _json.loads(body)
            except _json.JSONDecodeError as exc:
                raise ValueError(f"body is not valid JSON: {exc}") from exc
        if isinstance(body, dict):
            return body
        raise TypeError(
            f"body must be a dict, JSON str, or OutputModuleQueryBuilder, "
            f"got {type(body).__name__!r}."
        )

    # ------------------------------------------------------------------
    # JSON export
    # ------------------------------------------------------------------

    def export_json(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
    ) -> Dict[str, Any]:
        """
        Export results in JSON format (synchronous, returns data immediately).

        Args:
            body:               ViewConfig / TableViewConfig as a ``dict``, JSON
                                ``str``, or :class:`OutputModuleQueryBuilder`.
            validation_enabled: When ``True`` the query syntax is validated
                                before execution. Strongly recommended.

        Returns:
            Parsed JSON response dict.
        """
        endpoint = f"{self.__base_api}/export/json"
        params = {"validationEnabled": validation_enabled} if validation_enabled else None
        response = self._post(
            url=endpoint,
            data=self._resolve_body(body),
            params=params,
        )
        return self._handle_response(response)

    def export_json_query(
        self,
        builder: Any,
        validation_enabled: bool = False,
    ) -> Dict[str, Any]:
        """
        Convenience wrapper — export JSON using an
        :class:`~collibra_connector.query_builder.OutputModuleQueryBuilder`.

        Args:
            builder:            A configured ``OutputModuleQueryBuilder``.
            validation_enabled: Validate syntax before execution.

        Returns:
            Parsed JSON response dict.
        """
        return self.export_json(body=builder, validation_enabled=validation_enabled)

    def export_json_in_job(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
        send_notification: bool = False,
    ) -> Dict[str, Any]:
        """
        Export results as JSON asynchronously (returns a Job object).

        Args:
            body:               ViewConfig / TableViewConfig body.
            validation_enabled: Validate syntax before the job starts.
            file_name:          Optional output file name.
            send_notification:  Send an e-mail on job completion.

        Returns:
            Job representation dict.
        """
        endpoint = f"{self.__base_api}/export/json-job"
        params: Dict[str, Any] = {"sendNotification": send_notification}
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params)
        return self._handle_response(response)

    def export_json_to_file(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Export results as JSON and store in a Collibra file (synchronous).

        Returns:
            File information dict (contains the file ``id``).
        """
        endpoint = f"{self.__base_api}/export/json-file"
        params: Dict[str, Any] = {}
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params or None)
        return self._handle_response(response)

    # ------------------------------------------------------------------
    # CSV export
    # ------------------------------------------------------------------

    def export_csv(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        separator: str = ";",
        quote: str = '"',
        escape: str = "\\",
        header_row: bool = True,
    ) -> str:
        """
        Export results in CSV format (synchronous, returns CSV text).

        Args:
            body:               TableViewConfig body.
            validation_enabled: Validate syntax before execution.
            separator:          Column delimiter (default ``";"``).
            quote:              Quote character (default ``'"'``).
            escape:             Escape character (default ``"\\"``).
            header_row:         Include header row (default ``True``).

        Returns:
            CSV content as a string.
        """
        endpoint = f"{self.__base_api}/export/csv"
        params: Dict[str, Any] = {
            "separator": separator,
            "quote": quote,
            "escape": escape,
            "headerRow": header_row,
        }
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params)
        self._handle_response(response)
        return response.text

    def export_csv_query(
        self,
        builder: Any,
        validation_enabled: bool = False,
        separator: str = ";",
        quote: str = '"',
        escape: str = "\\",
        header_row: bool = True,
    ) -> str:
        """
        Convenience wrapper — export CSV using an
        :class:`~collibra_connector.query_builder.OutputModuleQueryBuilder`.

        Returns:
            CSV content as a string.
        """
        return self.export_csv(
            body=builder,
            validation_enabled=validation_enabled,
            separator=separator,
            quote=quote,
            escape=escape,
            header_row=header_row,
        )

    def export_csv_in_job(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
        separator: str = ";",
        quote: str = '"',
        escape: str = "\\",
        header_row: bool = True,
        send_notification: bool = False,
    ) -> Dict[str, Any]:
        """
        Export results as CSV asynchronously (returns a Job object).

        Returns:
            Job representation dict.
        """
        endpoint = f"{self.__base_api}/export/csv-job"
        params: Dict[str, Any] = {
            "separator": separator,
            "quote": quote,
            "escape": escape,
            "headerRow": header_row,
            "sendNotification": send_notification,
        }
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params)
        return self._handle_response(response)

    def export_csv_to_file(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
        separator: str = ";",
        quote: str = '"',
        escape: str = "\\",
        header_row: bool = True,
    ) -> Dict[str, Any]:
        """
        Export results as CSV and store in a Collibra file (synchronous).

        Returns:
            File information dict (contains the file ``id``).
        """
        endpoint = f"{self.__base_api}/export/csv-file"
        params: Dict[str, Any] = {
            "separator": separator,
            "quote": quote,
            "escape": escape,
            "headerRow": header_row,
        }
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params)
        return self._handle_response(response)

    # ------------------------------------------------------------------
    # Excel export
    # ------------------------------------------------------------------

    def export_excel_in_job(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
        sheet_name: Optional[str] = None,
        header_row: bool = True,
        send_notification: bool = False,
    ) -> Dict[str, Any]:
        """
        Export results as Excel (.xlsx) asynchronously (returns a Job object).

        Returns:
            Job representation dict.
        """
        endpoint = f"{self.__base_api}/export/excel-job"
        params: Dict[str, Any] = {
            "headerRow": header_row,
            "sendNotification": send_notification,
        }
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        if sheet_name:
            params["sheetName"] = sheet_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params)
        return self._handle_response(response)

    def export_excel_to_file(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
        sheet_name: Optional[str] = None,
        header_row: bool = True,
    ) -> Dict[str, Any]:
        """
        Export results as Excel (.xlsx) and store in a Collibra file (synchronous).

        Returns:
            File information dict (contains the file ``id``).
        """
        endpoint = f"{self.__base_api}/export/excel-file"
        params: Dict[str, Any] = {"headerRow": header_row}
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        if sheet_name:
            params["sheetName"] = sheet_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params)
        return self._handle_response(response)

    def export_excel_query(
        self,
        builder: Any,
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
        sheet_name: Optional[str] = None,
        header_row: bool = True,
        send_notification: bool = False,
    ) -> Dict[str, Any]:
        """
        Convenience wrapper — export Excel using an
        :class:`~collibra_connector.query_builder.OutputModuleQueryBuilder`.

        Submits an async job.

        Returns:
            Job representation dict.
        """
        return self.export_excel_in_job(
            body=builder,
            validation_enabled=validation_enabled,
            file_name=file_name,
            sheet_name=sheet_name,
            header_row=header_row,
            send_notification=send_notification,
        )

    # ------------------------------------------------------------------
    # XML export
    # ------------------------------------------------------------------

    def export_xml(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
    ) -> str:
        """
        Export results in XML format (synchronous, returns XML text).

        Returns:
            XML content as a string.
        """
        endpoint = f"{self.__base_api}/export/xml"
        params = {"validationEnabled": validation_enabled} if validation_enabled else None
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params)
        self._handle_response(response)
        return response.text

    def export_xml_in_job(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Export results as XML asynchronously (returns a Job object).

        Returns:
            Job representation dict.
        """
        endpoint = f"{self.__base_api}/export/xml-job"
        params: Dict[str, Any] = {}
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params or None)
        return self._handle_response(response)

    def export_xml_to_file(
        self,
        body: Union[dict, str, Any],
        validation_enabled: bool = False,
        file_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Export results as XML and store in a Collibra file (synchronous).

        Returns:
            File information dict (contains the file ``id``).
        """
        endpoint = f"{self.__base_api}/export/xml-file"
        params: Dict[str, Any] = {}
        if validation_enabled:
            params["validationEnabled"] = validation_enabled
        if file_name:
            params["fileName"] = file_name
        response = self._post(url=endpoint, data=self._resolve_body(body), params=params or None)
        return self._handle_response(response)

    # ------------------------------------------------------------------
    # Table view config helper
    # ------------------------------------------------------------------

    def get_table_view_config_by_view_id(
        self,
        view_id: str,
        view_location: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return a TableViewConfig derived from an existing view in the Collibra UI.

        The returned config can be used directly as the ``body`` argument for any
        ``export_*`` method.

        Args:
            view_id:       The UUID of the view (extracted from the DGC URL,
                           e.g. ``/glossary?view=<uuid>``).
            view_location: Optional view-location filter, e.g.
                           ``"BUSINESS_GLOSSARY_BUSINESS_ASSETS"``.

        Returns:
            TableViewConfig dict.
        """
        url = f"{self.__base_api}/tableViewConfigs/viewId/{view_id}"
        params = {}
        if view_location:
            params["viewLocation"] = view_location
        response = self._get(url=url, params=params or None)
        return self._handle_response(response)
