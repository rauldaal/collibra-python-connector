from typing import Any, Dict
from .Base import BaseAPI


class DiagramPictures(BaseAPI):
    """API class for diagram picture operations."""

    def __init__(self, connector: Any):
        super().__init__(connector)
        self.__base_api = connector.api + "/diagramPictures"

    def add_diagram_picture(
        self,
        asset_id: str,
        view_id: str
    ) -> Dict[str, Any]:
        """
        Creates a new diagram picture for the specified asset and view.

        :param asset_id: The UUID of the asset associated with the diagram picture (required).
        :param view_id: The UUID of the view associated with the diagram picture (required).
        :return: A dictionary containing the details of the created diagram picture.
        """
        if not asset_id or not view_id:
            raise ValueError("asset_id and view_id are required")
        
        data = {
            "assetId": asset_id,
            "viewId": view_id
        }

        # UUID validation
        for param_name in ["assetId", "viewId"]:
            val = data.get(param_name)
            if val and not self._uuid_validation(val):
                raise ValueError(f"{param_name} must be a valid UUID")

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)
