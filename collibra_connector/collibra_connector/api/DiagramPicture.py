import uuid
from .Base import BaseAPI


class DiagramPicture(BaseAPI):
    """API class for diagram picture operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/diagramPictures"

    def add_diagram_picture(self, asset_id: str, diagram_view_id: str,
                             name: str = None, description: str = None):
        """
        Adds a diagram picture for a given asset and diagram view.
        A diagram picture is a copy of a traceability diagram at a given time.
        :param asset_id: The UUID of the asset.
        :param diagram_view_id: The UUID of the diagram view.
        :param name: Optional name for the diagram picture.
        :param description: Optional description.
        :return: Created diagram picture details.
        """
        if not asset_id:
            raise ValueError("asset_id is required")
        try:
            uuid.UUID(asset_id)
        except ValueError as exc:
            raise ValueError("asset_id must be a valid UUID") from exc

        if not diagram_view_id:
            raise ValueError("diagram_view_id is required")
        try:
            uuid.UUID(diagram_view_id)
        except ValueError as exc:
            raise ValueError("diagram_view_id must be a valid UUID") from exc

        data = {"assetId": asset_id, "diagramViewId": diagram_view_id}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description

        response = self._post(url=self.__base_api, data=data)
        return self._handle_response(response)
