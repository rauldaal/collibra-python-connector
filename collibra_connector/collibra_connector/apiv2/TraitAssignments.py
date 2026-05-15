import uuid
from .Base import BaseAPI


class TraitAssignments(BaseAPI):
    """API class for trait assignment operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/traitAssignments"

    def get_trait_assignments_for_trait(self, trait_id: str):
        """
        Returns Trait Assignments for the Trait with the given ID.
        :param trait_id: The UUID of the trait.
        :return: List of trait assignments.
        """
        if not trait_id:
            raise ValueError("trait_id is required")
        try:
            uuid.UUID(trait_id)
        except ValueError as exc:
            raise ValueError("trait_id must be a valid UUID") from exc

        response = self._get(url=f"{self.__base_api}/trait/{trait_id}")
        return self._handle_response(response)

    def get_trait_assignments_for_trait_by_public_id(self, public_id: str):
        """
        Returns Trait Assignments for the Trait with the given public ID.
        :param public_id: The public ID of the trait.
        :return: List of trait assignments.
        """
        if not public_id:
            raise ValueError("public_id is required")

        response = self._get(url=f"{self.__base_api}/trait/publicId/{public_id}")
        return self._handle_response(response)
