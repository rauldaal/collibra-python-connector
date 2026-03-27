from .Base import BaseAPI


class SAML(BaseAPI):
    """API class for SAML operations."""

    def __init__(self, connector):
        super().__init__(connector)
        self.__base_api = connector.api + "/security/saml"

    def get_sp_metadata(self):
        """
        Returns the SAML Service Provider metadata for this Collibra instance.
        :return: SAML SP metadata as string.
        """
        response = self._get(url=self.__base_api)
        return self._handle_response(response)

    def change_certificate(self, cert_type: str, certificate: str):
        """
        Changes the certificate to be used with SAML.
        :param cert_type: The type of the SAML certificate (e.g., 'sp' or 'idp').
        :param certificate: The certificate content (PEM format).
        :return: Updated certificate details.
        """
        if not cert_type:
            raise ValueError("cert_type is required")
        if not certificate:
            raise ValueError("certificate is required")

        data = {"certificate": certificate}
        response = self._post(url=f"{self.__base_api}/certificate/{cert_type}", data=data)
        return self._handle_response(response)

    def delete_certificate(self, cert_type: str):
        """
        Deletes the specified SAML certificate from the SAML keystore.
        :param cert_type: The type of the SAML certificate (e.g., 'sp' or 'idp').
        :return: None
        """
        if not cert_type:
            raise ValueError("cert_type is required")

        response = self._delete(url=f"{self.__base_api}/certificate/{cert_type}")
        return self._handle_response(response)
