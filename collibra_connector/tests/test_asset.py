"""Tests for the Asset API class."""
import pytest
from unittest.mock import Mock, patch, MagicMock

from collibra_connector import CollibraConnector
from collibra_connector.api.Asset import Asset


@pytest.fixture
def connector():
    """Create a mock connector for testing."""
    return CollibraConnector(
        api="https://test.collibra.com",
        username="testuser",
        password="testpass"
    )


@pytest.fixture
def asset_api(connector):
    """Create an Asset API instance."""
    return connector.asset


class TestAssetAddAsset:
    """Tests for add_asset method - including the bug fix verification."""

    def test_add_asset_with_id_parameter(self, asset_api):
        """Test that _id parameter is correctly passed as 'id' in the request body.

        This test verifies the bug fix where `id` (Python built-in) was incorrectly
        used instead of `_id` (the parameter) in the request body.
        """
        with patch.object(asset_api, '_post') as mock_post:
            with patch.object(asset_api, '_handle_response') as mock_handle:
                mock_post.return_value = Mock()
                mock_handle.return_value = {"id": "test-asset-id"}

                asset_api.add_asset(
                    name="Test Asset",
                    domain_id="12345678-1234-1234-1234-123456789012",
                    _id="87654321-4321-4321-4321-210987654321"
                )

                # Verify the data passed to _post contains the correct _id value
                call_args = mock_post.call_args
                data = call_args.kwargs.get('data') or call_args[1].get('data')

                # The key assertion: 'id' in data should be the _id parameter value,
                # NOT the built-in id() function result
                assert data["id"] == "87654321-4321-4321-4321-210987654321"

    def test_add_asset_without_id_parameter(self, asset_api):
        """Test add_asset when _id is not provided."""
        with patch.object(asset_api, '_post') as mock_post:
            with patch.object(asset_api, '_handle_response') as mock_handle:
                mock_post.return_value = Mock()
                mock_handle.return_value = {"id": "generated-id"}

                asset_api.add_asset(
                    name="Test Asset",
                    domain_id="12345678-1234-1234-1234-123456789012"
                )

                call_args = mock_post.call_args
                data = call_args.kwargs.get('data') or call_args[1].get('data')

                # When _id is not provided, it should be None
                assert data["id"] is None

    def test_add_asset_requires_name(self, asset_api):
        """Test that name is required."""
        with pytest.raises(ValueError, match="Name and domain_id are required"):
            asset_api.add_asset(
                name="",
                domain_id="12345678-1234-1234-1234-123456789012"
            )

    def test_add_asset_requires_domain_id(self, asset_api):
        """Test that domain_id is required."""
        with pytest.raises(ValueError, match="Name and domain_id are required"):
            asset_api.add_asset(
                name="Test Asset",
                domain_id=""
            )

    def test_add_asset_validates_domain_id_uuid(self, asset_api):
        """Test that domain_id must be a valid UUID."""
        with pytest.raises(ValueError, match="domain_id must be a valid UUID"):
            asset_api.add_asset(
                name="Test Asset",
                domain_id="not-a-uuid"
            )

    def test_add_asset_validates_id_uuid(self, asset_api):
        """Test that _id must be a valid UUID if provided."""
        with pytest.raises(ValueError, match="id must be a valid UUID"):
            asset_api.add_asset(
                name="Test Asset",
                domain_id="12345678-1234-1234-1234-123456789012",
                _id="not-a-uuid"
            )

    def test_add_asset_validates_type_id_uuid(self, asset_api):
        """Test that type_id must be a valid UUID if provided."""
        with pytest.raises(ValueError, match="type_id must be a valid UUID"):
            asset_api.add_asset(
                name="Test Asset",
                domain_id="12345678-1234-1234-1234-123456789012",
                type_id="not-a-uuid"
            )

    def test_add_asset_validates_excluded_from_auto_hyperlink_type(self, asset_api):
        """Test that excluded_from_auto_hyperlink must be boolean."""
        with pytest.raises(ValueError, match="excluded_from_auto_hyperlink must be a boolean"):
            asset_api.add_asset(
                name="Test Asset",
                domain_id="12345678-1234-1234-1234-123456789012",
                excluded_from_auto_hyperlink="yes"
            )


class TestAssetGetAsset:
    """Tests for get_asset method."""

    def test_get_asset_success(self, asset_api):
        """Test successful asset retrieval."""
        with patch.object(asset_api, '_get') as mock_get:
            with patch.object(asset_api, '_handle_response') as mock_handle:
                mock_get.return_value = Mock()
                mock_handle.return_value = {
                    "id": "12345678-1234-1234-1234-123456789012",
                    "name": "Test Asset"
                }

                result = asset_api.get_asset("12345678-1234-1234-1234-123456789012")

                assert result["name"] == "Test Asset"


class TestAssetRemoveAsset:
    """Tests for remove_asset method."""

    def test_remove_asset_requires_asset_id(self, asset_api):
        """Test that asset_id is required."""
        with pytest.raises(ValueError, match="asset_id is required"):
            asset_api.remove_asset("")

    def test_remove_asset_validates_uuid(self, asset_api):
        """Test that asset_id must be a valid UUID."""
        with pytest.raises(ValueError, match="asset_id must be a valid UUID"):
            asset_api.remove_asset("not-a-uuid")


class TestAssetFindAssets:
    """Tests for find_assets method."""

    def test_find_assets_default_params(self, asset_api):
        """Test find_assets with default parameters."""
        with patch.object(asset_api, '_get') as mock_get:
            with patch.object(asset_api, '_handle_response') as mock_handle:
                mock_get.return_value = Mock()
                mock_handle.return_value = {"results": [], "total": 0}

                result = asset_api.find_assets()

                # Verify default limit is passed
                call_args = mock_get.call_args
                params = call_args.kwargs.get('params') or call_args[1].get('params')
                assert params["limit"] == 1000

    def test_find_assets_with_filters(self, asset_api):
        """Test find_assets with filters."""
        with patch.object(asset_api, '_get') as mock_get:
            with patch.object(asset_api, '_handle_response') as mock_handle:
                mock_get.return_value = Mock()
                mock_handle.return_value = {"results": [], "total": 0}

                asset_api.find_assets(
                    community_id="12345678-1234-1234-1234-123456789012",
                    domain_id="87654321-4321-4321-4321-210987654321",
                    limit=500
                )

                call_args = mock_get.call_args
                params = call_args.kwargs.get('params') or call_args[1].get('params')
                assert params["communityId"] == "12345678-1234-1234-1234-123456789012"
                assert params["domainId"] == "87654321-4321-4321-4321-210987654321"
                assert params["limit"] == 500

    def test_find_assets_validates_community_id_uuid(self, asset_api):
        """Test that community_id must be a valid UUID."""
        with pytest.raises(ValueError, match="community_id must be a valid UUID"):
            asset_api.find_assets(community_id="not-a-uuid")
