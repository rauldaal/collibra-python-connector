"""Tests for OutputModuleQueryBuilder, ResourceBuilder, and FilterBuilder."""
import json
import pytest
from unittest.mock import Mock, patch

from collibra_connector import OutputModuleQueryBuilder, ResourceBuilder, FilterBuilder
from collibra_connector import CollibraConnector


# ---------------------------------------------------------------------------
# FilterBuilder tests
# ---------------------------------------------------------------------------

class TestFilterBuilderLeafs:

    def test_equals(self):
        f = FilterBuilder.equals("StatusId", "abc-uuid")
        assert f.build() == {"Field": {"name": "StatusId", "operator": "EQUALS", "value": "abc-uuid"}}

    def test_not_equals(self):
        f = FilterBuilder.not_equals("Name", "Draft")
        assert f.build() == {"Field": {"name": "Name", "operator": "NOT_EQUALS", "value": "Draft"}}

    def test_starts_with(self):
        f = FilterBuilder.starts_with("FullName", "SF_")
        assert f.build()["Field"]["operator"] == "STARTS_WITH"

    def test_contains(self):
        f = FilterBuilder.contains("Description", "data")
        assert f.build()["Field"]["operator"] == "CONTAINS"

    def test_in_(self):
        f = FilterBuilder.in_("StatusId", ["uuid-1", "uuid-2"])
        result = f.build()
        assert result["Field"]["operator"] == "IN"
        assert result["Field"]["value"] == ["uuid-1", "uuid-2"]

    def test_is_null(self):
        f = FilterBuilder.is_null("RetiredDate")
        result = f.build()
        assert result["Field"]["operator"] == "IS_NULL"
        assert "value" not in result["Field"]

    def test_is_not_null(self):
        f = FilterBuilder.is_not_null("RetiredDate")
        assert f.build()["Field"]["operator"] == "IS_NOT_NULL"

    def test_greater_than(self):
        f = FilterBuilder.greater_than("Score", 50)
        assert f.build()["Field"]["operator"] == "GREATER_THAN"

    def test_less_than(self):
        f = FilterBuilder.less_than("Score", 100)
        assert f.build()["Field"]["operator"] == "LESS_THAN"

    def test_not_in(self):
        f = FilterBuilder.not_in("Tag", ["x", "y"])
        assert f.build()["Field"]["operator"] == "NOT_IN"

    def test_invalid_operator_raises(self):
        with pytest.raises(ValueError, match="Invalid operator"):
            FilterBuilder.field("Name", "LIKE", "foo")

    def test_missing_value_raises(self):
        with pytest.raises(ValueError, match="requires a value"):
            FilterBuilder.field("Name", "EQUALS")

    def test_build_without_condition_raises(self):
        f = FilterBuilder()
        with pytest.raises(ValueError, match="no condition"):
            f.build()


class TestFilterBuilderCompound:

    def test_and_(self):
        f = FilterBuilder.and_(
            FilterBuilder.equals("A", "1"),
            FilterBuilder.equals("B", "2"),
        )
        result = f.build()
        assert "AND" in result
        assert len(result["AND"]) == 2

    def test_or_(self):
        f = FilterBuilder.or_(
            FilterBuilder.equals("A", "1"),
            FilterBuilder.equals("B", "2"),
        )
        result = f.build()
        assert "OR" in result
        assert len(result["OR"]) == 2

    def test_nested_and_or(self):
        f = FilterBuilder.and_(
            FilterBuilder.equals("AssetTypeId", "type-uuid"),
            FilterBuilder.or_(
                FilterBuilder.equals("StatusId", "active"),
                FilterBuilder.is_null("RetiredDate"),
            ),
        )
        result = f.build()
        assert "AND" in result
        assert result["AND"][1]["OR"][0]["Field"]["name"] == "StatusId"

    def test_and_requires_at_least_one_condition(self):
        with pytest.raises(ValueError):
            FilterBuilder.and_()

    def test_or_requires_at_least_one_condition(self):
        with pytest.raises(ValueError):
            FilterBuilder.or_()


# ---------------------------------------------------------------------------
# ResourceBuilder tests
# ---------------------------------------------------------------------------

class TestResourceBuilderScalarFields:

    def test_name_is_set(self):
        rb = ResourceBuilder("MyAsset")
        block = rb.build_block()
        assert block["name"] == "MyAsset"

    def test_signifier(self):
        rb = ResourceBuilder("A").signifier("A_Name")
        assert rb.build_block()["Signifier"] == {"name": "A_Name"}

    def test_display_name(self):
        rb = ResourceBuilder("A").display_name("A_Display")
        assert rb.build_block()["DisplayName"] == {"name": "A_Display"}

    def test_id(self):
        rb = ResourceBuilder("A").id("A_Id")
        assert rb.build_block()["Id"] == {"name": "A_Id"}

    def test_generic_field(self):
        rb = ResourceBuilder("A").field("UserName", "Username")
        assert rb.build_block()["UserName"] == {"name": "Username"}

    def test_chaining_returns_self(self):
        rb = ResourceBuilder("A")
        result = rb.signifier("Name").id("Id").display_name("Display")
        assert result is rb


class TestResourceBuilderSubBlocks:

    def test_status(self):
        rb = ResourceBuilder("A").status("S", value_name="S_Value", id_name="S_Id")
        status = rb.build_block()["Status"]
        assert status["name"] == "S"
        assert status["Signifier"] == {"name": "S_Value"}
        assert status["Id"] == {"name": "S_Id"}

    def test_status_partial(self):
        rb = ResourceBuilder("A").status("S")
        status = rb.build_block()["Status"]
        assert "Signifier" not in status
        assert "Id" not in status

    def test_asset_type(self):
        rb = ResourceBuilder("A").asset_type("AT", signifier_name="AT_Label", id_name="AT_Id")
        at = rb.build_block()["AssetType"]
        assert at["name"] == "AT"
        assert at["signifier"] == {"name": "AT_Label"}
        assert at["Id"] == {"name": "AT_Id"}

    def test_domain(self):
        rb = ResourceBuilder("A").domain("D", domain_name="D_Name", id_name="D_Id")
        d = rb.build_block()["Domain"]
        assert d["Name"] == {"name": "D_Name"}
        assert d["Id"] == {"name": "D_Id"}

    def test_community(self):
        rb = ResourceBuilder("A").community("C", community_name="C_Name", id_name="C_Id")
        c = rb.build_block()["Community"]
        assert c["Name"] == {"name": "C_Name"}


class TestResourceBuilderAttributes:

    def test_attribute_default_type(self):
        rb = ResourceBuilder("A").attribute("label-uuid", "Attr1", "Attr1_Value")
        attrs = rb.build_block()["Attribute"]
        assert len(attrs) == 1
        assert attrs[0]["labelId"] == "label-uuid"
        assert attrs[0]["value"] == {"name": "Attr1_Value"}

    def test_date_attribute(self):
        rb = ResourceBuilder("A").date_attribute("label-uuid", "StartDate", "StartDate_Value")
        attrs = rb.build_block()["DateAttribute"]
        assert attrs[0]["date"] == {"name": "StartDate_Value"}

    def test_list_attribute_single(self):
        rb = ResourceBuilder("A").list_attribute("label-uuid", "Status", "Status_Value")
        assert "SingleValueListAttribute" in rb.build_block()
        entry = rb.build_block()["SingleValueListAttribute"][0]
        assert entry["Value"] == {"name": "Status_Value"}

    def test_list_attribute_multi(self):
        rb = ResourceBuilder("A").list_attribute("label-uuid", "Tags", "Tags_Value", multi=True)
        assert "MultiValueListAttribute" in rb.build_block()

    def test_numeric_attribute(self):
        rb = ResourceBuilder("A").numeric_attribute("label-uuid", "Score", "Score_Value")
        assert "NumericAttribute" in rb.build_block()

    def test_boolean_attribute(self):
        rb = ResourceBuilder("A").boolean_attribute("label-uuid", "Active", "Active_Value")
        assert "BooleanAttribute" in rb.build_block()

    def test_multiple_attributes_are_listed(self):
        rb = (
            ResourceBuilder("A")
            .attribute("uuid-1", "Attr1", "V1")
            .attribute("uuid-2", "Attr2", "V2")
        )
        attrs = rb.build_block()["Attribute"]
        assert len(attrs) == 2

    def test_invalid_attribute_type_raises(self):
        with pytest.raises(ValueError, match="Unknown attribute type"):
            ResourceBuilder("A").attribute("uuid", "Attr", "Value", attr_type="WeirdType")


class TestResourceBuilderResponsibility:

    def test_responsibility_basic(self):
        rb = ResourceBuilder("A").responsibility("role-uuid", "Owner", id_name="Owner_Id")
        resp = rb.build_block()["Responsibility"]
        assert len(resp) == 1
        assert resp[0]["roleId"] == "role-uuid"
        assert resp[0]["Id"] == {"name": "Owner_Id"}

    def test_responsibility_with_user(self):
        user_rb = (
            ResourceBuilder("User")
            .field("UserName", "Username")
            .field("FirstName", "FirstName")
            .field("LastName", "LastName")
            .field("EmailAddress", "Email")
            .id("UserId")
        )
        rb = ResourceBuilder("A").responsibility(
            "role-uuid", "Owner", id_name="Owner_Id", user=user_rb
        )
        resp = rb.build_block()["Responsibility"][0]
        assert resp["User"]["UserName"] == {"name": "Username"}
        assert resp["User"]["EmailAddress"] == {"name": "Email"}

    def test_multiple_responsibilities(self):
        rb = (
            ResourceBuilder("A")
            .responsibility("role-1", "Owner")
            .responsibility("role-2", "Steward")
        )
        assert len(rb.build_block()["Responsibility"]) == 2


class TestResourceBuilderRelation:

    def test_relation_direction_source(self):
        rb = ResourceBuilder("A").relation(
            type_id="rel-uuid",
            direction="SOURCE",
            name="A_to_B",
            related=ResourceBuilder("B").signifier("B_Name"),
        )
        rel = rb.build_block()["Relation"][0]
        assert rel["type"] == "SOURCE"
        assert "TargetAsset" in rel
        assert rel["TargetAsset"]["Signifier"] == {"name": "B_Name"}

    def test_relation_direction_target(self):
        rb = ResourceBuilder("A").relation(
            type_id="rel-uuid",
            direction="TARGET",
            name="B_to_A",
            related=ResourceBuilder("B").signifier("B_Name"),
        )
        rel = rb.build_block()["Relation"][0]
        assert "SourceAsset" in rel

    def test_relation_case_insensitive_direction(self):
        rb = ResourceBuilder("A").relation("rel-uuid", "source", "R")
        assert rb.build_block()["Relation"][0]["type"] == "SOURCE"

    def test_invalid_direction_raises(self):
        with pytest.raises(ValueError, match="direction must be"):
            ResourceBuilder("A").relation("rel-uuid", "BOTH", "R")

    def test_relation_without_related(self):
        rb = ResourceBuilder("A").relation("rel-uuid", "SOURCE", "RelAlias")
        rel = rb.build_block()["Relation"][0]
        assert "TargetAsset" not in rel

    def test_deeply_nested_relation(self):
        """Relations can be nested arbitrarily deep."""
        inner = ResourceBuilder("C").signifier("C_Name")
        middle = ResourceBuilder("B").relation("rel-2", "SOURCE", "B_to_C", related=inner)
        outer = ResourceBuilder("A").relation("rel-1", "SOURCE", "A_to_B", related=middle)
        rel = outer.build_block()["Relation"][0]
        nested = rel["TargetAsset"]["Relation"][0]
        assert nested["TargetAsset"]["Signifier"] == {"name": "C_Name"}

    def test_multiple_relations(self):
        rb = (
            ResourceBuilder("A")
            .relation("rel-1", "SOURCE", "Rel1")
            .relation("rel-2", "TARGET", "Rel2")
        )
        assert len(rb.build_block()["Relation"]) == 2


class TestResourceBuilderFilter:

    def test_filter_attached(self):
        rb = ResourceBuilder("A").filter(FilterBuilder.equals("Id", "uuid"))
        assert "Filter" in rb.build_block()

    def test_filter_wrong_type_raises(self):
        with pytest.raises(TypeError):
            ResourceBuilder("A").filter({"Field": {"name": "X"}})  # type: ignore


# ---------------------------------------------------------------------------
# OutputModuleQueryBuilder tests
# ---------------------------------------------------------------------------

class TestOutputModuleQueryBuilder:

    def test_default_uses_view_config(self):
        qb = OutputModuleQueryBuilder()
        body = qb.asset(ResourceBuilder("A").signifier("Name")).build()
        assert "ViewConfig" in body

    def test_table_view_config_flag(self):
        qb = OutputModuleQueryBuilder(use_table_view_config=True)
        body = qb.asset(ResourceBuilder("A").signifier("Name")).build()
        assert "TableViewConfig" in body

    def test_resources_key_present(self):
        qb = OutputModuleQueryBuilder().asset(ResourceBuilder("A").signifier("Name"))
        assert "Resources" in qb.build()["ViewConfig"]

    def test_asset_shorthand(self):
        qb = OutputModuleQueryBuilder().asset(ResourceBuilder("A").signifier("Name"))
        assert "Asset" in qb.build()["ViewConfig"]["Resources"]

    def test_domain_shorthand(self):
        qb = OutputModuleQueryBuilder().domain(ResourceBuilder("D").field("Name", "D_Name"))
        assert "Domain" in qb.build()["ViewConfig"]["Resources"]

    def test_community_shorthand(self):
        qb = OutputModuleQueryBuilder().community(ResourceBuilder("C").field("Name", "C_Name"))
        assert "Community" in qb.build()["ViewConfig"]["Resources"]

    def test_term_shorthand(self):
        qb = OutputModuleQueryBuilder().term(ResourceBuilder("T").signifier("T_Name"))
        assert "Term" in qb.build()["ViewConfig"]["Resources"]

    def test_add_resource_generic(self):
        qb = OutputModuleQueryBuilder().add_resource("BusinessTerm", ResourceBuilder("BT").id("BT_Id"))
        assert "BusinessTerm" in qb.build()["ViewConfig"]["Resources"]

    def test_add_resource_wrong_type_raises(self):
        with pytest.raises(TypeError):
            OutputModuleQueryBuilder().add_resource("Asset", {"name": "bad"})  # type: ignore

    def test_validate_empty_raises(self):
        with pytest.raises(ValueError, match="at least one resource"):
            OutputModuleQueryBuilder().validate()

    def test_build_empty_raises(self):
        with pytest.raises(ValueError):
            OutputModuleQueryBuilder().build()

    def test_build_json_is_valid_json(self):
        body = (
            OutputModuleQueryBuilder()
            .asset(ResourceBuilder("A").signifier("Name"))
            .build_json()
        )
        parsed = json.loads(body)
        assert "ViewConfig" in parsed

    def test_build_json_indent(self):
        body = (
            OutputModuleQueryBuilder()
            .asset(ResourceBuilder("A").signifier("Name"))
            .build_json(indent=2)
        )
        assert "\n" in body

    def test_repr(self):
        qb = OutputModuleQueryBuilder().asset(ResourceBuilder("A").signifier("Name"))
        r = repr(qb)
        assert "ViewConfig" in r
        assert "Asset" in r


# ---------------------------------------------------------------------------
# Full integration-style query test (mirrors the sample from test.py)
# ---------------------------------------------------------------------------

class TestFullQuery:

    def test_complex_query_structure(self):
        """Build a query equivalent to the sample in test.py and verify structure."""
        query = (
            OutputModuleQueryBuilder()
            .asset(
                ResourceBuilder("Request")
                .signifier("Request_FullName")
                .display_name("Request_DisplayName")
                .id("Request_Id")
                .asset_type("AssetType", signifier_name="AssetTypeName", id_name="AssetTypeId")
                .domain("Domain", domain_name="DomainName", id_name="Domain_Id")
                .status("Status", value_name="Status_Value", id_name="Status_Id")
                .attribute("attr-access-type-uuid", "AccessType", "AccessType_Value")
                .attribute("attr-retry-uuid", "RetryCount", "RetryCount_Value")
                .responsibility(
                    "role-requestor-uuid",
                    "Requestor",
                    id_name="ResponsibilityId",
                    user=ResourceBuilder("User")
                        .field("UserName", "Username")
                        .field("FirstName", "FirstName")
                        .field("LastName", "LastName")
                        .field("EmailAddress", "Email")
                        .id("UserId"),
                )
                .relation(
                    type_id="rel-datausage-request-uuid",
                    direction="TARGET",
                    name="Request_to_DSU",
                    related=ResourceBuilder("DSU")
                        .id("DSU_Id")
                        .signifier("DSU_FullName")
                        .relation(
                            type_id="rel-datausage-dataset-uuid",
                            direction="SOURCE",
                            name="DSU_to_DataSet",
                            related=ResourceBuilder("DataSet")
                                .id("DataSet_Id")
                                .signifier("DataSet_FullName")
                                .status("DS_Status", value_name="DS_Status_Value", id_name="DS_Status_Id")
                                .list_attribute("sf-status-uuid", "SFStatus", "SFStatus_Value")
                        ),
                )
                .relation(
                    type_id="rel-request-org-uuid",
                    direction="SOURCE",
                    name="Request_to_Org",
                    related=ResourceBuilder("Org").id("Org_Id").signifier("Org_FullName"),
                )
                .filter(
                    FilterBuilder.and_(
                        FilterBuilder.equals("AssetTypeId", "request-type-uuid"),
                        FilterBuilder.equals("Domain_Id", "domain-uuid"),
                        FilterBuilder.equals("Status_Id", "pending-uuid"),
                        FilterBuilder.starts_with("Request_FullName", "SF_"),
                        FilterBuilder.or_(
                            FilterBuilder.equals("DS_Status_Id", "approved-uuid"),
                            FilterBuilder.is_null("SFStatus"),
                        ),
                    )
                )
            )
        )

        body = query.build()

        # Top-level structure
        assert "ViewConfig" in body
        resources = body["ViewConfig"]["Resources"]
        assert "Asset" in resources

        asset = resources["Asset"]
        assert asset["name"] == "Request"
        assert asset["Signifier"] == {"name": "Request_FullName"}
        assert asset["AssetType"]["Id"] == {"name": "AssetTypeId"}
        assert len(asset["Attribute"]) == 2
        assert len(asset["Responsibility"]) == 1
        assert asset["Responsibility"][0]["User"]["UserName"] == {"name": "Username"}
        assert len(asset["Relation"]) == 2

        # Nested relation
        dsu_rel = asset["Relation"][0]
        assert dsu_rel["type"] == "TARGET"
        assert "SourceAsset" in dsu_rel
        dsu = dsu_rel["SourceAsset"]
        inner_rel = dsu["Relation"][0]
        assert inner_rel["TargetAsset"]["Signifier"] == {"name": "DataSet_FullName"}

        # Filter
        f = asset["Filter"]
        assert "AND" in f
        or_node = f["AND"][4]
        assert "OR" in or_node


# ---------------------------------------------------------------------------
# OutputModule.py _resolve_body tests
# ---------------------------------------------------------------------------

class TestResolveBody:

    def setup_method(self):
        conn = CollibraConnector(
            api="https://test.collibra.com",
            username="u",
            password="p",
        )
        self.om = conn.output_module

    def test_resolve_dict(self):
        d = {"ViewConfig": {"Resources": {}}}
        assert self.om._resolve_body(d) is d

    def test_resolve_json_string(self):
        s = '{"ViewConfig": {"Resources": {}}}'
        result = self.om._resolve_body(s)
        assert result == {"ViewConfig": {"Resources": {}}}

    def test_resolve_invalid_json_string_raises(self):
        with pytest.raises(ValueError, match="not valid JSON"):
            self.om._resolve_body("{bad json")

    def test_resolve_builder(self):
        qb = OutputModuleQueryBuilder().asset(ResourceBuilder("A").signifier("Name"))
        result = self.om._resolve_body(qb)
        assert "ViewConfig" in result

    def test_resolve_wrong_type_raises(self):
        with pytest.raises(TypeError):
            self.om._resolve_body(12345)  # type: ignore


# ---------------------------------------------------------------------------
# OutputModule API method integration tests (mocked)
# ---------------------------------------------------------------------------

@pytest.fixture
def connector():
    return CollibraConnector(
        api="https://test.collibra.com",
        username="testuser",
        password="testpass",
    )


@pytest.fixture
def output_module(connector):
    return connector.output_module


@pytest.fixture
def simple_query():
    return (
        OutputModuleQueryBuilder()
        .asset(ResourceBuilder("Asset").signifier("Name").id("Id"))
    )


class TestOutputModuleExportJson:

    def test_export_json_with_dict(self, output_module):
        body = {"ViewConfig": {"Resources": {"Asset": {"name": "A"}}}}
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {"view": {"Asset0": []}}
            output_module.export_json(body=body)
            mock_post.assert_called_once()
            assert "export/json" in mock_post.call_args[1]["url"]

    def test_export_json_with_builder(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {}
            output_module.export_json(body=simple_query)
            called_data = mock_post.call_args[1]["data"]
            assert "ViewConfig" in called_data

    def test_export_json_with_json_string(self, output_module):
        body = '{"ViewConfig": {"Resources": {"Asset": {"name": "A"}}}}'
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {}
            output_module.export_json(body=body)
            called_data = mock_post.call_args[1]["data"]
            assert isinstance(called_data, dict)

    def test_export_json_query_convenience(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {}
            output_module.export_json_query(simple_query)
            mock_post.assert_called_once()

    def test_export_json_validation_enabled_param(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {}
            output_module.export_json(body=simple_query, validation_enabled=True)
            params = mock_post.call_args[1]["params"]
            assert params == {"validationEnabled": True}


class TestOutputModuleExportCsv:

    def test_export_csv_uses_correct_endpoint(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_response = Mock()
            mock_response.text = "Name;Id\nAsset1;uuid-1"
            mock_post.return_value = mock_response
            mock_handle.return_value = {}
            result = output_module.export_csv(body=simple_query)
            assert "export/csv" in mock_post.call_args[1]["url"]
            assert result == "Name;Id\nAsset1;uuid-1"

    def test_export_csv_query_convenience(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock(text="csv")
            mock_handle.return_value = {}
            output_module.export_csv_query(simple_query)
            mock_post.assert_called_once()

    def test_export_csv_separator_param(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock(text="")
            mock_handle.return_value = {}
            output_module.export_csv(body=simple_query, separator=",")
            params = mock_post.call_args[1]["params"]
            assert params["separator"] == ","


class TestOutputModuleExportJobs:

    def test_export_csv_in_job(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {"id": "job-uuid", "type": "EXPORT_CSV"}
            output_module.export_csv_in_job(body=simple_query)
            assert "export/csv-job" in mock_post.call_args[1]["url"]

    def test_export_excel_in_job(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {"id": "job-uuid"}
            output_module.export_excel_in_job(body=simple_query)
            assert "export/excel-job" in mock_post.call_args[1]["url"]

    def test_export_excel_query_convenience(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {}
            output_module.export_excel_query(simple_query, sheet_name="Data")
            params = mock_post.call_args[1]["params"]
            assert params["sheetName"] == "Data"

    def test_export_json_in_job(self, output_module, simple_query):
        with patch.object(output_module, "_post") as mock_post, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_post.return_value = Mock()
            mock_handle.return_value = {}
            output_module.export_json_in_job(body=simple_query, file_name="out.json")
            params = mock_post.call_args[1]["params"]
            assert params["fileName"] == "out.json"


class TestOutputModuleGetTableViewConfig:

    def test_get_table_view_config(self, output_module):
        with patch.object(output_module, "_get") as mock_get, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_get.return_value = Mock()
            mock_handle.return_value = {"TableViewConfig": {"Resources": {}}}
            output_module.get_table_view_config_by_view_id("view-uuid")
            called_url = mock_get.call_args[1]["url"]
            assert "tableViewConfigs/viewId/view-uuid" in called_url

    def test_get_table_view_config_with_location(self, output_module):
        with patch.object(output_module, "_get") as mock_get, \
             patch.object(output_module, "_handle_response") as mock_handle:
            mock_get.return_value = Mock()
            mock_handle.return_value = {}
            output_module.get_table_view_config_by_view_id(
                "view-uuid", view_location="BUSINESS_GLOSSARY_BUSINESS_ASSETS"
            )
            params = mock_get.call_args[1]["params"]
            assert params["viewLocation"] == "BUSINESS_GLOSSARY_BUSINESS_ASSETS"
