"""
Collibra Connector - Main connector class for Collibra API.

This module provides the CollibraConnector class which serves as the main
entry point for interacting with the Collibra Data Governance Center API.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

import requests
from requests.auth import HTTPBasicAuth

from .api import (
    Activity,
    Application,
    Asset,
    AssetType,
    Assignment,
    Attachment,
    Attribute,
    AttributeType,
    Auth,
    Comment,
    Community,
    ComplexRelation,
    ComplexRelationType,
    DataQualityRule,
    DiagramPicture,
    Domain,
    DomainType,
    File,
    Import,
    Issue,
    JdbcDriver,
    Job,
    License,
    Mapping,
    Metadata,
    NavigationStatistics,
    OutputModule,
    Rating,
    Relation,
    RelationType,
    Reporting,
    Responsibility,
    Role,
    SAML,
    Scope,
    Search,
    Status,
    Tag,
    Trait,
    TraitAssignment,
    User,
    UserGroup,
    Utils,
    Validation,
    ViewPermission,
    Workflow,
    WorkflowDefinition,
    WorkflowInstance,
    WorkflowTask,
)
from .apiv2 import (
    Activities as V2Activities,
    Applications as V2Applications,
    Assets as V2Assets,
    AssetTypes as V2AssetTypes,
    Assignments as V2Assignments,
    Attachments as V2Attachments,
    Attributes as V2Attributes,
    AttributeTypes as V2AttributeTypes,
    Auth as V2Auth,
    Comments as V2Comments,
    Communities as V2Communities,
    ComplexRelations as V2ComplexRelations,
    ComplexRelationTypes as V2ComplexRelationTypes,
    DataQualityRules as V2DataQualityRules,
    DiagramPictures as V2DiagramPictures,
    Domains as V2Domains,
    DomainTypes as V2DomainTypes,
    Files as V2Files,
    Imports as V2Imports,
    Issues as V2Issues,
    JdbcDrivers as V2JdbcDrivers,
    Jobs as V2Jobs,
    Licenses as V2Licenses,
    Mappings as V2Mappings,
    Metadata as V2Metadata,
    NavigationStatistics as V2NavigationStatistics,
    OutputModules as V2OutputModules,
    Ratings as V2Ratings,
    Relations as V2Relations,
    RelationTypes as V2RelationTypes,
    Reporting as V2Reporting,
    Responsibilities as V2Responsibilities,
    Roles as V2Roles,
    SAML as V2SAML,
    Scopes as V2Scopes,
    Search as V2Search,
    Statuses as V2Statuses,
    Tags as V2Tags,
    Traits as V2Traits,
    TraitAssignments as V2TraitAssignments,
    Users as V2Users,
    UserGroups as V2UserGroups,
    Utils as V2Utils,
    Validations as V2Validations,
    ViewPermissions as V2ViewPermissions,
    Workflows as V2Workflows,
    WorkflowDefinitions as V2WorkflowDefinitions,
    WorkflowInstances as V2WorkflowInstances,
    WorkflowTasks as V2WorkflowTasks,
)

if TYPE_CHECKING:
    from requests.auth import AuthBase


class CollibraConnector:
    """
    Main connector class for interacting with the Collibra API.

    This class provides a unified interface to connect to and interact with
    the Collibra Data Governance Center API. It handles authentication,
    connection management, and provides access to all API modules.

    Attributes:
        activities: Activities API operations
        assets: Assets API operations
        communities: Communities API operations
        domains: Domains API operations
        users: Users API operations
        responsibilities: Responsibilities API operations
        workflows: Workflows API operations
        metadata: Metadata API operations
        comments: Comments API operations
        relations: Relations API operations
        output_modules: Output Modules API operations
        utils: Utility operations

    Example:
        >>> connector = CollibraConnector(
        ...     api="https://your-collibra-instance.com",
        ...     username="your-username",
        ...     password="your-password"
        ... )
        >>> if connector.test_connection():
        ...     assets = connector.assets.find_assets()

        # Using as context manager:
        >>> with CollibraConnector(api="...", username="...", password="...") as conn:
        ...     assets = conn.assets.find_assets()
    """

    DEFAULT_TIMEOUT: int = 30
    DEFAULT_MAX_RETRIES: int = 3
    DEFAULT_RETRY_DELAY: float = 1.0
    RETRYABLE_STATUS_CODES: tuple = (429, 500, 502, 503, 504)

    def __init__(
        self,
        api: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        **kwargs: Any
    ) -> None:
        """
        Initialize the CollibraConnector with API URL and authentication credentials.

        Credentials can be provided as arguments or via environment variables:
        COLLIBRA_URL, COLLIBRA_USERNAME, COLLIBRA_PASSWORD.

        Args:
            api: The base API URL for Collibra (e.g., 'https://your-instance.collibra.com').
            username: The username for authentication.
            password: The password for authentication.
            timeout: Request timeout in seconds. Defaults to 30.
            max_retries: Maximum number of retry attempts for failed requests. Defaults to 3.
            retry_delay: Base delay between retries in seconds (uses exponential backoff). Defaults to 1.0.
            **kwargs: Additional keyword arguments.
                - uuids (bool): If True, fetches all UUIDs on initialization.

        Raises:
            ValueError: If api, username, or password is empty and not in env vars.
        """
        import os

        # Load from env vars if not provided (None means not provided, "" means empty)
        if api is None:
            api = os.environ.get("COLLIBRA_URL")
        if username is None:
            username = os.environ.get("COLLIBRA_USERNAME")
        if password is None:
            password = os.environ.get("COLLIBRA_PASSWORD")

        if not api or not api.strip():
            raise ValueError("API URL cannot be empty")
        if not username or not username.strip():
            raise ValueError("Username cannot be empty")
        if not password or not password.strip():
            raise ValueError("Password cannot be empty")

        self.__auth: AuthBase = HTTPBasicAuth(username, password)
        self.__api: str = api.rstrip("/") + "/rest/2.0"
        self.__base_url: str = api.rstrip("/")
        self.__timeout: int = timeout
        self.__max_retries: int = max_retries
        self.__retry_delay: float = retry_delay
        self.__session: Optional[requests.Session] = None

        # Initialize api (v1) classes — singular attribute names
        self.activity: Activity = Activity(self)
        self.application: Application = Application(self)
        self.asset: Asset = Asset(self)
        self.asset_type: AssetType = AssetType(self)
        self.assignment: Assignment = Assignment(self)
        self.attachment: Attachment = Attachment(self)
        self.attribute: Attribute = Attribute(self)
        self.attribute_type: AttributeType = AttributeType(self)
        self.auth_api: Auth = Auth(self)
        self.comment: Comment = Comment(self)
        self.community: Community = Community(self)
        self.complex_relation: ComplexRelation = ComplexRelation(self)
        self.complex_relation_type: ComplexRelationType = ComplexRelationType(self)
        self.data_quality_rule: DataQualityRule = DataQualityRule(self)
        self.diagram_picture: DiagramPicture = DiagramPicture(self)
        self.domain: Domain = Domain(self)
        self.domain_type: DomainType = DomainType(self)
        self.file: File = File(self)
        self.import_api: Import = Import(self)
        self.issue: Issue = Issue(self)
        self.jdbc_driver: JdbcDriver = JdbcDriver(self)
        self.job: Job = Job(self)
        self.license: License = License(self)
        self.mapping: Mapping = Mapping(self)
        self.metadata: Metadata = Metadata(self)
        self.navigation_statistics: NavigationStatistics = NavigationStatistics(self)
        self.output_module: OutputModule = OutputModule(self)
        self.rating: Rating = Rating(self)
        self.relation: Relation = Relation(self)
        self.relation_type: RelationType = RelationType(self)
        self.reporting: Reporting = Reporting(self)
        self.responsibility: Responsibility = Responsibility(self)
        self.role: Role = Role(self)
        self.saml: SAML = SAML(self)
        self.scope: Scope = Scope(self)
        self.search: Search = Search(self)
        self.status: Status = Status(self)
        self.tag: Tag = Tag(self)
        self.trait: Trait = Trait(self)
        self.trait_assignment: TraitAssignment = TraitAssignment(self)
        self.user: User = User(self)
        self.user_group: UserGroup = UserGroup(self)
        self.utils: Utils = Utils(self)
        self.validation: Validation = Validation(self)
        self.view_permission: ViewPermission = ViewPermission(self)
        self.workflow: Workflow = Workflow(self)
        self.workflow_definition: WorkflowDefinition = WorkflowDefinition(self)
        self.workflow_instance: WorkflowInstance = WorkflowInstance(self)
        self.workflow_task: WorkflowTask = WorkflowTask(self)

        # Initialize apiv2 classes — plural attribute names
        # Where the class name is identical in both packages, v2_ prefix is used
        self.activities: V2Activities = V2Activities(self)
        self.applications: V2Applications = V2Applications(self)
        self.assets: V2Assets = V2Assets(self)
        self.asset_types: V2AssetTypes = V2AssetTypes(self)
        self.assignments: V2Assignments = V2Assignments(self)
        self.attachments: V2Attachments = V2Attachments(self)
        self.attributes: V2Attributes = V2Attributes(self)
        self.attribute_types: V2AttributeTypes = V2AttributeTypes(self)
        self.v2_auth_api: V2Auth = V2Auth(self)
        self.comments: V2Comments = V2Comments(self)
        self.communities: V2Communities = V2Communities(self)
        self.complex_relations: V2ComplexRelations = V2ComplexRelations(self)
        self.complex_relation_types: V2ComplexRelationTypes = V2ComplexRelationTypes(self)
        self.data_quality_rules: V2DataQualityRules = V2DataQualityRules(self)
        self.diagram_pictures: V2DiagramPictures = V2DiagramPictures(self)
        self.domains: V2Domains = V2Domains(self)
        self.domain_types: V2DomainTypes = V2DomainTypes(self)
        self.files: V2Files = V2Files(self)
        self.imports: V2Imports = V2Imports(self)
        self.issues: V2Issues = V2Issues(self)
        self.jdbc_drivers: V2JdbcDrivers = V2JdbcDrivers(self)
        self.jobs: V2Jobs = V2Jobs(self)
        self.licenses: V2Licenses = V2Licenses(self)
        self.mappings: V2Mappings = V2Mappings(self)
        self.v2_metadata: V2Metadata = V2Metadata(self)
        self.v2_navigation_statistics: V2NavigationStatistics = V2NavigationStatistics(self)
        self.output_modules: V2OutputModules = V2OutputModules(self)
        self.ratings: V2Ratings = V2Ratings(self)
        self.relations: V2Relations = V2Relations(self)
        self.relation_types: V2RelationTypes = V2RelationTypes(self)
        self.v2_reporting: V2Reporting = V2Reporting(self)
        self.responsibilities: V2Responsibilities = V2Responsibilities(self)
        self.roles: V2Roles = V2Roles(self)
        self.v2_saml: V2SAML = V2SAML(self)
        self.scopes: V2Scopes = V2Scopes(self)
        self.v2_search: V2Search = V2Search(self)
        self.statuses: V2Statuses = V2Statuses(self)
        self.tags: V2Tags = V2Tags(self)
        self.traits: V2Traits = V2Traits(self)
        self.trait_assignments: V2TraitAssignments = V2TraitAssignments(self)
        self.users: V2Users = V2Users(self)
        self.user_groups: V2UserGroups = V2UserGroups(self)
        self.v2_utils: V2Utils = V2Utils(self)
        self.validations: V2Validations = V2Validations(self)
        self.view_permissions: V2ViewPermissions = V2ViewPermissions(self)
        self.workflows: V2Workflows = V2Workflows(self)
        self.workflow_definitions: V2WorkflowDefinitions = V2WorkflowDefinitions(self)
        self.workflow_instances: V2WorkflowInstances = V2WorkflowInstances(self)
        self.workflow_tasks: V2WorkflowTasks = V2WorkflowTasks(self)

        # Initialize Logger without basicConfig
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())

        self.uuids: Dict[str, Dict[str, str]] = {}
        if kwargs.get('uuids'):
            self.uuids = self.utils.get_uuids() or {}

    def __enter__(self) -> "CollibraConnector":
        """Enter context manager, creating a session for connection pooling."""
        self.__session = requests.Session()
        self.__session.auth = self.__auth
        self.__session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, closing the session."""
        if self.__session:
            self.__session.close()
            self.__session = None

    def __repr__(self) -> str:
        """Return string representation of the connector."""
        return f"CollibraConnector(api={self.__base_url})"

    def __str__(self) -> str:
        """Return user-friendly string representation."""
        return f"CollibraConnector connected to {self.__base_url}"

    @property
    def api(self) -> str:
        """Get the full API URL including the REST version path."""
        return self.__api

    @property
    def auth(self) -> "AuthBase":
        """Get the authentication object."""
        return self.__auth

    @property
    def base_url(self) -> str:
        """Get the base URL without the REST version path."""
        return self.__base_url

    @property
    def timeout(self) -> int:
        """Get the request timeout in seconds."""
        return self.__timeout

    @property
    def max_retries(self) -> int:
        """Get the maximum number of retry attempts."""
        return self.__max_retries

    @property
    def retry_delay(self) -> float:
        """Get the base retry delay in seconds."""
        return self.__retry_delay

    @property
    def session(self) -> Optional[requests.Session]:
        """Get the current session (if using context manager)."""
        return self.__session

    def test_connection(self) -> bool:
        """
        Test the connection to the Collibra API.

        Makes a request to verify that the credentials are valid and
        the API is reachable.

        Returns:
            True if connection is successful, False otherwise.

        Example:
            >>> connector = CollibraConnector(api="...", username="...", password="...")
            >>> if connector.test_connection():
            ...     print("Connected successfully!")
        """
        try:
            response = self._make_request(
                method="GET",
                url=f"{self.__api}/auth/sessions/current"
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs: Any
    ) -> requests.Response:
        """
        Make an HTTP request with automatic retry logic.

        This method handles retries with exponential backoff for transient errors.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE).
            url: The URL to make the request to.
            **kwargs: Additional arguments to pass to the request.

        Returns:
            The response object from the request.

        Raises:
            requests.RequestException: If all retry attempts fail.
        """
        kwargs.setdefault("timeout", self.__timeout)
        kwargs.setdefault("auth", self.__auth)

        request_func = self.__session.request if self.__session else requests.request
        last_exception: Optional[Exception] = None

        for attempt in range(self.__max_retries):
            try:
                response = request_func(method, url, **kwargs)

                # Don't retry on success or client errors (except rate limiting)
                if response.status_code < 500 and response.status_code != 429:
                    return response

                # Retry on server errors and rate limiting
                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    if attempt < self.__max_retries - 1:
                        delay = self.__retry_delay * (2 ** attempt)
                        self.logger.warning(
                            f"Request failed with status {response.status_code}, "
                            f"retrying in {delay:.1f}s (attempt {attempt + 1}/{self.__max_retries})"
                        )
                        time.sleep(delay)
                        continue

                return response

            except (requests.ConnectionError, requests.Timeout) as e:
                last_exception = e
                if attempt < self.__max_retries - 1:
                    delay = self.__retry_delay * (2 ** attempt)
                    self.logger.warning(
                        f"Request failed with {type(e).__name__}, "
                        f"retrying in {delay:.1f}s (attempt {attempt + 1}/{self.__max_retries})"
                    )
                    time.sleep(delay)
                else:
                    raise

        # This should not be reached, but just in case
        if last_exception:
            raise last_exception
        raise requests.RequestException("Request failed after all retries")

    def get_version(self) -> str:
        """
        Get the version of this connector library.

        Returns:
            The version string of the collibra-connector package.
        """
        from . import __version__
        return __version__
