# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.20] - 2025-01-01

### Fixed
- **Critical Bug Fix**: Fixed `add_asset()` method where `id` (Python built-in function) was incorrectly used instead of `_id` (the parameter) in the request body. This caused assets to be created without the specified ID.

### Added

#### Core Features
- **Retry Logic**: Added automatic retry mechanism with exponential backoff for transient errors (connection errors, timeouts, server errors 5xx, and rate limiting 429).
- **Context Manager Support**: `CollibraConnector` now supports the `with` statement for automatic session management and connection pooling.
- **Type Hints**: Added comprehensive type annotations throughout the codebase with `py.typed` marker for mypy support.
- **Input Validation**: Added validation for empty/whitespace API URL, username, and password in connector initialization.

#### Helper Utilities
- **Paginator**: Memory-efficient iteration over large paginated datasets.
  - `pages()`: Iterate page by page
  - `items()`: Iterate item by item
  - `collect()`: Collect all items into a list
  - Support for both offset and cursor-based pagination
  - `max_items` parameter to limit total items fetched

- **BatchProcessor**: Process bulk operations with rate limiting.
  - Configurable batch size and delay between batches
  - Error handling strategies: "continue", "stop", or "collect"
  - Progress callback support for monitoring

- **BatchResult**: Dataclass for batch operation results.
  - Track successes and errors separately
  - Calculate success rate percentage

- **CachedMetadata**: Thread-safe cache for frequently accessed metadata.
  - Cache asset types, statuses, attribute types, domain types, roles
  - Configurable TTL (time-to-live)
  - Methods: `get_asset_type_id()`, `get_status_id()`, `get_attribute_type_id()`, etc.
  - `clear()` and `refresh_all()` methods

- **DataTransformer**: Utilities for transforming API responses.
  - `flatten_asset()`: Flatten nested structures to dot-notation keys
  - `extract_ids()`: Extract specific field from list of items
  - `group_by()`: Group items by a field value
  - `to_name_id_map()`: Convert items to name->ID mapping

- **timed_cache**: Decorator for caching function results with TTL.
  - Automatic cache expiration
  - `clear_cache()` method on decorated functions

#### Testing
- **Unit Tests**: Added comprehensive test suite with 75 tests covering:
  - Connector initialization and configuration
  - Context manager functionality
  - Retry logic behavior
  - Asset API operations
  - Exception hierarchy
  - All helper utilities (Paginator, BatchProcessor, DataTransformer, etc.)

#### Documentation
- **CONTRIBUTING.md**: Added contribution guidelines with:
  - Development setup instructions
  - Code style guide
  - Testing guidelines
  - Pull request process

- **README.md**: Completely rewritten with:
  - Badges for PyPI, Python version, License
  - Complete API reference with examples
  - Helper utilities documentation
  - Error handling guide
  - Configuration options
  - Development instructions

#### Development
- **Development Dependencies**: Added optional dev dependencies in pyproject.toml:
  - pytest >= 7.0.0
  - pytest-cov >= 4.0.0
  - mypy >= 1.0.0
  - types-requests >= 2.28.0
  - ruff >= 0.1.0

### Changed
- **Version Sync**: Synchronized `__version__` in `__init__.py` with `pyproject.toml` version.
- **Improved `__repr__`**: Now shows base URL without exposing credentials.
- **Added `__str__`**: Human-friendly string representation.
- **URL Handling**: Improved URL handling to strip trailing slashes from base URL.
- **Exports**: Added `__all__` to `__init__.py` with all public exports including:
  - Exceptions (CollibraAPIError, UnauthorizedError, etc.)
  - Helpers (Paginator, BatchProcessor, CachedMetadata, DataTransformer, etc.)

- **Configuration**: Enhanced `pyproject.toml` with:
  - Python version classifiers (3.8-3.12)
  - `Typing :: Typed` classifier
  - mypy configuration
  - pytest configuration
  - ruff linter configuration
  - Package data configuration for py.typed

### New Properties
- `max_retries`: Get the maximum retry attempts
- `retry_delay`: Get the base retry delay
- `session`: Get the current session (when using context manager)

### New Methods
- `get_version()`: Retrieve library version programmatically
- `_make_request()`: Internal method with retry logic (can be used by subclasses)

## [1.0.19] - Previous Release

- Initial stable release on PyPI
- Basic API coverage for Assets, Communities, Domains, Users, etc.
