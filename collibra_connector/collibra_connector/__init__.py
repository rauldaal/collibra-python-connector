#
#
#                           ███
#                     ██    ███   ███
#                    ████         ███
#              ███   ████   ██    ███   ████
#              ███   ████   ███   ███   ████
#                           ███   ███
#                           ███   ███
#          ██████████████   ███   ███   ████████
#           ███████████      █    ███    ██████
#
#         ███  █████████████   ████████████   ███
#         ███  █████████████   ████████████   ███
#
#           ██████           █     █████████████
#          ███████   ████   ███   ██████████████
#                    ████   ███
#                    ████   ███
#              ███   ████   ███   ███   ████
#               ██   ████   ██    ███    ██
#                    ████         ███
#                     ██    ███   ███
#                            █
#
#
"""

Collibra Connector Library
~~~~~~~~~~~~~~~~~~~~~~~~~~

Uses the Collibra API to connect and interact with Collibra's data governance platform.
This library provides a simple interface to handle connection and URLs

"""

from .connector import CollibraConnector
from .api.Exceptions import (
    CollibraAPIError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    ServerError,
)
from .helpers import (
    Paginator,
    PaginatedResponse,
    BatchProcessor,
    BatchResult,
    CachedMetadata,
    DataTransformer,
    DataFrameExporter,
    timed_cache,
)

__version__ = "1.0.20"
__all__ = [
    # Main connector
    "CollibraConnector",
    # Exceptions
    "CollibraAPIError",
    "UnauthorizedError",
    "ForbiddenError",
    "NotFoundError",
    "ServerError",
    # Helpers
    "Paginator",
    "PaginatedResponse",
    "BatchProcessor",
    "BatchResult",
    "CachedMetadata",
    "DataTransformer",
    "DataFrameExporter",
    "timed_cache",
]
