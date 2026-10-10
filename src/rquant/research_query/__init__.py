"""Public read-only SQL research, separate from every private database."""

from .contracts import QueryColumn, QueryRequest, QueryResult, result_csv
from .executor import QueryExecutor, QueryLimits
from .snapshot import PUBLIC_SCHEMA, VerifiedQuerySnapshot, build_query_snapshot

__all__ = [
    "PUBLIC_SCHEMA",
    "QueryColumn",
    "QueryRequest",
    "QueryResult",
    "VerifiedQuerySnapshot",
    "build_query_snapshot",
    "result_csv",
    "QueryExecutor",
    "QueryLimits",
    "QueryPrivateServer",
    "QueryPrivateClient",
    "QueryAdmissionRejectedError",
    "QueryAdmissionUnavailableError",
]


def __getattr__(name: str) -> object:
    if name in {
        "QueryPrivateServer",
        "QueryPrivateClient",
        "QueryAdmissionRejectedError",
        "QueryAdmissionUnavailableError",
    }:
        from . import service

        return getattr(service, name)
    raise AttributeError(name)
