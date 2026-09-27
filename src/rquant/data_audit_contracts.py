"""Dependency-free limits and table names shared by audit producers and readers."""

MAX_AUDIT_DAYS = 3660
MAX_REPORT_ISSUES = 10_000
MAX_INDEXED_ISSUES = 256

REPORT_PROJECTION_TABLES = frozenset(
    {
        "audit_report_overview",
        "audit_report_month",
        "audit_report_rule",
        "audit_report_issue",
    }
)
