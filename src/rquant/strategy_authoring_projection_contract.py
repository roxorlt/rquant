"""Data-only layouts shared by the template producer and existing S4 allowlist."""

from types import MappingProxyType

MAX_TEMPLATE_VERSIONS = 4096
MAX_TEMPLATE_COUNT = 500
MAX_TEMPLATE_PROJECTION_BYTES = 5 * 1024 * 1024
STRATEGY_TEMPLATE_PROJECTION_LAYOUTS = MappingProxyType(
    {
        "strategy_definition_state": (
            (
                ("status_key", "string"),
                ("identity_json", "string"),
                ("snapshot_sha256", "string"),
                ("available_at", "timestamp"),
            ),
            ("status_key",),
            1,
            4096,
            ("available_at",),
        ),
        "strategy_definition": (
            (
                ("strategy_id", "string"),
                ("version", "int"),
                ("owner_id", "string"),
                ("definition_json", "string"),
            ),
            ("strategy_id", "version"),
            MAX_TEMPLATE_VERSIONS,
            MAX_TEMPLATE_PROJECTION_BYTES,
            (),
        ),
        "strategy_template_source": (
            (("owner_id", "string"), ("sources_json", "string")),
            ("owner_id",),
            16,
            1024 * 1024,
            (),
        ),
    }
)
STRATEGY_TEMPLATE_PROJECTION_TABLES = frozenset(STRATEGY_TEMPLATE_PROJECTION_LAYOUTS)
