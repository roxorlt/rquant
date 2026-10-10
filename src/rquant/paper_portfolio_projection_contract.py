"""Fixed layouts for complete paper portfolio facts in the original S4 owner."""

from types import MappingProxyType

MAX_PAPER_PUBLICATION_BYTES = 4*1024*1024
PAPER_PORTFOLIO_PROJECTION_LAYOUTS = MappingProxyType({
    "paper_portfolio_state": ((("snapshot_hash", "string"), ("as_of_time", "timestamp"), ("account_count", "int"),
                               ("material_bytes", "int"), ("material_chunks", "int")), ("snapshot_hash",), 1, 4096, ("as_of_time",)),
    "paper_portfolio_account": ((("account_id", "string"), ("owner_id", "string"), ("configuration_fingerprint", "string"),
                                 ("configuration_version", "int"), ("ledger_revision", "int"), ("applied_sequence", "int")),
                                ("account_id",), 64, 64*1024, ()),
    "paper_portfolio_material": ((("snapshot_hash", "string"), ("chunk_index", "int"), ("payload", "string")),
                                 ("chunk_index",), 256, 6*1024*1024, ()),
})
PAPER_PORTFOLIO_PROJECTION_TABLES = frozenset(PAPER_PORTFOLIO_PROJECTION_LAYOUTS)
