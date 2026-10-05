"""Read the complete three-table paper graph from one verified Serving lease."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from collections.abc import Callable

from rquant.paper_portfolio_projection import PaperPortfolioSnapshot, validate_paper_portfolio_projections
from rquant.paper_portfolio_projection_contract import PAPER_PORTFOLIO_PROJECTION_TABLES
from rquant.paper_portfolio_models import PaperPortfolioStateIdentity
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.serving_publisher import ServingReader
from rquant.web.serving import BorrowedGeneration
from rquant.runtime_contracts import normalize_aware_utc


def read_paper_portfolios(borrowed: BorrowedGeneration | None) -> PaperPortfolioSnapshot | None:
    if borrowed is None:
        return None
    names = tuple(sorted(PAPER_PORTFOLIO_PROJECTION_TABLES))
    marks = borrowed.cursor.execute("SELECT table_name,available,row_count,owner_dataset_id,owner_generation_id,available_at FROM projection_status WHERE table_name IN (?,?,?) ORDER BY table_name LIMIT 4", names).fetchall()
    if len(marks) != 3 or tuple(row[0] for row in marks) != names or any(type(row[1]) is not bool or type(row[2]) is not int for row in marks):
        raise ValueError("paper projection status is incomplete")
    if all(not row[1] for row in marks):
        if any(row[2] != 0 or row[3] != "paper_accounts" or row[4] is not None or row[5] is not None
               or borrowed.manifest.row_counts.get(row[0], 0) != 0 for row in marks):
            raise ValueError("unpublished paper projections contain facts")
        return None
    watermark = next((item for item in borrowed.manifest.watermarks if item.dataset_id == "paper_accounts"), None)
    at = marks[0][5]
    if watermark is None or any(not row[1] or row[3] != "paper_accounts" or row[4] != watermark.generation_id
                                or row[5] != at or at is None or at > borrowed.manifest.built_at
                                or row[2] != borrowed.manifest.row_counts.get(row[0]) for row in marks):
        raise ValueError("paper projection generation differs")
    tables = {}
    for name, _, count, *_ in marks:
        contract = PAGE_PROJECTION_CONTRACTS[name]
        if count > contract.max_rows:
            raise ValueError("paper projection exceeds its original row budget")
        rows = borrowed.cursor.execute(f"SELECT {', '.join(contract.column_names)} FROM {name} ORDER BY {', '.join(contract.sort_keys)} LIMIT ?", (contract.max_rows+1,)).fetchall()
        if len(rows) != count:
            raise ValueError("paper physical projection rows differ")
        tables[name] = ServingProjectionPayload(table_name=name, available_at=at,
                                                 rows=tuple({key: normalize_aware_utc(value).isoformat() if isinstance(value, datetime) else value
                                                             for key, value in zip(contract.column_names, row, strict=True)} for row in rows))
    result = validate_paper_portfolio_projections(tables)
    if result is None:
        raise ValueError("paper complete committed graph is absent")
    return result


class PaperPortfolioServingSource:
    def __init__(self, root: Path, *, clock: Callable[[], datetime], stale_after: timedelta = timedelta(minutes=10)) -> None:
        if stale_after.total_seconds() <= 0:
            raise ValueError("paper admission requires a finite positive source freshness")
        self.root, self.clock, self.stale_after = Path(root), clock, stale_after

    def require_fresh(self, *, generation_id: str, account_id: str, actor_id: str,
                      configuration_fingerprint: str, metadata_identity: PaperPortfolioStateIdentity) -> None:
        reader = ServingReader(self.root)
        with reader.acquire_generation() as lease:
            cursor = lease.connection.cursor()
            try:
                snapshot = read_paper_portfolios(BorrowedGeneration(manifest=lease.manifest, pointer=lease.pointer, cursor=cursor, fallback_detail=None))
            finally:
                cursor.close()
            if snapshot is None or lease.pointer is None or lease.manifest.generation_id != generation_id:
                raise ValueError("paper admission source generation changed")
            now = self.clock()
            if snapshot.available_at > now or lease.manifest.built_at > now or now-lease.manifest.built_at > self.stale_after:
                raise ValueError("paper admission source is stale or future")
            account = next((item for item in snapshot.for_owner(actor_id) if item.configuration.binding.account_id == account_id), None)
            if account is None:
                raise PermissionError("paper admission account belongs to another user")
            if account.configuration.fingerprint != configuration_fingerprint or account.metadata_identity != metadata_identity:
                raise ValueError("paper admission original configuration or metadata identity changed")
            pointer = reader.current_pointer()
            if (pointer.generation_id, pointer.manifest_sha256) != (lease.pointer.generation_id, lease.pointer.manifest_sha256):
                raise ValueError("paper admission source changed during verification")
