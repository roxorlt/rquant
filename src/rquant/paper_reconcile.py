"""Recompute a frozen v5 read transaction with the original broker algorithms."""

from __future__ import annotations

import base64
import hashlib
import os
import sqlite3
from contextlib import closing
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant.paper_contracts import PaperAccountSnapshot
from rquant.paper_ledger_anchor import Ed25519PaperLedgerAnchorVerifier
from rquant.paper_portfolio_ledger import PaperPortfolioHistoryRecord, PaperPortfolioLedgerFrame, PaperPortfolioLedgerSource
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, Sha256
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

MAX_COPY_BYTES = 8*1024*1024


class PaperReconcileComparison(RuntimeContractModel):
    account: PaperAccountSnapshot
    history: tuple[PaperPortfolioHistoryRecord, ...] = Field(max_length=5000)

    @classmethod
    def from_frame(cls, frame: PaperPortfolioLedgerFrame) -> Self:
        if frame.account is None:
            raise ValueError("对账缺少完整账户估值")
        return cls(account=frame.account, history=frame.history)

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperFrozenLedgerCopy(RuntimeContractModel):
    sqlite_base64: str = Field(min_length=1, max_length=((MAX_COPY_BYTES+2)//3)*4)
    copy_sha256: Sha256
    source_db_sha256: Sha256
    source_wal_sha256: Sha256 | None = None
    source_anchor_sha256: Sha256 | None = None
    anchor_json: str | None = Field(default=None, max_length=128*1024)
    active_key_id: str | None = Field(default=None, min_length=1, max_length=128)
    active_public_key: str | None = Field(default=None, max_length=4096)
    ledger_id: str | None = Field(default=None, max_length=128)
    anchor_max_age_seconds: int | None = Field(default=None, strict=True, gt=0, le=31536000)
    anchor_future_skew_seconds: int | None = Field(default=None, strict=True, ge=0, le=3600)

    @model_validator(mode="after")
    def bounded_original_copy(self) -> Self:
        raw = base64.b64decode(self.sqlite_base64, validate=True)
        if not 1 <= len(raw) <= MAX_COPY_BYTES or not raw.startswith(b"SQLite format 3\x00") or hashlib.sha256(raw).hexdigest() != self.copy_sha256:
            raise ValueError("paper readonly copy differs from its exact original bytes")
        group = (self.anchor_json, self.active_key_id, self.active_public_key, self.ledger_id,
                 self.anchor_max_age_seconds, self.anchor_future_skew_seconds, self.source_anchor_sha256)
        if any(item is not None for item in group) and not all(item is not None for item in group):
            raise ValueError("paper readonly anchor proof is incomplete")
        if self.anchor_json is not None and hashlib.sha256(self.anchor_json.encode()).hexdigest() != self.source_anchor_sha256:
            raise ValueError("paper readonly anchor bytes changed")
        return self


class PaperReconcileInput(RuntimeContractModel):
    contract: Literal["paper-reconcile-input/v1"] = "paper-reconcile-input/v1"
    configuration: PaperPortfolioConfiguration
    as_of: AwareUtcDatetime
    initial_cash: Decimal
    prices: dict[str, Decimal] = Field(max_length=500)
    ledger_revision: int = Field(strict=True, ge=1)
    head_fingerprint: Sha256
    ledger_generation: Sha256
    expected: PaperReconcileComparison
    frozen: PaperFrozenLedgerCopy

    @model_validator(mode="before")
    @classmethod
    def original_cash_guard(cls, value: object) -> object:
        if isinstance(value, dict):
            PortfolioBacktestConfig.validate_numeric_admission({"initial_cash": value.get("initial_cash")})
        return value

    @model_validator(mode="after")
    def exact_account(self) -> Self:
        if self.expected.account.account_id != self.configuration.binding.account_id or self.expected.account.as_of_time != self.as_of:
            raise ValueError("paper reconcile comparison belongs to a different account or cutoff")
        if len(self.model_dump_json().encode()) > 16*1024*1024:
            raise ValueError("paper reconcile source exceeds its original research input budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperReconcileDifference(RuntimeContractModel):
    path: str = Field(max_length=512)
    expected: str = Field(max_length=2048)
    actual: str = Field(max_length=2048)


class PaperReconcileResult(RuntimeContractModel):
    contract: Literal["paper-reconcile-result/v1"] = "paper-reconcile-result/v1"
    input_hash: Sha256
    configuration_fingerprint: Sha256
    ledger_revision: int = Field(strict=True, ge=1)
    head_fingerprint: Sha256
    copy_sha256: Sha256
    expected_fingerprint: Sha256
    actual_fingerprint: Sha256
    status: Literal["consistent", "differences"]
    difference_count: int = Field(strict=True, ge=0)
    differences: tuple[PaperReconcileDifference, ...] = Field(max_length=1000)
    truncated: bool = Field(strict=True)
    account: PaperAccountSnapshot

    @model_validator(mode="after")
    def actual_result(self) -> Self:
        if ((self.status == "consistent") != (self.difference_count == 0)
                or len(self.differences) != min(self.difference_count, 1000)
                or self.truncated != (self.difference_count > 1000)
                or (self.difference_count == 0 and self.expected_fingerprint != self.actual_fingerprint)):
            raise ValueError("paper reconciliation result cannot claim unverified consistency")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


def freeze_paper_reconcile(source: PaperPortfolioLedgerSource, configuration: PaperPortfolioConfiguration, *,
                           as_of: datetime, prices: dict[str, Decimal], expected: PaperReconcileComparison | None = None) -> PaperReconcileInput:
    if type(source) is not PaperPortfolioLedgerSource:
        raise TypeError("paper reconcile requires the original concrete readonly source")
    frame = source.read(configuration=configuration, as_of=as_of, prices=prices)
    comparison = expected or PaperReconcileComparison.from_frame(frame)
    tracked = (source.path, source.path.with_name(source.path.name+"-wal"), source.anchor_path)
    def hashes() -> tuple[str | None, ...]:
        return tuple(hashlib.sha256(path.read_bytes()).hexdigest() if path is not None and path.exists() else None for path in tracked)
    before = hashes()
    with source.open() as broker, tempfile.TemporaryDirectory(prefix="rquant-paper-read-copy-") as directory:
        connection = broker._connect()
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        if int(connection.execute("PRAGMA page_count").fetchone()[0])*page_size > MAX_COPY_BYTES:
            raise ValueError("paper readonly copy exceeds its original snapshot byte budget")
        _, head = broker._attestation_head(connection)
        marker = connection.execute("SELECT head_marker_fingerprint FROM paper_ledger_head_marker WHERE revision=?", (head["revision"],)).fetchone()
        if head["revision"] != frame.ledger_revision or marker is None or marker[0] != frame.head_fingerprint:
            raise ValueError("paper source ledger changed before its frozen read")
        path = Path(directory)/"snapshot.sqlite"
        with closing(sqlite3.connect(path)) as destination:
            connection.backup(destination)
            # Only the new private copy uses DELETE mode so it needs no absent WAL on read.
            destination.execute("PRAGMA journal_mode = DELETE")
        path.chmod(0o600)
        raw = path.read_bytes()
    proof: dict[str, object] = {}
    if source.anchor_path is not None:
        verifier = source.anchor_verifier
        if type(verifier) is not Ed25519PaperLedgerAnchorVerifier:
            raise TypeError("paper readonly copy requires its original pinned anchor verifier")
        proof = dict(anchor_json=source.anchor_path.read_text(), active_key_id=verifier.active_key_id,
                     active_public_key=verifier._active_public_key.decode("ascii"), ledger_id=verifier.allowed_ledger_id,
                     anchor_max_age_seconds=int(verifier.max_age.total_seconds()), anchor_future_skew_seconds=int(verifier.future_skew.total_seconds()))
    after = hashes()
    if before != after:
        raise ValueError("paper source bytes changed while freezing the original readonly input")
    return PaperReconcileInput(configuration=configuration, as_of=as_of, initial_cash=source.initial_cash, prices=prices,
                               ledger_revision=frame.ledger_revision, head_fingerprint=frame.head_fingerprint, ledger_generation=frame.ledger_generation,
                               expected=comparison, frozen=PaperFrozenLedgerCopy(sqlite_base64=base64.b64encode(raw).decode("ascii"),
                               copy_sha256=hashlib.sha256(raw).hexdigest(), source_db_sha256=before[0], source_wal_sha256=before[1],
                               source_anchor_sha256=before[2], **proof))


def execute_paper_reconcile(value: PaperReconcileInput) -> PaperReconcileResult:
    from rquant.paper_broker import BrokerCostPolicy

    value = PaperReconcileInput.model_validate(value.model_dump(mode="python"))
    with tempfile.TemporaryDirectory(prefix="rquant-paper-reconcile-") as directory:
        root = Path(directory)
        root.chmod(0o700)
        path = root/"frozen.sqlite"
        path.write_bytes(base64.b64decode(value.frozen.sqlite_base64, validate=True))
        path.chmod(0o600)
        verifier, anchor_path = None, None
        proof = value.frozen
        if proof.anchor_json is not None:
            anchor_path = root/"anchor.json"
            anchor_path.write_text(proof.anchor_json)
            anchor_path.chmod(0o600)
            verifier = Ed25519PaperLedgerAnchorVerifier(active_key_id=proof.active_key_id, active_public_key=proof.active_public_key.encode("ascii"),
                                                       allowed_ledger_id=proof.ledger_id, max_age=timedelta(seconds=proof.anchor_max_age_seconds),
                                                       future_skew=timedelta(seconds=proof.anchor_future_skew_seconds), clock=lambda: value.as_of)
        source = PaperPortfolioLedgerSource(path=path, account_id=value.configuration.binding.account_id, initial_cash=value.initial_cash,
                                            cost_policy=BrokerCostPolicy.from_execution_cost_spec(value.configuration.execution_cost_spec),
                                            ledger_id=proof.ledger_id, anchor_path=anchor_path, anchor_verifier=verifier)
        frame = source.read(configuration=value.configuration, as_of=value.as_of, prices=value.prices)
        if (frame.ledger_revision, frame.head_fingerprint, frame.ledger_generation) != (value.ledger_revision, value.head_fingerprint, value.ledger_generation):
            raise ValueError("paper immutable copy differs from the original ledger sequence or head")
        actual = PaperReconcileComparison.from_frame(frame)
    differences: list[PaperReconcileDifference] = []
    total = 0
    def compare(left: object, right: object, path: str) -> None:
        nonlocal total
        if isinstance(left, dict) and isinstance(right, dict):
            for key in sorted(left.keys() | right.keys()):
                compare(left.get(key), right.get(key), path+"."+key)
        elif isinstance(left, (tuple, list)) and isinstance(right, (tuple, list)):
            for index in range(max(len(left), len(right))):
                compare(left[index] if index < len(left) else None, right[index] if index < len(right) else None, path+f"[{index}]")
        elif left != right:
            total += 1
            if len(differences) < 1000:
                differences.append(PaperReconcileDifference(path=path, expected=str(left)[:2048], actual=str(right)[:2048]))
    compare(value.expected.model_dump(mode="json"), actual.model_dump(mode="json"), "account")
    return PaperReconcileResult(input_hash=value.fingerprint, configuration_fingerprint=value.configuration.fingerprint,
                                ledger_revision=frame.ledger_revision, head_fingerprint=frame.head_fingerprint, copy_sha256=proof.copy_sha256,
                                expected_fingerprint=value.expected.fingerprint, actual_fingerprint=actual.fingerprint,
                                status="differences" if total else "consistent", difference_count=total,
                                differences=tuple(differences), truncated=total > 1000, account=frame.account)
