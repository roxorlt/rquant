"""Read one original v5 transaction, including immutable order provenance."""

from __future__ import annotations

from datetime import datetime
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Mapping, Self

from pydantic import Field, model_validator

from rquant.paper_broker import (
    BrokerCostPolicy, PaperBrokerReconciliation, PaperBrokerReconciliationError,
    PaperBrokerStore, PaperHistoryFill, _close_event_fingerprint,
)
from rquant.paper_contracts import PaperAccountSnapshot, PaperOrder, PaperOrderIntent, PaperOrderStatus
from rquant.paper_ledger_anchor import Ed25519PaperLedgerAnchorVerifier
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, Sha256
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256, normalize_aware_utc

MAX_FULL_ORDERS = 5_000
MAX_FULL_FILLS = 25_000
MAX_LEDGER_EVENTS = 50_000
MAX_FRAME_BYTES = 5 * 1024 * 1024


class PaperLedgerPriceGap(RuntimeContractModel):
    ledger_revision: int = Field(strict=True, ge=1)
    head_fingerprint: Sha256
    missing_codes: tuple[str, ...]


class PaperMissingClosePrices(ValueError):
    def __init__(self, gap: PaperLedgerPriceGap) -> None:
        self.gap = gap
        super().__init__("缺少收盘估值：" + ", ".join(gap.missing_codes))


class PaperPortfolioHistoryRecord(RuntimeContractModel):
    sequence: int = Field(strict=True, ge=2)
    order: PaperOrder
    intent: PaperOrderIntent
    fills: tuple[PaperHistoryFill, ...]

    @model_validator(mode="after")
    def exact_order(self) -> Self:
        if (self.order.intent_id != self.intent.intent_id or self.order.account_id != self.intent.account_id
                or any(item.order_id != self.order.order_id for item in self.fills)):
            raise ValueError("paper complete history identity differs")
        return self


class PaperPortfolioLedgerFrame(RuntimeContractModel):
    configuration_fingerprint: Sha256
    as_of: AwareUtcDatetime
    ledger_revision: int = Field(strict=True, ge=1)
    ledger_generation: Sha256
    head_fingerprint: Sha256
    account_id: str
    account: PaperAccountSnapshot | None
    missing_valuation_codes: tuple[str, ...] = Field(default=(), max_length=500)
    reconciliation: PaperBrokerReconciliation
    history: tuple[PaperPortfolioHistoryRecord, ...] = Field(max_length=MAX_FULL_ORDERS)

    @model_validator(mode="after")
    def complete(self) -> Self:
        sequences = tuple(item.sequence for item in self.history)
        if (sequences != tuple(sorted(set(sequences))) or any(item.order.account_id != self.account_id for item in self.history)
                or any(sequence > self.ledger_revision for sequence in sequences)
                or self.reconciliation.account_id != self.account_id
                or self.reconciliation.order_count != len(self.history) or not self.reconciliation.is_consistent):
            raise ValueError("paper complete history is detached from the original transaction")
        if (self.account is None) != bool(self.missing_valuation_codes) or (self.account is not None and self.account.account_id != self.account_id):
            raise ValueError("paper missing valuation must remain separate from complete original history")
        if len(self.model_dump_json().encode()) > MAX_FRAME_BYTES:
            raise ValueError("paper complete history exceeds the bounded published material budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperPortfolioLedgerSource:
    def __init__(self, *, path: Path, account_id: str, initial_cash: Decimal, cost_policy: BrokerCostPolicy,
                 ledger_id: str | None = None, anchor_path: Path | None = None,
                 anchor_verifier: Ed25519PaperLedgerAnchorVerifier | None = None) -> None:
        self.path = Path(path).absolute()
        self.account_id = account_id
        self.initial_cash = _parse_decimal(initial_cash, field_name="paper initial cash")
        PortfolioBacktestConfig.validate_numeric_admission({"initial_cash": initial_cash})
        if not 0 < self.initial_cash <= Decimal("1000000000000"):
            raise ValueError("paper initial cash exceeds its original amount budget")
        self.cost_policy = BrokerCostPolicy.model_validate(cost_policy.model_dump(mode="python"))
        self.ledger_id = ledger_id
        self.anchor_path = anchor_path
        self.anchor_verifier = anchor_verifier
        value = self.path.lstat()
        self._identity = (value.st_dev, value.st_ino)

    @contextmanager
    def open(self) -> Iterator[PaperBrokerStore]:
        value = self.path.lstat()
        if (value.st_dev, value.st_ino) != self._identity:
            raise ValueError("original paper ledger source was replaced")
        with PaperBrokerStore.open_readonly(self.path, account_id=self.account_id, initial_cash=self.initial_cash,
                                            cost_policy=self.cost_policy, ledger_id=self.ledger_id, ledger_anchor_path=self.anchor_path,
                                            ledger_anchor_verifier=self.anchor_verifier) as broker:
            yield broker

    def read(self, *, configuration: PaperPortfolioConfiguration, as_of: datetime,
             prices: Mapping[str, Decimal], allow_missing_prices: bool = False) -> PaperPortfolioLedgerFrame:
        if type(allow_missing_prices) is not bool:
            raise TypeError("paper missing-price admission must be an explicit server choice")
        configuration = PaperPortfolioConfiguration.model_validate(configuration.model_dump(mode="python"))
        if (configuration.binding.account_id != self.account_id or configuration.execution_cost_spec != self.cost_policy.execution_cost_spec
                or (self.ledger_id is not None and self.ledger_id != configuration.binding.ledger_id)):
            raise ValueError("paper ledger source belongs to a different account or configuration")
        value = self.path.lstat()
        if (value.st_dev, value.st_ino) != self._identity:
            raise ValueError("original paper ledger source was replaced")
        cutoff = normalize_aware_utc(as_of)
        if len(prices) > 500:
            raise ValueError("paper valuation exceeds its original 500-code budget")
        prices = {code: _parse_decimal(price, field_name="paper valuation") for code, price in prices.items()}
        if any(not 0 < price <= Decimal("1000000000") for price in prices.values()):
            raise ValueError("paper valuation exceeds the original amount budget")
        with self.open() as broker:
            connection = broker._connect()
            for table, maximum in (("paper_order", MAX_FULL_ORDERS), ("paper_fill", MAX_FULL_FILLS), ("paper_ledger_attestation", MAX_LEDGER_EVENTS)):
                count = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                if count > maximum:
                    raise ValueError("paper complete ledger exceeds its fixed read budget")
            reconciliation = broker.reconcile()
            _, latest = broker._attestation_head(connection)
            head = connection.execute("SELECT * FROM paper_ledger_head_marker WHERE revision=?", (latest["revision"],)).fetchone()
            latest_order = connection.execute("SELECT MAX(updated_at) FROM paper_order WHERE account_id=?", (self.account_id,)).fetchone()[0]
            if latest_order is not None and broker._required_ledger_timestamp(latest_order, label="paper latest order") > cutoff:
                raise ValueError("paper cutoff cannot precede its latest ledger event")
            holding_codes = {row[0] for row in connection.execute("SELECT DISTINCT ts_code FROM paper_lot WHERE account_id=? AND remaining_quantity>0", (self.account_id,))}
            if len(holding_codes | set(prices)) > 500:
                raise ValueError("complete paper valuation/holding union exceeds the original budget")
            missing = holding_codes - set(prices)
            if missing and not allow_missing_prices:
                raise PaperMissingClosePrices(PaperLedgerPriceGap(ledger_revision=int(latest["revision"]), head_fingerprint=head["head_marker_fingerprint"], missing_codes=tuple(sorted(missing))))
            account = broker.account_snapshot(as_of=cutoff, market_prices=prices) if not missing else None
            # Reuse the original row verifiers. Sequence comes from the immutable
            # intent_execution attestation, never from rowid or an updated timestamp.
            events = connection.execute("SELECT * FROM paper_ledger_attestation ORDER BY revision").fetchall()
            markers = connection.execute("SELECT * FROM paper_ledger_head_marker ORDER BY revision").fetchall()
            if len(events) != len(markers) or len(events) != int(latest["revision"]):
                raise PaperBrokerReconciliationError("paper immutable history sequence is incomplete")
            initial_sequences: dict[str, int] = {}
            previous_event = previous_marker = None
            for event, marker in zip(events, markers, strict=True):
                broker._validate_attestation_row(event)
                broker._validate_head_marker_row(marker)
                if (event["revision"] != marker["revision"] or marker["attestation_fingerprint"] != event["attestation_fingerprint"]
                        or event["ledger_generation"] != head["ledger_generation"]
                        or (previous_event is not None and event["previous_attestation_fingerprint"] != previous_event)
                        or (previous_marker is not None and marker["previous_head_marker_fingerprint"] != previous_marker)):
                    raise PaperBrokerReconciliationError("paper immutable history sequence is detached")
                previous_event, previous_marker = event["attestation_fingerprint"], marker["head_marker_fingerprint"]
                if event["event_kind"] == "intent_execution":
                    fingerprint = event["event_fingerprint"]
                    if fingerprint in initial_sequences:
                        raise PaperBrokerReconciliationError("paper initial history sequence is not unique")
                    initial_sequences[fingerprint] = int(event["revision"])
            rows = connection.execute("SELECT o.*,i.payload_json,i.initial_execution_id,i.persisted_at AS intent_persisted_at "
                                      "FROM paper_order o JOIN paper_intent i ON i.intent_id=o.intent_id WHERE o.account_id=?",
                                      (self.account_id,)).fetchall()
            fill_rows = connection.execute("SELECT f.* FROM paper_fill f JOIN paper_order o ON o.order_id=f.order_id "
                                            "WHERE o.account_id=? ORDER BY f.order_id,f.sequence", (self.account_id,)).fetchall()
            fills: dict[str, list[PaperHistoryFill]] = {}
            for row in fill_rows:
                fill = PaperHistoryFill(**broker._fill_from_row(row).model_dump(mode="python"), persisted_at=row["persisted_at"])
                if fill.persisted_at > cutoff or fill.executed_at > cutoff:
                    raise PaperBrokerReconciliationError("paper complete history fill is later than cutoff")
                fills.setdefault(fill.order_id, []).append(fill)
            history = []
            closed = {}
            for row in rows:
                order = broker._order_from_row(row)
                intent = PaperOrderIntent.model_validate_json(row["payload_json"])
                receipt = broker._execution_receipt(connection, execution_id=row["initial_execution_id"])
                if (receipt is None or receipt.persisted_at > cutoff or order.updated_at > cutoff
                        or broker._required_ledger_timestamp(row["intent_persisted_at"], label="history intent") > cutoff):
                    raise PaperBrokerReconciliationError("paper complete history order is later than cutoff")
                event_hash = canonical_sha256({"intent": intent.model_dump(mode="python"), "receipt": receipt.model_dump(mode="python")})
                sequence = initial_sequences.get(event_hash)
                if sequence is None:
                    raise PaperBrokerReconciliationError("paper order has no original immutable sequence")
                parts = tuple(fills.get(order.order_id, ()))
                final = receipt if not parts else broker._execution_receipt(connection, execution_id=parts[-1].execution_id)
                if final is None:
                    raise PaperBrokerReconciliationError("paper final execution receipt is missing")
                if order.status in (PaperOrderStatus.CANCELLED, PaperOrderStatus.EXPIRED):
                    closed[_close_event_fingerprint(order, final)] = order
                elif final.order != order:
                    raise PaperBrokerReconciliationError("paper order differs from its original final receipt")
                history.append(PaperPortfolioHistoryRecord(sequence=sequence, order=order, intent=intent, fills=parts))
            broker._verify_close_attestations(connection, closed=closed, cutoff=cutoff, head=head)
            return PaperPortfolioLedgerFrame(configuration_fingerprint=configuration.fingerprint, as_of=cutoff,
                                              ledger_revision=int(latest["revision"]), ledger_generation=head["ledger_generation"],
                                              head_fingerprint=head["head_marker_fingerprint"], account_id=self.account_id, account=account,
                                              missing_valuation_codes=tuple(sorted(missing)),
                                              reconciliation=reconciliation, history=tuple(sorted(history, key=lambda item: item.sequence)))
