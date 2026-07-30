"""Single-writer SQLite runtime for deterministic A-share paper execution."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Annotated
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.paper_contracts import (
    PaperAccountSnapshot,
    PaperFill,
    PaperHolding,
    PaperOrder,
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSide,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

NonNegativeDecimal = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
PositiveDecimal = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
_CENT = Decimal("0.01")
_PRICE_TICK = Decimal("0.0001")
_BPS = Decimal("10000")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class DuplicateIntentConflictError(RuntimeError):
    """An existing intent id was reused with different immutable content."""


class PaperBrokerReconciliationError(RuntimeError):
    """The independently reconstructed ledger does not match stored balances."""


class BrokerCostPolicy(RuntimeContractModel):
    """Immutable costs used for one broker runtime generation."""

    commission_rate: NonNegativeDecimal
    minimum_commission: NonNegativeDecimal
    sell_stamp_tax_rate: NonNegativeDecimal
    buy_slippage_bps: NonNegativeDecimal = Decimal("0")
    sell_slippage_bps: NonNegativeDecimal = Decimal("0")

    @model_validator(mode="after")
    def validate_slippage(self) -> BrokerCostPolicy:
        if self.buy_slippage_bps >= _BPS or self.sell_slippage_bps >= _BPS:
            raise ValueError("slippage bps must be below 10000")
        if self.commission_rate >= 1 or self.sell_stamp_tax_rate >= 1:
            raise ValueError("cost rates must be below one")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class BrokerExecutionContext(RuntimeContractModel):
    """Frozen executable quote and explicit market/risk constraints."""

    executable_price: PositiveDecimal
    acquisition_available_date: date | None = None
    suspended: bool = False
    limit_locked: bool = False
    risk_rejected: bool = False


class PaperBrokerReconciliation(RuntimeContractModel):
    is_consistent: bool
    account_id: str = Field(min_length=1)
    order_count: int = Field(ge=0)
    fill_count: int = Field(ge=0)
    open_lot_quantity: int = Field(ge=0)
    cash: NonNegativeDecimal
    realized_pnl: Decimal = Field(allow_inf_nan=False)


def _money(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("money must be finite")
    return format(value, "f")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _intent_payload(intent: PaperOrderIntent) -> str:
    return json.dumps(
        intent.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


class PaperBrokerStore:
    """Own the only mutable paper ledger and serialize all writes through SQLite."""

    def __init__(
        self,
        path: Path,
        *,
        account_id: str,
        initial_cash: Decimal,
        cost_policy: BrokerCostPolicy,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id must not be empty")
        if not initial_cash.is_finite() or initial_cash <= 0:
            raise ValueError("initial_cash must be finite and positive")
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.account_id = account_id.strip()
        self.initial_cash = initial_cash
        self.cost_policy = cost_policy
        self.busy_timeout_ms = busy_timeout_ms
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        except BaseException:
            connection.close()
            raise
        return connection

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS broker_account (
                    account_id TEXT PRIMARY KEY,
                    initial_cash TEXT NOT NULL CHECK(typeof(initial_cash) = 'text'),
                    cash TEXT NOT NULL CHECK(typeof(cash) = 'text'),
                    realized_pnl TEXT NOT NULL CHECK(typeof(realized_pnl) = 'text'),
                    cost_policy_fingerprint TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_intent (
                    intent_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    payload_json TEXT NOT NULL,
                    persisted_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_order (
                    order_id TEXT PRIMARY KEY,
                    intent_id TEXT NOT NULL UNIQUE REFERENCES paper_intent(intent_id),
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    ts_code TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity % 100 = 0),
                    filled_quantity INTEGER NOT NULL CHECK(filled_quantity >= 0),
                    average_fill_price TEXT CHECK(
                        average_fill_price IS NULL OR typeof(average_fill_price) = 'text'
                    ),
                    status TEXT NOT NULL,
                    reject_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_fill (
                    fill_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL REFERENCES paper_order(order_id),
                    sequence INTEGER NOT NULL CHECK(sequence >= 1),
                    quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity % 100 = 0),
                    price TEXT NOT NULL CHECK(typeof(price) = 'text'),
                    commission TEXT NOT NULL CHECK(typeof(commission) = 'text'),
                    tax TEXT NOT NULL CHECK(typeof(tax) = 'text'),
                    executed_at TEXT NOT NULL,
                    price_snapshot_id TEXT NOT NULL,
                    UNIQUE(order_id, sequence)
                );
                CREATE TABLE IF NOT EXISTS paper_lot (
                    lot_id TEXT PRIMARY KEY REFERENCES paper_fill(fill_id),
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    ts_code TEXT NOT NULL,
                    acquisition_trade_date TEXT NOT NULL,
                    available_date TEXT NOT NULL,
                    original_quantity INTEGER NOT NULL CHECK(
                        original_quantity > 0 AND original_quantity % 100 = 0
                    ),
                    remaining_quantity INTEGER NOT NULL CHECK(
                        remaining_quantity >= 0 AND remaining_quantity % 100 = 0
                    ),
                    unit_cost TEXT NOT NULL CHECK(typeof(unit_cost) = 'text')
                );
                CREATE TABLE IF NOT EXISTS paper_lot_consumption (
                    fill_id TEXT NOT NULL REFERENCES paper_fill(fill_id),
                    lot_id TEXT NOT NULL REFERENCES paper_lot(lot_id),
                    quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity % 100 = 0),
                    unit_cost TEXT NOT NULL CHECK(typeof(unit_cost) = 'text'),
                    PRIMARY KEY(fill_id, lot_id)
                );
                """
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT initial_cash, cost_policy_fingerprint
                    FROM broker_account WHERE account_id = ?
                    """,
                    (self.account_id,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO broker_account(
                            account_id, initial_cash, cash, realized_pnl, cost_policy_fingerprint
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            self.account_id,
                            _money(self.initial_cash),
                            _money(self.initial_cash),
                            "0",
                            self.cost_policy.fingerprint,
                        ),
                    )
                elif Decimal(row["initial_cash"]) != self.initial_cash:
                    raise ValueError("existing account initial_cash does not match")
                elif row["cost_policy_fingerprint"] != self.cost_policy.fingerprint:
                    raise ValueError("existing account cost_policy does not match")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _before_commit(self, _connection: sqlite3.Connection) -> None:
        """Fault-injection boundary used to prove whole-submit rollback."""

    def submit_intent(
        self,
        intent: PaperOrderIntent,
        *,
        decision_time: datetime,
        trade_date: date,
        quote: BrokerExecutionContext,
    ) -> PaperOrder:
        decision_time = _utc(decision_time)
        if intent.account_id != self.account_id:
            raise ValueError("intent account_id does not match broker account")
        if decision_time < intent.available_at:
            raise ValueError("decision_time cannot precede intent available_at")
        if trade_date != decision_time.astimezone(_SHANGHAI).date():
            raise ValueError("trade_date must match decision_time in Asia/Shanghai")
        payload = _intent_payload(intent)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT payload_json FROM paper_intent WHERE intent_id = ?",
                    (intent.intent_id,),
                ).fetchone()
                if existing is not None:
                    if existing["payload_json"] != payload:
                        raise DuplicateIntentConflictError(
                            f"intent_id {intent.intent_id} already has different content"
                        )
                    order = self._order_for_intent(connection, intent.intent_id)
                    if order is None:
                        raise PaperBrokerReconciliationError(
                            "persisted intent is missing its order"
                        )
                    connection.rollback()
                    return order

                connection.execute(
                    """
                    INSERT INTO paper_intent(intent_id, account_id, payload_json, persisted_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (intent.intent_id, self.account_id, payload, _utc_iso(decision_time)),
                )
                order, fill = self._evaluate_intent(
                    connection,
                    intent=intent,
                    decision_time=decision_time,
                    trade_date=trade_date,
                    quote=quote,
                )
                self._insert_order(connection, order)
                if fill is not None:
                    self._apply_fill(
                        connection,
                        intent=intent,
                        fill=fill,
                        trade_date=trade_date,
                        available_date=quote.acquisition_available_date,
                    )
                self._before_commit(connection)
                connection.commit()
                return order
            except BaseException:
                connection.rollback()
                raise

    def _evaluate_intent(
        self,
        connection: sqlite3.Connection,
        *,
        intent: PaperOrderIntent,
        decision_time: datetime,
        trade_date: date,
        quote: BrokerExecutionContext,
    ) -> tuple[PaperOrder, PaperFill | None]:
        reject_reason: PaperRejectReason | None = None
        if quote.suspended:
            reject_reason = PaperRejectReason.SUSPENDED
        elif quote.limit_locked:
            reject_reason = PaperRejectReason.LIMIT_LOCKED
        elif quote.risk_rejected:
            reject_reason = PaperRejectReason.RISK_REJECTED
        elif decision_time >= intent.expires_at:
            reject_reason = PaperRejectReason.EXPIRED

        if reject_reason is not None:
            return self._new_order(intent, decision_time, reject_reason=reject_reason), None
        if decision_time < intent.earliest_execution_at:
            return self._new_order(intent, decision_time), None

        fill_price = self._fill_price(intent.side, quote.executable_price)
        # Frozen conservative rule: a missed limit remains ACCEPTED with no cash
        # reservation. A retry of the same immutable intent returns this evidence.
        if intent.order_type is PaperOrderType.LIMIT:
            assert intent.limit_price is not None
            misses_limit = (intent.side is PaperSide.BUY and fill_price > intent.limit_price) or (
                intent.side is PaperSide.SELL and fill_price < intent.limit_price
            )
            if misses_limit:
                return self._new_order(intent, decision_time), None

        commission = self._commission(fill_price * intent.quantity)
        tax = (
            self._stamp_tax(fill_price * intent.quantity)
            if intent.side is PaperSide.SELL
            else Decimal("0.00")
        )
        if intent.side is PaperSide.BUY:
            if quote.acquisition_available_date is None:
                raise ValueError("BUY execution requires acquisition_available_date")
            if quote.acquisition_available_date <= trade_date:
                raise ValueError("BUY available date must be after acquisition trade date")
            cash = self._account_values(connection)[0]
            if cash < fill_price * intent.quantity + commission:
                return self._new_order(
                    intent,
                    decision_time,
                    reject_reason=PaperRejectReason.INSUFFICIENT_CASH,
                ), None
        else:
            total, available = self._position_quantities(
                connection,
                ts_code=intent.ts_code,
                trade_date=trade_date,
            )
            if total < intent.quantity:
                return self._new_order(
                    intent,
                    decision_time,
                    reject_reason=PaperRejectReason.INSUFFICIENT_POSITION,
                ), None
            if available < intent.quantity:
                return self._new_order(
                    intent,
                    decision_time,
                    reject_reason=PaperRejectReason.T_PLUS_ONE,
                ), None

        filled = self._new_order(
            intent,
            decision_time,
            status=PaperOrderStatus.FILLED,
            fill_price=fill_price,
        )
        fill = PaperFill(
            order_id=filled.order_id,
            sequence=1,
            quantity=intent.quantity,
            price=fill_price,
            commission=commission,
            tax=tax,
            executed_at=decision_time,
            price_snapshot_id=intent.price_snapshot_id,
        )
        return filled, fill

    def _new_order(
        self,
        intent: PaperOrderIntent,
        decision_time: datetime,
        *,
        status: PaperOrderStatus = PaperOrderStatus.ACCEPTED,
        reject_reason: PaperRejectReason | None = None,
        fill_price: Decimal | None = None,
    ) -> PaperOrder:
        if reject_reason is not None:
            status = PaperOrderStatus.REJECTED
        filled_quantity = intent.quantity if status is PaperOrderStatus.FILLED else 0
        return PaperOrder(
            intent_id=intent.intent_id,
            account_id=self.account_id,
            ts_code=intent.ts_code,
            side=intent.side,
            order_type=intent.order_type,
            quantity=intent.quantity,
            filled_quantity=filled_quantity,
            average_fill_price=fill_price,
            status=status,
            reject_reason=reject_reason,
            created_at=decision_time,
            updated_at=decision_time,
        )

    def _fill_price(self, side: PaperSide, executable_price: Decimal) -> Decimal:
        if side is PaperSide.BUY:
            multiplier = Decimal("1") + self.cost_policy.buy_slippage_bps / _BPS
        else:
            multiplier = Decimal("1") - self.cost_policy.sell_slippage_bps / _BPS
        return (executable_price * multiplier).quantize(_PRICE_TICK, rounding=ROUND_HALF_UP)

    def _commission(self, notional: Decimal) -> Decimal:
        return max(
            notional * self.cost_policy.commission_rate,
            self.cost_policy.minimum_commission,
        ).quantize(_CENT, rounding=ROUND_HALF_UP)

    def _stamp_tax(self, notional: Decimal) -> Decimal:
        return (notional * self.cost_policy.sell_stamp_tax_rate).quantize(
            _CENT, rounding=ROUND_HALF_UP
        )

    def _insert_order(self, connection: sqlite3.Connection, order: PaperOrder) -> None:
        connection.execute(
            """
            INSERT INTO paper_order(
                order_id, intent_id, account_id, ts_code, side, order_type,
                quantity, filled_quantity, average_fill_price, status,
                reject_reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order.order_id,
                order.intent_id,
                order.account_id,
                order.ts_code,
                order.side.value,
                order.order_type.value,
                order.quantity,
                order.filled_quantity,
                _money(order.average_fill_price) if order.average_fill_price is not None else None,
                order.status.value,
                order.reject_reason.value if order.reject_reason is not None else None,
                _utc_iso(order.created_at),
                _utc_iso(order.updated_at),
            ),
        )

    def _apply_fill(
        self,
        connection: sqlite3.Connection,
        *,
        intent: PaperOrderIntent,
        fill: PaperFill,
        trade_date: date,
        available_date: date | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO paper_fill(
                fill_id, order_id, sequence, quantity, price, commission,
                tax, executed_at, price_snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill.fill_id,
                fill.order_id,
                fill.sequence,
                fill.quantity,
                _money(fill.price),
                _money(fill.commission),
                _money(fill.tax),
                _utc_iso(fill.executed_at),
                fill.price_snapshot_id,
            ),
        )
        cash, realized = self._account_values(connection)
        if intent.side is PaperSide.BUY:
            assert available_date is not None
            total_cost = fill.notional + fill.commission
            connection.execute(
                "UPDATE broker_account SET cash = ? WHERE account_id = ?",
                (_money(cash - total_cost), self.account_id),
            )
            connection.execute(
                """
                INSERT INTO paper_lot(
                    lot_id, account_id, ts_code, acquisition_trade_date,
                    available_date, original_quantity, remaining_quantity, unit_cost
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.fill_id,
                    self.account_id,
                    intent.ts_code,
                    trade_date.isoformat(),
                    available_date.isoformat(),
                    fill.quantity,
                    fill.quantity,
                    _money(total_cost / fill.quantity),
                ),
            )
            return

        remaining = fill.quantity
        cost_basis = Decimal("0")
        lots = connection.execute(
            """
            SELECT lot_id, remaining_quantity, unit_cost
            FROM paper_lot
            WHERE account_id = ? AND ts_code = ? AND remaining_quantity > 0
              AND available_date <= ?
            ORDER BY available_date, acquisition_trade_date, lot_id
            """,
            (self.account_id, intent.ts_code, trade_date.isoformat()),
        ).fetchall()
        for lot in lots:
            if remaining == 0:
                break
            consumed = min(remaining, int(lot["remaining_quantity"]))
            unit_cost = Decimal(lot["unit_cost"])
            connection.execute(
                "UPDATE paper_lot SET remaining_quantity = remaining_quantity - ? WHERE lot_id = ?",
                (consumed, lot["lot_id"]),
            )
            connection.execute(
                """
                INSERT INTO paper_lot_consumption(fill_id, lot_id, quantity, unit_cost)
                VALUES (?, ?, ?, ?)
                """,
                (fill.fill_id, lot["lot_id"], consumed, _money(unit_cost)),
            )
            cost_basis += unit_cost * consumed
            remaining -= consumed
        if remaining:
            raise PaperBrokerReconciliationError("available lot allocation became incomplete")
        net_proceeds = fill.notional - fill.commission - fill.tax
        connection.execute(
            """
            UPDATE broker_account SET cash = ?, realized_pnl = ? WHERE account_id = ?
            """,
            (
                _money(cash + net_proceeds),
                _money(realized + net_proceeds - cost_basis),
                self.account_id,
            ),
        )

    def _account_values(self, connection: sqlite3.Connection) -> tuple[Decimal, Decimal]:
        row = connection.execute(
            "SELECT cash, realized_pnl FROM broker_account WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if row is None:
            raise PaperBrokerReconciliationError("broker account is missing")
        return Decimal(row["cash"]), Decimal(row["realized_pnl"])

    def _position_quantities(
        self,
        connection: sqlite3.Connection,
        *,
        ts_code: str,
        trade_date: date,
    ) -> tuple[int, int]:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(remaining_quantity), 0) AS total,
                COALESCE(SUM(
                    CASE WHEN available_date <= ? THEN remaining_quantity ELSE 0 END
                ), 0) AS available
            FROM paper_lot
            WHERE account_id = ? AND ts_code = ?
            """,
            (trade_date.isoformat(), self.account_id, ts_code),
        ).fetchone()
        return int(row["total"]), int(row["available"])

    def order(self, order_id: str) -> PaperOrder | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_order WHERE account_id = ? AND order_id = ?",
                (self.account_id, order_id),
            ).fetchone()
            return self._order_from_row(row) if row is not None else None

    def order_for_intent(self, intent_id: str) -> PaperOrder | None:
        with self._connect() as connection:
            return self._order_for_intent(connection, intent_id)

    def _order_for_intent(
        self, connection: sqlite3.Connection, intent_id: str
    ) -> PaperOrder | None:
        row = connection.execute(
            "SELECT * FROM paper_order WHERE account_id = ? AND intent_id = ?",
            (self.account_id, intent_id),
        ).fetchone()
        return self._order_from_row(row) if row is not None else None

    @staticmethod
    def _order_from_row(row: sqlite3.Row) -> PaperOrder:
        return PaperOrder(
            order_id=row["order_id"],
            intent_id=row["intent_id"],
            account_id=row["account_id"],
            ts_code=row["ts_code"],
            side=row["side"],
            order_type=row["order_type"],
            quantity=row["quantity"],
            filled_quantity=row["filled_quantity"],
            average_fill_price=row["average_fill_price"],
            status=row["status"],
            reject_reason=row["reject_reason"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def fills(self, order_id: str | None = None) -> tuple[PaperFill, ...]:
        with self._connect() as connection:
            if order_id is None:
                rows = connection.execute(
                    """
                    SELECT f.* FROM paper_fill AS f
                    JOIN paper_order AS o ON o.order_id = f.order_id
                    WHERE o.account_id = ? ORDER BY f.executed_at, f.fill_id
                    """,
                    (self.account_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT f.* FROM paper_fill AS f
                    JOIN paper_order AS o ON o.order_id = f.order_id
                    WHERE o.account_id = ? AND f.order_id = ?
                    ORDER BY f.sequence
                    """,
                    (self.account_id, order_id),
                ).fetchall()
        return tuple(self._fill_from_row(row) for row in rows)

    @staticmethod
    def _fill_from_row(row: sqlite3.Row) -> PaperFill:
        return PaperFill(
            fill_id=row["fill_id"],
            order_id=row["order_id"],
            sequence=row["sequence"],
            quantity=row["quantity"],
            price=row["price"],
            commission=row["commission"],
            tax=row["tax"],
            executed_at=row["executed_at"],
            price_snapshot_id=row["price_snapshot_id"],
        )

    def account_snapshot(
        self,
        *,
        as_of: AwareUtcDatetime,
        market_prices: Mapping[str, Decimal],
    ) -> PaperAccountSnapshot:
        as_of = _utc(as_of)
        as_of_trade_date = as_of.astimezone(_SHANGHAI).date().isoformat()
        with self._connect() as connection:
            latest = connection.execute(
                """
                SELECT MAX(updated_at) AS latest_at FROM paper_order
                WHERE account_id = ?
                """,
                (self.account_id,),
            ).fetchone()["latest_at"]
            if latest is not None and as_of < _utc(datetime.fromisoformat(latest)):
                raise ValueError("as_of cannot precede the latest ledger event")
            cash, realized = self._account_values(connection)
            rows = connection.execute(
                """
                SELECT
                    ts_code,
                    SUM(remaining_quantity) AS quantity,
                    SUM(CASE WHEN available_date <= ? THEN remaining_quantity ELSE 0 END)
                        AS available_quantity
                FROM paper_lot
                WHERE account_id = ? AND remaining_quantity > 0
                GROUP BY ts_code ORDER BY ts_code
                """,
                (as_of_trade_date, self.account_id),
            ).fetchall()
            holdings: list[PaperHolding] = []
            for row in rows:
                code = str(row["ts_code"])
                if code not in market_prices:
                    raise ValueError(f"missing market price for {code}")
                lot_rows = connection.execute(
                    """
                    SELECT remaining_quantity, unit_cost FROM paper_lot
                    WHERE account_id = ? AND ts_code = ? AND remaining_quantity > 0
                    """,
                    (self.account_id, code),
                ).fetchall()
                quantity = int(row["quantity"])
                cost = sum(
                    (
                        Decimal(lot["unit_cost"]) * int(lot["remaining_quantity"])
                        for lot in lot_rows
                    ),
                    Decimal("0"),
                )
                available = int(row["available_quantity"])
                holdings.append(
                    PaperHolding(
                        code=code,
                        quantity=quantity,
                        available_quantity=available,
                        frozen_quantity=quantity - available,
                        average_cost=cost / quantity,
                        market_price=market_prices[code],
                    )
                )
        unrealized = sum(
            (
                (holding.market_price - holding.average_cost) * holding.quantity
                for holding in holdings
            ),
            Decimal("0"),
        )
        holdings_value = sum(
            (holding.market_price * holding.quantity for holding in holdings),
            Decimal("0"),
        )
        return PaperAccountSnapshot(
            account_id=self.account_id,
            as_of_time=as_of,
            cash=cash,
            available_cash=cash,
            frozen_cash=Decimal("0"),
            holdings=tuple(holdings),
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            nav=cash + holdings_value,
        )

    def reconcile(self) -> PaperBrokerReconciliation:
        errors: list[str] = []
        with self._connect() as connection:
            account = connection.execute(
                """
                SELECT initial_cash, cash, realized_pnl FROM broker_account
                WHERE account_id = ?
                """,
                (self.account_id,),
            ).fetchone()
            if account is None:
                raise PaperBrokerReconciliationError("broker account is missing")
            orders = connection.execute(
                "SELECT * FROM paper_order WHERE account_id = ? ORDER BY order_id",
                (self.account_id,),
            ).fetchall()
            fills = connection.execute(
                """
                SELECT f.*, o.side FROM paper_fill AS f
                JOIN paper_order AS o ON o.order_id = f.order_id
                WHERE o.account_id = ? ORDER BY f.fill_id
                """,
                (self.account_id,),
            ).fetchall()
            fill_by_order: dict[str, list[sqlite3.Row]] = {}
            for fill in fills:
                fill_by_order.setdefault(str(fill["order_id"]), []).append(fill)
            for row in orders:
                order = self._order_from_row(row)
                order_fills = fill_by_order.get(order.order_id, [])
                fill_quantity = sum(int(fill["quantity"]) for fill in order_fills)
                if fill_quantity != order.filled_quantity:
                    errors.append(f"order {order.order_id} fill quantity mismatch")
                if order.status is PaperOrderStatus.FILLED and len(order_fills) != 1:
                    errors.append(f"order {order.order_id} must have one fill")
                if order.status is not PaperOrderStatus.FILLED and order_fills:
                    errors.append(f"unfilled order {order.order_id} has fills")

            expected_cash = Decimal(account["initial_cash"])
            expected_realized = Decimal("0")
            for fill in fills:
                parsed = self._fill_from_row(fill)
                if fill["side"] == PaperSide.BUY.value:
                    expected_cash -= parsed.notional + parsed.commission
                    lot = connection.execute(
                        """
                        SELECT original_quantity FROM paper_lot WHERE lot_id = ?
                        """,
                        (parsed.fill_id,),
                    ).fetchone()
                    if lot is None or int(lot["original_quantity"]) != parsed.quantity:
                        errors.append(f"buy fill {parsed.fill_id} lot mismatch")
                else:
                    expected_cash += parsed.notional - parsed.commission - parsed.tax
                    allocations = connection.execute(
                        """
                        SELECT quantity, unit_cost FROM paper_lot_consumption
                        WHERE fill_id = ?
                        """,
                        (parsed.fill_id,),
                    ).fetchall()
                    allocated = sum(int(item["quantity"]) for item in allocations)
                    if allocated != parsed.quantity:
                        errors.append(f"sell fill {parsed.fill_id} allocation mismatch")
                    cost_basis = sum(
                        (
                            Decimal(item["unit_cost"]) * int(item["quantity"])
                            for item in allocations
                        ),
                        Decimal("0"),
                    )
                    expected_realized += (
                        parsed.notional - parsed.commission - parsed.tax - cost_basis
                    )

            lots = connection.execute(
                """
                SELECT lot_id, original_quantity, remaining_quantity FROM paper_lot
                WHERE account_id = ?
                """,
                (self.account_id,),
            ).fetchall()
            for lot in lots:
                consumed = connection.execute(
                    """
                    SELECT COALESCE(SUM(quantity), 0) FROM paper_lot_consumption
                    WHERE lot_id = ?
                    """,
                    (lot["lot_id"],),
                ).fetchone()[0]
                if int(lot["remaining_quantity"]) + int(consumed) != int(lot["original_quantity"]):
                    errors.append(f"lot {lot['lot_id']} quantity mismatch")

            stored_cash = Decimal(account["cash"])
            stored_realized = Decimal(account["realized_pnl"])
            if stored_cash != expected_cash:
                errors.append("cash does not reconcile from fills")
            if stored_realized != expected_realized:
                errors.append("realized_pnl does not reconcile from fills and lots")
            open_quantity = sum(int(lot["remaining_quantity"]) for lot in lots)
        if errors:
            raise PaperBrokerReconciliationError("; ".join(errors))
        return PaperBrokerReconciliation(
            is_consistent=True,
            account_id=self.account_id,
            order_count=len(orders),
            fill_count=len(fills),
            open_lot_quantity=open_quantity,
            cash=stored_cash,
            realized_pnl=stored_realized,
        )
