"""Contemporaneous close NAV facts retained outside the financial ledger."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal, localcontext
from typing import Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, TypeAdapter, model_validator

from rquant.backtest.contracts import SSECalendar
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.paper_portfolio_ledger import PaperMissingClosePrices, PaperPortfolioLedgerSource
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, Sha256
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.strategy_promotion_contracts import NativeMinuteForwardConfiguration, NativeMinuteForwardValuation
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rquant.paper_research_runtime import NativeMinuteForwardState
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256, normalize_aware_utc

_SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_NAV_DAYS = 2520


class PaperClosePrice(RuntimeContractModel):
    ts_code: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    close_price: Decimal | None = Field(default=None, gt=0, le=Decimal("1000000000"), allow_inf_nan=False)
    trading_status: Literal["normal", "suspended", "missing", "error"]
    industry_l1: str | None = Field(default=None, min_length=1, max_length=80)
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    source_snapshot_id: Sha256

    @model_validator(mode="before")
    @classmethod
    def bounded_price(cls, value: object) -> object:
        if isinstance(value, Mapping) and value.get("close_price") is not None:
            _parse_decimal(value["close_price"], field_name="paper close price")
        return value

    @model_validator(mode="after")
    def actual_price(self) -> Self:
        if self.observed_at > self.available_at or (self.trading_status in ("missing", "error") and self.close_price is not None):
            raise ValueError("paper close lacks its actual price status and visibility")
        return self


class PaperCloseMaterials(RuntimeContractModel):
    contract: Literal["paper-close-materials/v1"] = "paper-close-materials/v1"
    configuration: PaperPortfolioConfiguration
    calendar: SSECalendar
    trade_date: date
    close_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    prices: tuple[PaperClosePrice, ...] = Field(max_length=500)

    @model_validator(mode="after")
    def exact_close(self) -> Self:
        expected = datetime.combine(self.trade_date, time(15), tzinfo=_SHANGHAI)
        if (self.trade_date not in self.calendar.dates or self.close_at != expected
                or self.configuration.configured_at > self.close_at or self.available_at < self.close_at
                or len({item.ts_code for item in self.prices}) != len(self.prices)
                or any(item.observed_at != self.close_at or item.available_at > self.available_at for item in self.prices)):
            raise ValueError("paper close does not bind its original open day and close cutoff")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperDailyNav(RuntimeContractModel):
    configuration_fingerprint: Sha256
    account_id: str
    trade_date: date
    calendar_source_identity: Sha256
    material_fingerprint: Sha256
    ledger_revision: int = Field(strict=True, ge=1)
    ledger_head_fingerprint: Sha256
    close_at: AwareUtcDatetime
    published_at: AwareUtcDatetime
    status: Literal["complete", "unavailable"]
    account: PaperAccountSnapshot | None = None
    normalized_nav: Decimal | None = Field(default=None, gt=0, allow_inf_nan=False)
    daily_return: Decimal | None = Field(default=None, gt=-1, allow_inf_nan=False)
    reason: str | None = Field(default=None, max_length=512)

    @model_validator(mode="after")
    def honest_gap(self) -> Self:
        if self.status == "unavailable" and (self.account is not None or self.normalized_nav is not None or self.daily_return is not None or not self.reason):
            raise ValueError("unavailable paper NAV cannot carry a substituted number")
        if self.status == "complete" and (self.account is None or self.normalized_nav is None or self.account.account_id != self.account_id):
            raise ValueError("complete paper NAV requires its original account")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class NativePaperCloseMaterials(PaperCloseMaterials):
    contract: Literal["native-forward-close-materials/v1"] = "native-forward-close-materials/v1"
    configuration: NativeMinuteForwardConfiguration
    trade_calendar_sha256: Sha256
    valuation: NativeMinuteForwardValuation

    @model_validator(mode="after")
    def full_original_native_valuation(self) -> Self:
        value, configuration = self.valuation, self.configuration
        if (value.input_hash, value.profile_hash, value.calendar_sha256, value.trade_date, value.as_of) != (
            configuration.fingerprint, configuration.execution_profile.profile_hash,
            self.trade_calendar_sha256, self.trade_date, self.close_at
        ) or self.trade_date <= configuration.paper_approved_at.astimezone(_SHANGHAI).date():
            raise ValueError("native close differs from its original profile, raw calendar or manual paper start")
        if value.observed_at > self.available_at:
            raise ValueError("native close cannot publish before its actual observation")
        expected = tuple((item.quote.ts_code, item.quote.context.executable_price,
            item.quote.snapshot_id, item.quote.available_at) for item in value.price_proofs)
        if tuple((item.ts_code, item.close_price, item.source_snapshot_id, item.available_at) for item in self.prices) != expected:
            raise ValueError("native close marks differ from full original PIT quote proofs")
        if value.status == "complete" and self.close_at - value.market_pointer.published_at > timedelta(
            seconds=configuration.execution_profile.quote_max_age_seconds):
            raise ValueError("native close market publication is stale")
        if len(self.model_dump_json().encode()) > 5 * 1024 * 1024:
            raise ValueError("native close exceeds the original published material budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


PaperCloseInput = PaperCloseMaterials | NativePaperCloseMaterials


class PaperPortfolioViewStore:
    def __init__(self, state: PaperPortfolioStateStore | NativeMinuteForwardState) -> None:
        self.state = state
        from rquant.paper_research_runtime import NativeMinuteForwardState

        if type(state) is NativeMinuteForwardState:
            with state._connection() as connection:
                tables = frozenset(row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                    "('portfolio_daily_nav','portfolio_close_material')"))
            if tables == {"portfolio_daily_nav", "portfolio_close_material"}:
                return
        with state._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS portfolio_daily_nav(configuration TEXT NOT NULL,trade_date TEXT NOT NULL,material TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(configuration,trade_date))")
            connection.execute("CREATE TABLE IF NOT EXISTS portfolio_close_material(configuration TEXT NOT NULL,trade_date TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(configuration,trade_date))")

    def nav_series(self) -> tuple[PaperDailyNav, ...]:
        with self.state._connection() as connection:
            rows = connection.execute("SELECT n.body,c.body FROM portfolio_daily_nav n LEFT JOIN portfolio_close_material c "
                                      "ON c.configuration=n.configuration AND c.trade_date=n.trade_date "
                                      "WHERE n.configuration=? ORDER BY n.trade_date LIMIT ?",
                                      (self.state.configuration.fingerprint, MAX_NAV_DAYS + 1)).fetchall()
        if len(rows) > MAX_NAV_DAYS:
            raise ValueError("paper NAV sequence exceeds its original 2520-day budget")
        if not rows:
            return ()
        if any(row[1] is None for row in rows):
            raise ValueError("paper NAV is missing its original close calendar material")
        records = tuple(PaperDailyNav.model_validate_json(row[0]) for row in rows)
        materials = tuple(TypeAdapter(PaperCloseInput).validate_json(row[1]) for row in rows)
        calendar = materials[-1].calendar
        if any(material.calendar != calendar or material.configuration != self.state.configuration
               or (record.material_fingerprint, record.calendar_source_identity, record.trade_date, record.close_at) !=
                  (material.fingerprint, calendar.source_identity, material.trade_date, material.close_at)
               for record, material in zip(records, materials, strict=True)):
            raise ValueError("paper NAV differs from its full original close calendar")
        days = calendar.dates[bisect_left(calendar.dates, records[0].trade_date):bisect_right(calendar.dates, records[-1].trade_date)]
        if len(days) > MAX_NAV_DAYS:
            raise ValueError("paper NAV calendar coverage exceeds its original 2520-day budget")
        indexed = {record.trade_date: record for record in records}
        if len(indexed) != len(records) or any(day not in days for day in indexed):
            raise ValueError("paper NAV dates are detached from the original calendar")
        result, witness_index = [], 0
        for day in days:
            if day in indexed:
                result.append(indexed[day])
                continue
            while records[witness_index].trade_date < day:
                witness_index += 1
            witness = records[witness_index]
            # This absence is visible at the next actual close publication.
            # Its revision/head cite that witness; no past account is invented.
            result.append(PaperDailyNav(configuration_fingerprint=witness.configuration_fingerprint,
                account_id=witness.account_id, trade_date=day, calendar_source_identity=calendar.source_identity,
                material_fingerprint=canonical_sha256({"contract": "paper-missing-close/v1", "calendar": calendar.source_identity,
                    "missing_day": day, "witness_material": witness.material_fingerprint, "witness_day": witness.trade_date,
                    "ledger_revision": witness.ledger_revision, "ledger_head": witness.ledger_head_fingerprint,
                    "published_at": witness.published_at}),
                ledger_revision=witness.ledger_revision, ledger_head_fingerprint=witness.ledger_head_fingerprint,
                close_at=datetime.combine(day, time(15), tzinfo=_SHANGHAI), published_at=witness.published_at,
                status="unavailable", reason="该交易日净值未发布。"))
        return tuple(result)

    def record_close(self, source: PaperPortfolioLedgerSource, material: PaperCloseInput, *, published_at: datetime) -> PaperDailyNav:
        material = TypeAdapter(PaperCloseInput).validate_python(material.model_dump(mode="python"))
        configuration = self.state.refresh_configuration()
        if material.configuration != configuration or type(source) is not PaperPortfolioLedgerSource:
            raise ValueError("paper close belongs to a different immutable configuration")
        day = material.trade_date.isoformat()
        with self.state._connection(write=True) as connection:
            old = connection.execute("SELECT material,body FROM portfolio_daily_nav WHERE configuration=? AND trade_date=?",
                                     (configuration.fingerprint, day)).fetchone()
            if old:
                if old[0] != material.fingerprint:
                    raise ValueError("original daily close material differs")
                return PaperDailyNav.model_validate_json(old[1])
            published = normalize_aware_utc(published_at)
            if (published < material.available_at or published.astimezone(_SHANGHAI).date() != material.trade_date
                    or published > material.close_at + timedelta(hours=6)):
                raise ValueError("daily paper NAV requires a contemporaneous close publication")
            previous = self.nav_series()
            if len(previous) >= MAX_NAV_DAYS or (previous and previous[-1].trade_date >= material.trade_date):
                raise ValueError("paper daily NAV is out of order or exceeds its fixed budget")
            prices = {item.ts_code: item.close_price for item in material.prices if item.close_price is not None and item.trading_status in ("normal", "suspended")}
            # Inspect current holdings from the same fixed original transaction.
            # Missing marks are never replaced by the last execution price.
            try:
                frame = source.read(configuration=configuration, as_of=material.close_at, prices=prices)
                missing = set(holding.code for holding in frame.account.holdings) - set(prices)
                if missing:
                    raise ValueError("缺少收盘估值：" + ", ".join(sorted(missing)))
                account = frame.account
                reason = None
                if type(material) is NativePaperCloseMaterials:
                    if material.valuation.status != "complete":
                        account = None
                        reason = "; ".join(material.valuation.unavailable_reasons)
                    elif account != material.valuation.account:
                        raise ValueError("native close account differs from its full original financial transaction")
            except PaperMissingClosePrices as exc:
                gap = exc.gap
                account = None
                frame = None
                reason = str(exc)
            with localcontext() as context:
                context.prec = 34
                normalized = None if account is None else account.nav / source.initial_cash
                daily = None
                if account is not None:
                    if not previous:
                        daily = normalized - 1
                    elif previous[-1].status == "complete":
                        indices = material.calendar.dates
                        index = indices.index(material.trade_date)
                        if index > 0 and indices[index - 1] == previous[-1].trade_date:
                            daily = account.nav / previous[-1].account.nav - 1
            result = PaperDailyNav(configuration_fingerprint=configuration.fingerprint, account_id=configuration.binding.account_id,
                                  trade_date=material.trade_date, calendar_source_identity=material.calendar.source_identity,
                                  material_fingerprint=material.fingerprint,
                                  ledger_revision=frame.ledger_revision if frame else gap.ledger_revision,
                                  ledger_head_fingerprint=frame.head_fingerprint if frame else gap.head_fingerprint, close_at=material.close_at, published_at=published,
                                  status="complete" if account is not None else "unavailable", account=account,
                                  normalized_nav=normalized, daily_return=daily, reason=reason)
            connection.execute("INSERT INTO portfolio_daily_nav VALUES(?,?,?,?)", (configuration.fingerprint, day, material.fingerprint, result.model_dump_json()))
            connection.execute("INSERT INTO portfolio_close_material VALUES(?,?,?)", (configuration.fingerprint, day, material.model_dump_json()))
        return result
