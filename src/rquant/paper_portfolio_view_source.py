"""Publish contemporary marks and complete history from the original read transaction."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import TYPE_CHECKING, Self
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.paper_broker import PaperBrokerStore
from rquant.paper_operator_commands import PaperOperatorApplication
from rquant.paper_portfolio_exposure_source import PaperExposureReadReceipt
from rquant.paper_portfolio_models import Sha256
from rquant.paper_portfolio_projection import (
    PaperDailyNavPoint,
    PaperPortfolioPublishedAccount,
    PaperPortfolioSnapshot,
)
from rquant.paper_portfolio_runtime import PaperPortfolioRuntime
from rquant.paper_portfolio_views import (
    PaperCloseMaterials,
    PaperClosePrice,
    PaperPortfolioViewStore,
)
from rquant.paper_signal_worker import PaperSignalQueueStore
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc

_SHANGHAI = ZoneInfo("Asia/Shanghai")
if TYPE_CHECKING:
    from rquant.paper_research_artifact import PaperResearchResultReader


class PaperPublicationReservation(RuntimeContractModel):
    sequence: int = Field(strict=True, ge=1, lt=2**63 - 1)
    identity: Sha256
    snapshot: PaperPortfolioSnapshot

    @model_validator(mode="after")
    def exact_original_material(self) -> Self:
        if self.identity != paper_portfolio_publication_identity(self.snapshot):
            raise ValueError("paper publication state lost its complete original material")
        return self


class PaperPortfolioViewSource:
    def __init__(
        self,
        runtime: PaperPortfolioRuntime,
        *,
        broker: PaperBrokerStore,
        queue: PaperSignalQueueStore,
        health_metrics_enabled: bool = False,
    ) -> None:
        if type(health_metrics_enabled) is not bool:
            raise TypeError("paper health opt-in must be bool")
        if type(runtime) is not PaperPortfolioRuntime or type(queue) is not PaperSignalQueueStore:
            raise TypeError("paper publication requires its concrete original role and queue")
        runtime.require_broker(broker)
        if queue.policy.account_id != runtime.state.configuration.binding.account_id:
            raise ValueError("paper view queue belongs to a different account")
        self.runtime, self.broker, self.queue = runtime, broker, queue
        self.health_metrics_enabled = health_metrics_enabled
        self.views = PaperPortfolioViewStore(runtime.state)
        with runtime.state._connection(write=True) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS portfolio_publication("
                "singleton INTEGER PRIMARY KEY CHECK(singleton=1),sequence INTEGER NOT NULL,"
                "identity TEXT NOT NULL,body TEXT NOT NULL)"
            )

    @property
    def research_results(self) -> PaperResearchResultReader | None:
        return self.runtime.research_results

    @research_results.setter
    def research_results(self, value: PaperResearchResultReader) -> None:
        self.runtime.research_results = value

    def publication_sequence(self, value: PaperPortfolioSnapshot, *, minimum_sequence: int) -> int:
        identity = paper_portfolio_publication_identity(value)
        if type(minimum_sequence) is not int or not 0 <= minimum_sequence < 2**63 - 1:
            raise ValueError("paper source publication sequence exceeds its original budget")
        with self.runtime.state._connection(write=True) as connection:
            row = connection.execute(
                "SELECT sequence,identity,body FROM portfolio_publication WHERE singleton=1"
            ).fetchone()
            old = PaperPublicationReservation.model_validate_json(row[2]) if row else None
            if old is not None:
                if (old.sequence, old.identity) != tuple(row[:2]):
                    raise ValueError("paper publication sequence or complete material was replaced")
                if old.identity != identity and value.available_at < old.snapshot.available_at:
                    raise ValueError("paper publication material arrived out of order")
            sequence = (
                max(minimum_sequence, old.sequence + (old.identity != identity))
                if old
                else minimum_sequence + 1
            )
            if sequence >= 2**63 - 1:
                raise ValueError("paper source publication sequence is exhausted")
            reservation = PaperPublicationReservation(
                sequence=sequence, identity=identity, snapshot=value
            )
            connection.execute(
                "INSERT OR REPLACE INTO portfolio_publication VALUES(1,?,?,?)",
                (sequence, identity, reservation.model_dump_json()),
            )
        return sequence

    def read(self, *, as_of: datetime) -> PaperPortfolioPublishedAccount:
        runtime = self.runtime
        cutoff = normalize_aware_utc(as_of)
        with runtime.state._connection(write=True):
            configuration = runtime.state.refresh_configuration()
            material, reason = None, None
            try:
                material = runtime.materials.latest(decision_at=cutoff)
                if material.observed_at < cutoff - timedelta(seconds=90):
                    reason = "缺少当前估值"
            except ValueError as exc:
                reason = str(exc)
            prices = (
                {
                    item.ts_code: item.valuation_price
                    for item in material.facts
                    if item.valuation_price is not None
                    and item.trading_status in ("normal", "suspended")
                }
                if material is not None and reason is None
                else {}
            )
            source = runtime.ledger_source_for(self.broker)
            frame = source.read(
                configuration=configuration, as_of=cutoff, prices=prices, allow_missing_prices=True
            )
            if frame.account is None:
                reason = reason or "缺少当前估值：" + ", ".join(frame.missing_valuation_codes)
            calendar = runtime.calendar
            if material is not None and calendar is not None:
                local_close = material.observed_at.astimezone(_SHANGHAI)
                if (
                    local_close.time() == time(15)
                    and local_close.date() in calendar.dates
                    and all(item.observed_at == material.observed_at for item in material.facts)
                ):
                    close = PaperCloseMaterials(
                        configuration=configuration,
                        calendar=calendar,
                        trade_date=local_close.date(),
                        close_at=material.observed_at,
                        available_at=material.available_at,
                        prices=tuple(
                            PaperClosePrice(
                                ts_code=item.ts_code,
                                close_price=item.valuation_price,
                                trading_status=item.trading_status,
                                industry_l1=item.industry_l1,
                                observed_at=item.observed_at,
                                available_at=item.available_at,
                                source_snapshot_id=item.source_snapshot_id,
                            )
                            for item in material.facts
                        ),
                    )
                    self.views.record_close(source, close, published_at=cutoff)
            reduction = (
                runtime.reduction_status(
                    self.broker, self.queue, as_of=cutoff, prices=prices, frame=frame
                )
                if frame.account is not None
                else None
            )
            exposure = (
                runtime.exposure_store.exposure(
                    frame,
                    facts=material.facts,
                    as_of=cutoff,
                    **({"include_health_receipt": True} if self.health_metrics_enabled else {}),
                )
                if runtime.exposure_store is not None
                and frame.account is not None
                and material is not None
                and reason is None
                else None
            )
            exposure_receipt = exposure if isinstance(exposure, PaperExposureReadReceipt) else None
            if exposure_receipt is not None:
                exposure = exposure_receipt.result
            attribution = (
                runtime.exposure_store.attribution(as_of=cutoff)
                if runtime.exposure_store is not None
                else None
            )
            research = ()
            if self.research_results is not None:
                with runtime.state._connection() as connection:
                    rows = connection.execute(
                        "SELECT command_id FROM paper_research_admissions "
                        "ORDER BY json_extract(owned_body,'$.accepted_at') DESC,"
                        "command_id DESC LIMIT 20"
                    ).fetchall()
                research = tuple(
                    self.research_results.summary(
                        account_id=configuration.binding.account_id,
                        job_id=UUID(row[0]),
                        owner_id=configuration.binding.owner_id,
                        as_of=cutoff,
                    )
                    for row in rows
                )
            nav = tuple(PaperDailyNavPoint.from_record(item) for item in self.views.nav_series())
            operator = runtime.operator.current()
            if operator.configuration_fingerprint != configuration.fingerprint:
                operator = PaperOperatorApplication.model_validate(
                    operator.model_copy(
                        update={
                            "configuration_fingerprint": configuration.fingerprint,
                            "status": "unavailable",
                            "paused": True,
                            "reason": "配置等待角色应用。",
                        }
                    ).model_dump(mode="python")
                )
            value = PaperPortfolioPublishedAccount(
                metadata_identity=runtime.state.identity(),
                configuration=configuration,
                operator=operator,
                frame=frame,
                market_material=material if reason is None else None,
                status="complete" if reason is None else "unavailable",
                reason=reason,
                nav=nav,
                recent_research=research,
                calendar=calendar,
                risk=runtime.state.last_observation(),
                reduction=reduction,
                exposure=exposure,
                exposure_reason="缺少同期基准行业权重" if exposure is None else exposure.reason,
                exposure_receipt=exposure_receipt,
                attribution=attribution,
            )
            dates = value.complete_comparison_dates()
            band = next(
                (
                    item.sealed.band
                    for item in research
                    if dates is not None
                    and item.sealed is not None
                    and item.sealed.band is not None
                    and item.configuration_fingerprint == configuration.fingerprint
                    and item.sealed.band.dates == dates
                ),
                None,
            )
            update = {"band": band}
            value = PaperPortfolioPublishedAccount.model_validate(
                value.model_copy(update=update).model_dump(mode="python")
            )
            return publish_paper_band_position(value)


def publish_paper_band_position(
    value: PaperPortfolioPublishedAccount,
) -> PaperPortfolioPublishedAccount:
    value = PaperPortfolioPublishedAccount.model_validate(value.model_dump(mode="python"))
    band = value.band
    if (
        band is None
        or value.complete_comparison_dates() != band.dates
        or not any(
            item.sealed is not None
            and item.sealed.band == band
            and item.configuration_fingerprint == value.configuration.fingerprint
            for item in value.recent_research
        )
    ):
        return value
    last, bound = value.nav[-1].normalized_nav, band.points[-1]
    position = "inside" if bound.lower <= last <= bound.upper else "outside"
    return PaperPortfolioPublishedAccount.model_validate(
        value.model_copy(update={"band_position": position}).model_dump(mode="python")
    )


def paper_portfolio_publication_identity(value: PaperPortfolioSnapshot) -> str:
    body = value.model_dump(mode="python")
    body.pop("available_at")
    for account in body["accounts"]:
        account["operator"].pop("observed_at")
        if account["frame"] is not None:
            account["frame"].pop("as_of")
            if account["frame"]["account"] is not None:
                account["frame"]["account"].pop("as_of_time")
        for key in ("reduction", "exposure"):
            if account[key] is not None:
                account[key].pop("as_of")
                account[key].pop("ledger_frame_fingerprint")
    return canonical_sha256(body)
