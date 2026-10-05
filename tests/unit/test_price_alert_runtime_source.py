from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pandas as pd
import pytest

from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.price_alert_runtime_source import (
    PriceQuoteRequestBinding,
    freeze_price_quote_request,
    read_latest_price_quote_snapshot,
    read_price_alert_scope,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.watchlist_quote_gateway import WatchlistQuoteGateway, WatchlistQuoteGatewayConfig

AT = datetime(2026, 10, 5, 2, 0, tzinfo=UTC)


def test_request_binds_original_preimage_and_pre_call_immutable_bytes(tmp_path: Path) -> None:
    tmp_path.chmod(0o700)
    facts = dict(
        source="akshare.stock_zh_a_spot",
        quote_source_generation_id="1" * 64,
        scope_generation_id="2" * 64,
        scope_manifest_sha256="3" * 64,
        codes=("600000.SH",),
        scheduled_at=AT,
        universe_as_of=AT,
        trade_date=AT.date(),
        schema_version=2,
    )
    binding = PriceQuoteRequestBinding.create(**facts)
    assert binding.request_id == canonical_sha256(
        {
            "source": facts["source"],
            "codes": ("600000.SH",),
            "scheduled_at": AT,
            "universe_as_of": AT,
            "trade_date": AT.date(),
            "schema_version": 2,
        }
    )
    path = freeze_price_quote_request(tmp_path, binding)
    assert path.read_bytes() == binding.wire_bytes()
    assert freeze_price_quote_request(tmp_path, binding) == path
    with pytest.raises(ValueError):
        PriceQuoteRequestBinding.create(**{**facts, "codes": ("600000.SH", "600000.SH")})
    with pytest.raises(ValueError):
        freeze_price_quote_request(
            tmp_path, binding.model_copy(update={"scope_manifest_sha256": "4" * 64})
        )


def quote_fixture(tmp_path: Path) -> tuple[LiveBatchSpool, PriceQuoteRequestBinding, Path]:
    tmp_path.chmod(0o700)
    spool = LiveBatchSpool(tmp_path / "spool")
    config = WatchlistQuoteGatewayConfig(
        producer_version="test", producer_commit="b" * 40, rollout_mode="published"
    )
    binding = PriceQuoteRequestBinding.create(
        source=config.source,
        quote_source_generation_id=spool._source_generation(LiveChannel.WATCHLIST_QUOTE),
        scope_generation_id="2" * 64,
        scope_manifest_sha256="3" * 64,
        codes=("600000.SH",),
        scheduled_at=AT,
        universe_as_of=AT,
        trade_date=AT.date(),
        schema_version=2,
    )
    root = tmp_path / "requests"
    root.mkdir(mode=0o700)
    freeze_price_quote_request(root, binding)

    def provider(
        codes: tuple[str, ...], *, timeout_seconds: float, on_started: object
    ) -> pd.DataFrame:
        on_started(AT)
        return pd.DataFrame(
            [
                dict(
                    ts_code="600000.SH",
                    price=10.125,
                    open=10.0,
                    high=10.2,
                    low=9.9,
                    volume=100.0,
                    amount=1000.0,
                    source_observed_at=AT,
                )
            ]
        )

    gateway = WatchlistQuoteGateway(config=config, spool=spool, provider=provider, clock=lambda: AT)
    gateway.capture_once(
        codes=binding.codes, scheduled_at=AT, universe_as_of=AT, trade_date=AT.date()
    )
    return spool, binding, root


def test_actual_published_gateway_latest_reads_named_batch_not_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spool, binding, root = quote_fixture(tmp_path)
    monkeypatch.setattr(
        spool, "current", lambda *_: (_ for _ in ()).throw(AssertionError("history scan"))
    )
    snapshot = read_latest_price_quote_snapshot(
        spool,
        request_root=root,
        binding=binding,
        evaluated_at=AT,
        expected_producer_commit="b" * 40,
    )
    assert snapshot.quotes[0].price == "10.125"
    assert snapshot.sequence == 0
    assert snapshot.request_binding_sha256 == sha256(binding.wire_bytes()).hexdigest()
    assert snapshot.quotes[0].source_timestamp_provenance == "provider_source_timestamp"
    assert snapshot.quotes[0].observed_at == AT
    with pytest.raises(ValueError):
        read_latest_price_quote_snapshot(
            spool,
            request_root=root,
            binding=binding.model_copy(update={"request_id": "a" * 64}),
            evaluated_at=AT,
            expected_producer_commit="b" * 40,
        )


@pytest.mark.parametrize("seconds,valid", [(15, True), (16, False), (-1, False)])
def test_actual_quote_time_bound(tmp_path: Path, seconds: int, valid: bool) -> None:
    spool, binding, root = quote_fixture(tmp_path)
    args = dict(
        request_root=root,
        binding=binding,
        evaluated_at=AT + timedelta(seconds=seconds),
        expected_producer_commit="b" * 40,
    )
    if valid:
        assert len(read_latest_price_quote_snapshot(spool, **args).quotes) == 1
    else:
        with pytest.raises(ValueError):
            read_latest_price_quote_snapshot(spool, **args)


def test_no_scope_is_unavailable_not_known_zero() -> None:
    assert read_price_alert_scope(None, evaluated_at=AT).availability == "unavailable"


def test_real_serving_four_projection_lease_and_known_zero(tmp_path: Path) -> None:
    from rquant.serving_publisher import ServingReader
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT
    from tests.unit.test_web_price_alert_rules import member, publish, rule

    root = tmp_path / "serving"
    publish(root, rules=(rule(),), members=(member(),))
    with ServingReader(root).acquire_generation() as lease:
        scope = read_price_alert_scope(lease, evaluated_at=FIXTURE_BUILT_AT)
        assert scope.availability == "ready"
        assert scope.codes == ("600001.SH",)
        assert scope.generation_id == lease.manifest.generation_id
        assert (
            read_price_alert_scope(
                lease, evaluated_at=FIXTURE_BUILT_AT + timedelta(seconds=31)
            ).availability
            == "unavailable"
        )
    publish(root, sequence=1, rules=(), members=())
    with ServingReader(root).acquire_generation() as lease:
        scope = read_price_alert_scope(lease, evaluated_at=FIXTURE_BUILT_AT + timedelta(minutes=1))
        assert scope.availability == "ready"
        assert scope.effective_rules == ()


def test_scope_hash_is_not_an_arbitrary_nonzero_claim(tmp_path: Path) -> None:
    from rquant.price_alert_runtime_source import PriceAlertScopeSnapshot
    from rquant.serving_publisher import ServingReader
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT
    from tests.unit.test_web_price_alert_rules import member, publish, rule

    root = tmp_path / "serving"
    publish(root, rules=(rule(),), members=(member(),))
    with ServingReader(root).acquire_generation() as lease:
        scope = read_price_alert_scope(lease, evaluated_at=FIXTURE_BUILT_AT)
        with pytest.raises(ValueError):
            PriceAlertScopeSnapshot.model_validate(
                scope.model_copy(update={"rule_rows_sha256": "a" * 64})
            )
