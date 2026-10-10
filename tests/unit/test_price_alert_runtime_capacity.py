import sqlite3
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from rquant.price_alert_runtime_contracts import parse_price_alert_event
from rquant.price_alert_runtime_source import (
    PriceAlertScopeSnapshot,
    read_latest_price_quote_snapshot,
)
from rquant.price_alert_runtime_store import PriceEvaluationRecord, PriceRoundInput
from rquant.runtime_builder_price_alert import price_alert_runtime_builder
from rquant.runtime_service_builtin import watchlist_quote_source_builder
from rquant.serving_manual_watchlist_projection import ManualWatchlistAuthoritySnapshot
from rquant.serving_price_alert_rule_projection import PriceAlertRuleAuthoritySnapshot
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_price_alert_event_contracts import AT, event
from tests.unit.test_price_alert_runtime_builders import quote_manifest, runtime_fixture
from tests.unit.test_price_alert_runtime_source import quote_fixture
from tests.unit.test_price_alert_runtime_store import round_input, store_fixture
from tests.unit.test_web_price_alert_rules import member, publish, rule


def maximum_scope(at, *, owner_count=10, rule_count=1000, code_count=500):
    rules = tuple(
        sorted(
            (
                rule(
                    f"u{index % owner_count:02d}",
                    rule_id=f"r{index:04d}",
                    updated=at - timedelta(seconds=1),
                ).model_copy(update={"ts_code": f"{600000 + index % code_count:06d}.SH"})
                for index in range(rule_count)
            ),
            key=lambda value: (value.owner_id, value.rule_id),
        )
    )
    identities = sorted({(row.owner_id, row.ts_code) for row in rules})
    members = tuple(
        member(owner).model_copy(update={"ts_code": code, "updated_at": at - timedelta(seconds=1)})
        for owner, code in identities
    )
    return rules, members


def test_actual_maximum_scope_round_overflow_is_recorded_without_partial_commit(
    tmp_path: Path,
) -> None:
    now, unused, manifest, activation, frequency, requests, serving, calendar = runtime_fixture(
        tmp_path
    )
    rules, members = maximum_scope(now)
    with (
        patch("tests.support.web_serving_fixture.FIXTURE_BUILT_AT", now - timedelta(minutes=1)),
        patch("tests.unit.test_web_price_alert_rules.FIXTURE_BUILT_AT", now - timedelta(minutes=1)),
    ):
        publish(serving, sequence=1, rules=rules, members=members)

    def provider(codes, *, timeout_seconds, on_started):
        assert len(codes) == 500
        on_started(now)
        return pd.DataFrame(
            [
                dict(
                    ts_code=code,
                    price=11.0,
                    open=10.0,
                    high=11.0,
                    low=10.0,
                    volume=10.0,
                    amount=100.0,
                    source_observed_at=now,
                )
                for code in codes
            ]
        )

    quote = watchlist_quote_source_builder(
        provider_factory=lambda: provider, universe_loader=None, clock=lambda: now
    )(quote_manifest(tmp_path, calendar))
    quote_result = quote()
    assert quote_result.batch_published is True, quote_result.model_dump(mode="json")
    step = price_alert_runtime_builder(clock=lambda: now, runtime_root=tmp_path)(manifest)
    try:
        result = step()
        snapshot = step.store.runtime_snapshot(observed_at=now)
        assert (
            result.processed_count == 0
            and "price_alert:capacity_exceeded" in result.degraded_reasons
        )
        assert snapshot.round.input_metadata.availability == "unavailable"
        assert snapshot.round.input_metadata.reason == "capacity_exceeded"
        assert snapshot.round.decision_count == 0 and snapshot.rules == ()
        assert (
            snapshot.source.high_watermark == 0
            and step.store.events_after(0, inspected_at=now) == ()
        )
        print(
            {
                "actual_owners": 10,
                "actual_rules": 1000,
                "actual_quote_codes": 500,
                "partial_events": 0,
                "round_availability": "unavailable",
                "ledger_bytes": step.store.path.stat().st_size,
            }
        )
    finally:
        step.close()


@pytest.mark.parametrize(
    "owner_count,rule_count,code_count,valid",
    [
        (32, 32, 1, True),
        (33, 33, 1, False),
        (1, 100, 1, True),
        (1, 101, 1, False),
        (10, 1000, 500, True),
        (11, 1001, 500, False),
        (10, 500, 500, True),
        (10, 501, 501, False),
    ],
)
def test_exact_scope_capacity_and_one_over(
    owner_count: int, rule_count: int, code_count: int, valid: bool
) -> None:
    rules, members = maximum_scope(
        AT, owner_count=owner_count, rule_count=rule_count, code_count=code_count
    )
    body = dict(
        generation_id="1" * 64,
        manifest_sha256="2" * 64,
        source_generation_id="3" * 64,
        source_sequence=1,
        built_at=AT,
        available_at=AT,
        inspected_at=AT,
        rules=rules,
        members=members,
        rule_rows_sha256=PriceAlertRuleAuthoritySnapshot.digest(rules),
        member_rows_sha256=ManualWatchlistAuthoritySnapshot.digest(members),
    )
    if valid:
        scope = PriceAlertScopeSnapshot(**body)
        assert len(scope.effective_rules) == rule_count and len(scope.codes) == code_count
    else:
        with pytest.raises(ValueError, match="exceeds"):
            PriceAlertScopeSnapshot(**body)


def test_actual_event_4k_boundary_and_one_over_before_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = event()
    exponent = 4096 - len(base.wire_bytes())
    at_limit = event(quote_sequence=10**exponent)
    assert (
        len(at_limit.wire_bytes()) == 4096
        and parse_price_alert_event(at_limit.wire_bytes()) == at_limit
    )
    with pytest.raises(ValueError, match="4 KiB"):
        event(quote_sequence=10 ** (exponent + 1))
    monkeypatch.setattr(
        "rquant.price_alert_runtime_contracts.strict_canonical_json_loads",
        lambda *_: (_ for _ in ()).throw(AssertionError("oversized data reached the JSON parser")),
    )
    with pytest.raises(ValueError, match="4 KiB"):
        parse_price_alert_event(at_limit.wire_bytes() + b" ")


def exact_round_payload(size: int) -> bytes:
    records = [
        PriceEvaluationRecord(
            owner_id=f"u{owner:02d}",
            rule_id=f"r{index:03d}",
            rule_version=1,
            membership_version=1,
            ts_code="600000.SH",
            state="unavailable",
            reason="a",
        ).model_dump(mode="json")
        for owner in range(32)
        for index in range(100)
    ]
    body = PriceRoundInput.model_construct(
        evaluated_at=AT,
        scope_generation_id="1" * 64,
        scope_manifest_sha256="2" * 64,
        scope_source_generation_id="3" * 64,
        scope_source_sequence=1,
        requested_codes=1,
        valid_quotes=0,
        records=(),
    ).model_dump(mode="json")
    body["records"] = records
    remaining = size - len(canonical_json_bytes(body))
    assert remaining > 0
    for record in records:
        growth = min(449, remaining)
        length = growth + 1
        record["reason"] = "\0" * (length // 6) + "a" * (length % 6)
        assert 1 <= len(record["reason"]) <= 80
        remaining -= growth
        if not remaining:
            break
    payload = canonical_json_bytes(body)
    assert remaining == 0 and len(payload) == size
    return payload


def test_actual_round_1m_boundary_and_one_over() -> None:
    at_limit = exact_round_payload(1024 * 1024)
    value = PriceRoundInput.model_validate_json(at_limit)
    assert len(value.records) == 3200 and len(value.wire_bytes()) == 1024 * 1024
    with pytest.raises(ValueError, match="budget"):
        PriceRoundInput.model_validate_json(exact_round_payload(1024 * 1024 + 1))


def replace_named_quote(spool, payload: bytes) -> None:
    from rquant.live_contracts import BatchEnvelope, CurrentPointer, LiveChannel

    channel = LiveChannel.WATCHLIST_QUOTE
    pointer_path = spool._current_path(channel)
    pointer = CurrentPointer.model_validate_json(pointer_path.read_bytes())
    envelope_path = spool._manifest_path(channel, pointer.sequence)
    envelope = BatchEnvelope.model_validate_json(envelope_path.read_bytes())
    digest = sha256(payload).hexdigest()
    # Only this owned synthetic fixture is resealed; immutable product writers are unchanged.
    spool._payload_path(channel, pointer.sequence).write_bytes(payload)
    envelope_path.write_text(
        envelope.model_copy(update={"content_sha256": digest}).model_dump_json()
    )
    pointer_path.write_text(pointer.model_copy(update={"content_sha256": digest}).model_dump_json())


@pytest.mark.parametrize("extra", [0, 1])
def test_actual_quote_file_4m_boundary_and_one_over(tmp_path: Path, extra: int) -> None:
    from rquant.live_contracts import LiveChannel

    spool, binding, requests = quote_fixture(tmp_path)
    original = spool._payload_path(LiveChannel.WATCHLIST_QUOTE, 0).read_bytes()
    footer_size = int.from_bytes(original[-8:-4], "little")
    offset = len(original) - 8 - footer_size
    payload = (
        original[:offset] + b"\0" * (4 * 1024 * 1024 + extra - len(original)) + original[offset:]
    )
    assert len(payload) == 4 * 1024 * 1024 + extra
    replace_named_quote(spool, payload)
    kwargs = dict(
        request_root=requests, binding=binding, evaluated_at=AT, expected_producer_commit="b" * 40
    )
    if extra:
        from rquant.live_spool import LiveSpoolIntegrityError

        with pytest.raises(LiveSpoolIntegrityError):
            read_latest_price_quote_snapshot(spool, **kwargs)
    else:
        assert read_latest_price_quote_snapshot(spool, **kwargs).quotes[0].price == "10.125"


@pytest.mark.parametrize("extra", [0, 1])
def test_actual_quote_decoded_4m_boundary_before_dataframe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: int
) -> None:
    from io import BytesIO

    import pyarrow as pa
    import pyarrow.parquet as pq

    from rquant.live_contracts import LiveChannel

    spool, binding, requests = quote_fixture(tmp_path)
    frame = pq.read_table(
        BytesIO(spool._payload_path(LiveChannel.WATCHLIST_QUOTE, 0).read_bytes())
    ).to_pandas()
    target = 4 * 1024 * 1024 + extra
    padding = target
    for _ in range(3):
        frame["source"] = "a" * padding
        stream = BytesIO()
        pq.write_table(
            pa.Table.from_pandas(frame, preserve_index=False),
            stream,
            compression="zstd",
            use_dictionary=False,
            write_statistics=False,
        )
        payload = stream.getvalue()
        metadata = pq.ParquetFile(BytesIO(payload)).metadata
        decoded = sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
        if decoded == target:
            break
        padding += target - decoded
    assert decoded == target and len(payload) < 4 * 1024 * 1024
    replace_named_quote(spool, payload)
    reached = []
    original_decoder = __import__(
        "rquant.watchlist_quote_gateway", fromlist=["decode_watchlist_quote_payload"]
    ).decode_watchlist_quote_payload

    def decode(value):
        reached.append(True)
        return original_decoder(value)

    monkeypatch.setattr("rquant.price_alert_runtime_source.decode_watchlist_quote_payload", decode)
    with pytest.raises(ValueError):
        read_latest_price_quote_snapshot(
            spool,
            request_root=requests,
            binding=binding,
            evaluated_at=AT,
            expected_producer_commit="b" * 40,
        )
    # The exact budget reaches decoding, then rejects the deliberately incorrect source.
    assert reached == ([] if extra else [True])


@pytest.mark.parametrize("extra", [0, 1])
def test_actual_ledger_512m_limit_preserves_prior_bytes(tmp_path: Path, extra: int) -> None:
    store, activation, policy = store_fixture(tmp_path)
    try:
        original = store.commit_round(round_input(activation, policy), policy=policy).events[0]
        with store.path.open("r+b") as stream:
            stream.truncate(512 * 1024 * 1024 + extra)
        with pytest.raises(ValueError, match="capacity"):
            store.commit_round(
                round_input(activation, policy, at=AT + timedelta(seconds=60)), policy=policy
            )
        assert store.source_descriptor().high_watermark == 1
        assert store.events_after(0, inspected_at=AT + timedelta(seconds=60)) == (original,)
        assert store.path.stat().st_size == 512 * 1024 * 1024 + extra
        with sqlite3.connect(store.path) as connection:
            assert (
                connection.execute("SELECT COUNT(*) FROM price_alert_round_receipt").fetchone()[0]
                == 1
            )
    finally:
        store.close()


def test_actual_100000th_event_and_one_over_keep_all_history(tmp_path: Path) -> None:
    from rquant.price_alert_runtime_contracts import require_price_alert_activation

    store, activation, policy = store_fixture(tmp_path)
    binding = require_price_alert_activation(activation, "evaluation")
    try:
        # A bounded, synthetic sealed archive exercises the row limit. It is not market evidence.
        def archive_rows():
            for sequence in range(1, 100000):
                at = AT - timedelta(days=2000 - sequence // 60, minutes=60 - sequence % 60)
                item = event(
                    rule_id=f"history/{sequence:06d}",
                    trade_date=at.date(),
                    quote_observed_at=at,
                    quote_available_at=at,
                    evaluated_at=at,
                    available_at=at,
                    expires_at=at + timedelta(seconds=120),
                    frequency_policy_sha256=policy.sha256,
                    producer_manifest_sha256=binding.producer_manifest_sha256,
                    producer_commit=binding.producer_commit,
                    source_epoch=binding.source_epoch,
                )
                encoded = item.wire_bytes()
                yield (
                    sequence,
                    item.event_id,
                    item.owner_id,
                    item.rule_id,
                    item.membership_version,
                    policy.sha256,
                    item.observation_key,
                    encoded,
                    sha256(encoded).hexdigest(),
                )

        with store._connection(write=True) as connection:
            connection.executemany(
                "INSERT INTO price_alert_event_log VALUES(?,?,?,?,?,?,?,?,?)", archive_rows()
            )
            connection.execute(
                "UPDATE price_alert_runtime_identity SET high_watermark=99999 WHERE key='current'"
            )
        receipt = store.commit_round(round_input(activation, policy), policy=policy)
        assert len(receipt.events) == 1 and receipt.source_high_watermark == 100000
        before = store.path.stat().st_size
        with pytest.raises(ValueError, match="100000"):
            store.commit_round(
                round_input(activation, policy, at=AT + timedelta(seconds=60)), policy=policy
            )
        with sqlite3.connect(store.path) as connection:
            assert connection.execute(
                "SELECT COUNT(*),MAX(sequence) FROM price_alert_event_log"
            ).fetchone() == (100000, 100000)
            assert (
                connection.execute("SELECT COUNT(*) FROM price_alert_round_receipt").fetchone()[0]
                == 1
            )
            assert (
                connection.execute("SELECT COUNT(*) FROM price_alert_frequency_state").fetchone()[0]
                == 1
            )
        assert store.events_after(99999, inspected_at=AT + timedelta(seconds=60)) == receipt.events
        assert store.path.stat().st_size == before < 512 * 1024 * 1024
        print(
            {
                "actual_immutable_event_rows": 100000,
                "actual_ledger_bytes": before,
                "one_over": "rollback",
                "history_deleted": 0,
            }
        )
    finally:
        store.close()


def exact_projection_inputs(size: int):
    from rquant.price_alert_route import PriceAlertBusRoutedRecord, PriceAlertRouteReceipt
    from rquant.price_alert_runtime_contracts import PriceAlertSourceDescriptor
    from rquant.price_alert_runtime_projection import (
        PRICE_RUNTIME_TABLES,
        PriceAlertRuntimeState,
        _rows_sha,
    )
    from rquant.serving_read_models import (
        ServingProjectionInput,
        ServingProjectionPayload,
        _projection_json_bytes,
    )

    count = 200
    source = PriceAlertSourceDescriptor(
        source_id="synthetic-price",
        ledger_id="1" * 64,
        source_epoch="d" * 64,
        generation_id="2" * 64,
        producer_manifest_sha256="b" * 64,
        evaluation_contract_sha256="3" * 64,
        frequency_policy_sha256="3" * 64,
        routing_policy_sha256="4" * 64,
        first_sequence=1,
        high_watermark=count,
    )

    def event_row(index: int, digits: int = 0):
        item = event(
            owner_id=f"u{index % 32:02d}", rule_id=f"r{index:04d}", quote_sequence=10**digits
        )
        payload = item.wire_bytes()
        receipt = PriceAlertRouteReceipt.create(
            source_id=source.source_id,
            source_sequence=index + 1,
            event_id=item.event_id,
            owner_id=item.owner_id,
            rule_id=item.rule_id,
            rule_version=item.rule_version,
            membership_version=item.membership_version,
            disposition="no_target",
            reason_code="no_owner_target",
            routing_policy_sha256=source.routing_policy_sha256,
            targets=(),
            target_count=0,
            target_manifest_hash=sha256(canonical_json_bytes([])).hexdigest(),
            source_inspected_at=AT,
            routed_at=AT,
        )
        record = PriceAlertBusRoutedRecord(
            global_sequence=index + 1,
            event_id=item.event_id,
            payload_hash=sha256(payload).hexdigest(),
            payload_json=payload.decode(),
            event=item,
            received_at=AT,
            bus_generation_id="5" * 64,
            source=source,
            source_sequence=index + 1,
            receipt=receipt,
        )
        return {
            "owner_id": item.owner_id,
            "event_id": item.event_id,
            "global_sequence": index + 1,
            "body_json": record.wire_bytes().decode(),
        }, 4096 - len(payload)

    rows = [event_row(index)[0] for index in range(count)]

    def projections(reason="not_started"):
        state = PriceAlertRuntimeState(
            availability="not_running",
            reason=reason,
            producer=None,
            authority=None,
            notifier_as_of=AT,
            shadow=False,
            rule_count=0,
            event_count=count,
            attempt_count=0,
            runtime_rows_sha256=_rows_sha(()),
            event_rows_sha256=_rows_sha(rows),
            attempt_rows_sha256=_rows_sha(()),
        )
        return tuple(
            ServingProjectionInput.bind(
                ServingProjectionPayload(table_name=name, available_at=AT, rows=tuple(content)),
                owner_dataset_id="signals",
                owner_generation_id="6" * 64,
            )
            for name, content in zip(
                PRICE_RUNTIME_TABLES,
                (
                    [{"snapshot_key": "current", "body_json": state.wire_bytes().decode()}],
                    [],
                    sorted(rows, key=lambda row: (row["owner_id"], row["event_id"])),
                    [],
                ),
                strict=True,
            )
        )

    base = projections()
    missing = size - sum(_projection_json_bytes(value) for value in base)
    assert missing >= 0
    for index in range(count):
        _, room = event_row(index)
        growth = min(room, missing // 2)
        rows[index], _ = event_row(index, growth)
        missing -= growth * 2
        if missing < 2:
            break
    assert missing < 2
    result = projections("not_started" + "x" * missing)
    assert sum(_projection_json_bytes(value) for value in result) == size
    return result


def test_actual_price_projection_2m_boundary_and_one_over() -> None:
    from rquant.serving_read_models import ServingReadModelInput

    at_limit = exact_projection_inputs(2 * 1024 * 1024)
    assert len(ServingReadModelInput(observed_at=AT, projections=at_limit).projections) == 4
    with pytest.raises(ValueError, match="2 MiB"):
        ServingReadModelInput(
            observed_at=AT, projections=exact_projection_inputs(2 * 1024 * 1024 + 1)
        )


def test_private_history_projection_budget_includes_actual_owner_binding(tmp_path: Path) -> None:
    from rquant.notification_state import NotificationStateStore
    from rquant.price_alert_route import PriceAlertBusRoutedRecord, _price_ingest
    from rquant.price_alert_runtime_projection import price_runtime_projections
    from rquant.serving_read_models import ServingProjectionInput, _projection_json_bytes
    from tests.unit.test_price_alert_notification_admission import notifier_activation
    from tests.unit.test_price_alert_route import route_fixture

    producer, unused, router, policy, source, record = route_fixture(tmp_path)
    producer.close()
    cap, digest = notifier_activation(tmp_path, policy)
    state = NotificationStateStore(tmp_path / "capacity-notify.sqlite3")
    state.install_price_alert_delivery_v1(cap)
    inputs = exact_projection_inputs(2 * 1024 * 1024 + 1)
    archived = tuple(
        PriceAlertBusRoutedRecord.model_validate_json(row["body_json"]) for row in inputs[2].rows
    )
    # Typed, synthetic sealed history exercises the actual private read adapter.
    # It is not a claim about a maximum real producer round or market inputs.
    with state._write_transaction() as connection:
        for item in sorted(archived, key=lambda value: value.global_sequence):
            sequence, inserted = _price_ingest(connection, item.event, item.received_at)
            assert inserted and sequence == item.global_sequence
            connection.execute(
                "INSERT INTO price_alert_route_receipt VALUES(?,?,?,?,?,?)",
                (
                    item.event_id,
                    item.source.source_id,
                    item.source_sequence,
                    item.source.wire_bytes(),
                    item.receipt.wire_bytes(),
                    item.bus_generation_id,
                ),
            )
    with state._read_snapshot() as connection:
        try:
            payloads = price_runtime_projections(
                connection, producer=None, observed_at=AT, shadow=False
            )
        except ValueError as error:
            assert "2 MiB" in str(error)
        else:
            bound = tuple(
                ServingProjectionInput.bind(
                    value, owner_dataset_id="signals", owner_generation_id="6" * 64
                )
                for value in payloads
            )
            actual = sum(_projection_json_bytes(value) for value in bound)
            print(
                {
                    "complete_bound_projection_bytes": actual,
                    "limit": 2 * 1024 * 1024,
                    "unbound_projection_bytes": sum(
                        _projection_json_bytes(value) for value in payloads
                    ),
                }
            )
            pytest.fail(
                f"price domain returned {actual} bound bytes "
                "instead of rejecting only the new domain"
            )
