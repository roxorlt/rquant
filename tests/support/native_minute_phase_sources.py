"""Explicit modeled sources for the original native minute phase producer.

These are new synthetic research originals. They are never historical captures,
an official SSE calendar, a sealed result, or post-approval live observations.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration
from rquant.experiment_platform import NativeMinutePhaseRead
from rquant.experiment_registry import DateRange
from rquant.intraday_feature_engine import IntradayFeatureConfig
from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    LiveChannel,
    LiveSourceDescriptor,
)
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.minute_backtest_contracts import (
    MinuteReplayExecutionProfile,
    MinuteReplayMaterial,
    MinuteReplayWork,
)
from rquant.minute_backtest_definition import bootstrap_minute_definition
from rquant.minute_backtest_producer import measure_minute_formal_work
from rquant.minute_backtest_publication_contracts import (
    MinuteCaptureLineage,
    MinuteCodeFile,
    MinuteDerivation,
    MinuteOriginMaterial,
    MinuteProvenance,
    MinutePublicationEvidence,
    MinuteRuntimeContent,
    MinuteSourceContentSeed,
    MinuteVisibilityPolicy,
)
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintBatch,
    PaperExecutionConstraintPointer,
    PaperExecutionConstraintPublisher,
    PaperExecutionConstraintSnapshot,
)
from rquant.research_run_spec import InstrumentClassificationProvenance, InstrumentContext
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.runtime_definition_bootstrap import (
    BuiltinDefinitionStrategyBinding,
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_routing_policy import RoutingPolicyDocument
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotSpool,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

NativeMinuteStrategyId = Literal["n_shape", "auction_gap", "growth_board_surge"]
ORIGINAL_FIXTURE_SHA256 = "d670d16f860f0f11247c64ecf0985fc45316bb4e7dcaa6cb6f1180c618a958b8"
_REPOSITORY = Path(__file__).resolve().parents[2]
_ORIGINAL_FIXTURE = _REPOSITORY / (
    "data/verification/minute-engine-completion-20261007/core-implementation-01/behavior/"
    "daily-n_shape-bar_end.json"
)
_BAR_MINUTES = (30, 31, 32, 33, 359, 360)
_DEPENDENCY_FILES = (
    "src/rquant/definition_registry.py",
    "src/rquant/executable_dependencies.py",
    "src/rquant/intraday_feature_engine.py",
    "src/rquant/live_contracts.py",
    "src/rquant/live_spool.py",
    "src/rquant/market_minute_gateway.py",
    "src/rquant/minute_backtest_contracts.py",
    "src/rquant/minute_backtest_definition.py",
    "src/rquant/minute_backtest_producer.py",
    "src/rquant/minute_backtest_publication_contracts.py",
    "src/rquant/minute_backtest_runner.py",
    "src/rquant/minute_backtest_source.py",
    "src/rquant/minute_backtest_validation.py",
    "src/rquant/paper_execution_constraints.py",
    "src/rquant/runtime_definition_bootstrap.py",
    "src/rquant/strategy_candidate_snapshot.py",
    "src/rquant/strategy_evaluators.py",
    "tests/support/native_minute_phase_sources.py",
)

NATIVE_PHASE_VISIBILITY_POLICY = MinuteVisibilityPolicy(
    policy_id="synthetic-native-minute-phase-bar-end",
    version=1,
    timestamp_semantics="bar_end",
    market_event_basis=(
        "New deterministic synthetic whole-minute OHLCV rows; timestamps are modeled bar ends."
    ),
    market_visibility_basis=(
        "Opening bars have modeled five-second publication delay; 14:59 and 15:00 bars are "
        "modeled visible at their ends. No real completion time is claimed."
    ),
    candidate_visibility_basis=(
        "New synthetic prior-session references and candidate publications are modeled visible "
        "at 09:25 Asia/Shanghai each selected session."
    ),
    constraint_visibility_basis=(
        "New synthetic listing/classification and execution constraints are modeled valid from "
        "09:25 through 15:01 Asia/Shanghai."
    ),
    native_definition_basis=(
        "Current original native bootstrap and executable fingerprints; pre-observation private "
        "replay availability is a research assumption."
    ),
    limitations=(
        "Synthetic finite behavior and capacity fixture only. The supplied calendar is a modeled "
        "open-date set, not an official SSE holiday calendar. No historical capture, real market "
        "return, sealed result, promotion approval, or 20 real forward sessions is claimed. Prices "
        "and volumes use a fixed three-session scenario; every feature, signal, fill, fee and NAV "
        "must be computed by the original engine."
    ),
)


class _PhysicalMaterial(RuntimeContractModel):
    relative_path: str
    content_sha256: str
    size_bytes: int


class NativePhaseSourceManifest(RuntimeContractModel):
    contract: Literal["synthetic-native-minute-phase-source/v1"] = (
        "synthetic-native-minute-phase-source/v1"
    )
    synthetic: Literal[True] = True
    historical_capture: Literal[False] = False
    official_trade_calendar: Literal[False] = False
    original_fixture_sha256: str
    python_version: str
    read: NativeMinutePhaseRead | None
    seed_hash: str
    calendar_sha256: str
    profile_hash: str
    native_registration_fingerprint: str
    native_executable_fingerprint: str
    modeled_replay_available_at: datetime
    actual_research_published_at: datetime
    warmup_dates: tuple[date, ...]
    replay_dates: tuple[date, ...]
    materials: tuple[_PhysicalMaterial, ...]
    origins: tuple[_PhysicalMaterial, ...]
    source_code: tuple[MinuteCodeFile, ...]
    limitations: str


def _at(day: date, minute: int) -> datetime:
    return datetime.combine(day, time(1), UTC) + timedelta(minutes=minute)


def _write_new(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("xb") as stream:
        os.chmod(path, 0o600)
        stream.write(payload)


def _new_root(root: Path) -> None:
    if not root.is_absolute() or root != Path(os.path.abspath(root)) or root.parent.is_symlink():
        raise ValueError("synthetic source requires a new normalized absolute directory")
    root.mkdir(mode=0o700)


def _blueprint() -> tuple[MinuteReplayExecutionProfile, IntradayFeatureConfig]:
    payload = _ORIGINAL_FIXTURE.read_bytes()
    if hashlib.sha256(payload).hexdigest() != ORIGINAL_FIXTURE_SHA256:
        raise PermissionError("original fixture shape reference changed")
    # The old complete Frozen input is deliberately not admitted under a new graph.
    original = json.loads(payload)["frozen_input"]
    return (
        MinuteReplayExecutionProfile.model_validate_json(json.dumps(original["execution_profile"])),
        IntradayFeatureConfig.model_validate_json(json.dumps(original["feature_config"])),
    )


def _dates(
    calendar: MarketCalendarAuthority, window: DateRange, published_at: datetime, lookback: int
) -> tuple[tuple[date, ...], tuple[date, ...]]:
    if not calendar.coverage_start <= window.start_date <= window.end_date <= calendar.coverage_end:
        raise ValueError("synthetic phase window lacks calendar coverage")
    if window.start_date not in calendar.open_dates or window.end_date not in calendar.open_dates:
        raise ValueError("synthetic phase boundaries are not supplied modeled open dates")
    index = calendar.open_dates.index(window.start_date)
    if index < lookback or not any(day > window.end_date for day in calendar.open_dates):
        raise ValueError("synthetic phase calendar lacks warmup or the next acquisition session")
    warmup = calendar.open_dates[index - lookback : index]
    replay = tuple(
        day for day in calendar.open_dates if window.start_date <= day <= window.end_date
    )
    available = datetime.combine(window.start_date, time(), UTC)
    if calendar.generated_at > available or _at(replay[-1], 360) > published_at:
        raise ValueError("synthetic calendar or publication clock is from the future")
    return warmup, replay


def _codes(native_id: str) -> tuple[str, ...]:
    return (
        ("300001.SZ", "300002.SZ", "300003.SZ")
        if native_id == "growth_board_surge"
        else ("600000.SH", "600001.SH", "600002.SH")
    )


def _scenario(day_index: int, code_index: int) -> tuple[tuple[float, ...], tuple[float, ...]]:
    phase = (day_index - code_index) % 3
    if phase == 0:
        return (9.9, 10.1, 10.12, 10.14, 10.18, 10.18), (
            150.0,
            1500.0,
            1500.0,
            1500.0,
            100.0,
            100.0,
        )
    if phase == 1:
        return (9.4, 9.4, 9.42, 9.43, 9.45, 9.45), (100.0,) * 6
    return (9.29, 9.29, 9.3, 9.3, 9.3, 9.3), (100.0,) * 6


def _rows(
    day: date, day_index: int, codes: tuple[str, ...], *, warmup: bool
) -> list[dict[str, object]]:
    rows = []
    for bar_index, minute in enumerate(_BAR_MINUTES):
        stamp = _at(day, minute)
        available = stamp if minute >= 359 else stamp + timedelta(seconds=5)
        for code_index, code in enumerate(codes):
            prices, volumes = _scenario(day_index, code_index)
            price, volume = prices[bar_index], volumes[bar_index]
            row = {
                "ts_code": code,
                "trade_time": stamp,
                "open": price,
                "high": price + 0.01,
                "low": price - 0.01,
                "close": price,
                "vol": volume,
                "amount": volume * price,
            }
            if warmup:
                row |= {"trade_date": day, "available_at": available}
            rows.append(row)
    return rows


def _parquet(rows: list[dict[str, object]]) -> bytes:
    frame = pd.DataFrame(rows)
    table = pa.Table.from_pandas(frame, preserve_index=False).replace_schema_metadata(None)
    output = BytesIO()
    parquet.write_table(
        table,
        output,
        compression="zstd",
        use_dictionary=False,
        store_schema=False,
        write_statistics=False,
    )
    return output.getvalue()


def _static(native_id: str, day_index: int, code_index: int) -> dict[str, object]:
    previous_prices, _ = _scenario(day_index - 1, code_index)
    prices, _ = _scenario(day_index, code_index)
    close, high = previous_prices[-1], max(previous_prices) + 0.01
    limit_pct = 20.0 if native_id == "growth_board_surge" else 10.0
    limit_up = round(close * (1 + limit_pct / 100), 2)
    values: dict[str, object] = {
        "candidate_price_basis": "raw_session",
        "limit_up_price_session_raw": limit_up,
    }
    if native_id == "n_shape":
        return values | {
            "limit_pct": limit_pct,
            "t_close_session_raw": close,
            "t_high_session_raw": high,
        }
    if native_id == "growth_board_surge":
        return values | {
            "board_type": "gem",
            "ma_alignment": (day_index - code_index) % 3 == 0,
            "large_net_vol_t1": 1.0,
            "session_pre_close_raw": close,
        }
    auction_price = prices[0]
    return values | {
        "auction_price_raw": auction_price,
        "auction_vol_ratio_5d": 1.0,
        "gap_pct_close": (auction_price / close - 1) * 100,
    }


def _origin(key: str, payload: bytes, format: Literal["parquet", "json"]) -> MinuteOriginMaterial:
    return MinuteOriginMaterial(
        object_key=key,
        content_base64=base64.b64encode(payload).decode("ascii"),
        content_sha256=hashlib.sha256(payload).hexdigest(),
        format=format,
    )


def _build_source(
    root: Path,
    *,
    native: StrategySpecRegistration,
    wrapper: StrategySpecRegistration,
    binding: BuiltinDefinitionStrategyBinding,
    profile: MinuteReplayExecutionProfile,
    features: IntradayFeatureConfig,
    calendar: MarketCalendarAuthority,
    window: DateRange,
    source_key: str,
    source_version: int,
    owner_id: str,
    published_at: datetime,
    read: NativeMinutePhaseRead | None,
    replay_available_at: datetime | None = None,
) -> MinuteSourceContentSeed:
    warmup_dates, replay_dates = _dates(calendar, window, published_at, features.lookback_sessions)
    source = root / "source"
    source.mkdir(mode=0o700)
    codes = _codes(native.logical_id)
    day_indexes = {day: index for index, day in enumerate(calendar.open_dates)}
    calendar_bytes = json.dumps(
        {
            "synthetic": True,
            "official_sse_calendar": False,
            "coverage_start": calendar.coverage_start.isoformat(),
            "coverage_end": calendar.coverage_end.isoformat(),
            "rows": [
                {"exchange": "SSE", "cal_date": day.isoformat(), "is_open": True}
                for day in calendar.open_dates
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    _write_new(source / "calendar.json", calendar_bytes)
    routing = (
        RoutingPolicyDocument(default_no_target_reason="offline-paper", rules=())
        .model_dump_json()
        .encode()
    )
    if hashlib.sha256(routing).hexdigest() != profile.routing_policy_fingerprint:
        raise PermissionError("synthetic source routing differs from the frozen full profile")
    _write_new(source / "routing-policy.json", routing)
    history = [
        row for day in warmup_dates for row in _rows(day, day_indexes[day], codes, warmup=True)
    ]
    _write_new(source / "warmup.parquet", _parquet(history))
    listing_rows = [
        {
            "ts_code": code,
            "exchange": "SZSE" if code.endswith(".SZ") else "SSE",
            "market": "CN",
            "instrument_class": "EQUITY",
            "security_class": "A_SHARE",
            "synthetic": True,
        }
        for code in codes
    ]
    listing_bytes = json.dumps(
        {"synthetic": True, "rows": listing_rows}, sort_keys=True, separators=(",", ":")
    ).encode()
    listing_sha = hashlib.sha256(listing_bytes).hexdigest()
    references = {}
    for day in replay_dates:
        reference_rows = [
            {
                "ts_code": code,
                "effective_trade_date": day.isoformat(),
                "reference_trade_date": calendar.open_dates[day_indexes[day] - 1].isoformat(),
                "modeled_available_at": _at(day, 25).isoformat(),
                "synthetic": True,
                "static_features": _static(native.logical_id, day_indexes[day], code_index),
            }
            for code_index, code in enumerate(codes)
        ]
        reference_bytes = json.dumps(
            {
                "synthetic": True,
                "rows": reference_rows,
                "prior_session_ma_alignment": (
                    "explicit synthetic input assumption, not an actual moving-average audit"
                ),
                "auction_volume_ratio": (
                    "explicit synthetic five-session ratio input, not observed auction volume"
                ),
                "order_flow": "modeled t-minus-one daily proxy, not a tick or Level2 observation",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        references[day] = _origin(
            f"synthetic-static-references:{day.isoformat()}", reference_bytes, "json"
        )
    definition = BuiltinStrategyEvaluatorRegistry(
        producer_commit=calendar.producer_commit
    ).load_definition(native.logical_id, 1)
    candidates = StrategyCandidateSnapshotSpool(source / "candidates")
    for day in replay_dates:
        captured = _at(day, 25)
        snapshots = {
            "daily_state": references[day].content_sha256,
            "trade_calendar": calendar.content_sha256,
        }
        records = tuple(
            StrategyCandidateRecord(
                strategy_id=native.logical_id,
                strategy_version="1",
                candidate_id=code,
                variant="synthetic-three-session-scenario",
                decision_at=captured,
                available_at=captured,
                effective_trade_date=day,
                reference_trade_date=calendar.open_dates[day_indexes[day] - 1],
                price_basis=StrategyCandidatePriceBasis.RAW,
                static_features=_static(native.logical_id, day_indexes[day], code_index),
                reference_snapshot_ids=snapshots,
            )
            for code_index, code in enumerate(codes)
        )
        candidates.publish_strategy_records(
            strategy_id=native.logical_id,
            strategy_version="1",
            definition_fingerprint=binding.registration_fingerprint,
            executable_fingerprint=binding.executable_fingerprint,
            candidate_schema_fingerprint=binding.candidate_schema_fingerprint,
            static_feature_schema={
                name: {"dtype": item.dtype, "semantic": item.semantic}
                for name, item in definition.static_feature_schema.items()
            },
            source_snapshot_ids=snapshots,
            trade_date=day,
            captured_at=captured,
            producer_commit=calendar.producer_commit,
            rows=records,
        )
    for sequence, day in enumerate(replay_dates):
        constraints = []
        for code, listing in zip(codes, listing_rows, strict=True):
            context = InstrumentContext(
                ts_code=code,
                market="CN",
                exchange=listing["exchange"],
                instrument_class="EQUITY",
                security_class="A_SHARE",
                classification_provenance=InstrumentClassificationProvenance(
                    reference_dataset="security_listing_status",
                    reference_record_id=canonical_sha256(listing),
                    reference_generation_id=listing_sha,
                ),
            )
            fields = dict(
                ts_code=code,
                trade_date=day,
                available_at=_at(day, 25),
                expires_at=_at(day, 361),
                suspended=False,
                buy_limit_locked=False,
                sell_limit_locked=False,
                risk_rejected=False,
                instrument_context=context,
                source_snapshot_ids={"security_listing_status": listing_sha},
                producer_commit=calendar.producer_commit,
            )
            constraints.append(
                PaperExecutionConstraintSnapshot(**fields, content_hash=canonical_sha256(fields))
            )
        fields = dict(
            schema_version=1,
            sequence=sequence,
            producer_commit=calendar.producer_commit,
            records=tuple(constraints),
        )
        PaperExecutionConstraintPublisher(
            root=source / "constraints",
            producer_commit=calendar.producer_commit,
            clock=lambda day=day: _at(day, 25),
        ).publish(PaperExecutionConstraintBatch(**fields, content_hash=canonical_sha256(fields)))
        _write_new(
            source / "constraint-publications" / f"{sequence}.json",
            (source / "constraints/current.json").read_bytes(),
        )
    market = LiveBatchSpool(source / "market")
    identity = LiveSourceDescriptor(
        channel=LiveChannel.MARKET_MINUTE,
        generation_id=canonical_sha256(
            {
                "synthetic": True,
                "source_key": source_key,
                "calendar": calendar.content_sha256,
                "window": window,
                "native": native.fingerprint,
            }
        ),
        high_watermark=-1,
    )
    _write_new(source / "market/sources/market_minute.json", identity.model_dump_json().encode())
    ticks = []
    original_market_rows = []
    sequence = 0
    for day in replay_dates:
        rows = _rows(day, day_indexes[day], codes, warmup=False)
        for minute in _BAR_MINUTES:
            stamp = _at(day, minute)
            visible = stamp if minute >= 359 else stamp + timedelta(seconds=5)
            selected = [row for row in rows if row["trade_time"] == stamp]
            frame = MarketMinuteGateway.normalize_frame(pd.DataFrame(selected))
            normalized_rows = frame.to_dict(orient="records")
            original_market_rows.extend(normalized_rows)
            payload = _parquet(normalized_rows)
            market.publish(
                BatchEnvelope(
                    schema_version=1,
                    channel=LiveChannel.MARKET_MINUTE,
                    dataset_id="market_minute",
                    source="synthetic-native-minute-phase",
                    source_request_id=f"synthetic-minute-{sequence}",
                    batch_id=f"synthetic-minute-{sequence}",
                    sequence=sequence,
                    revision=1,
                    event_time_start=stamp,
                    event_time_end=stamp,
                    source_time=stamp,
                    received_at=visible,
                    available_at=visible,
                    row_count=len(codes),
                    content_sha256=hashlib.sha256(payload).hexdigest(),
                    quality_status=BatchQualityStatus.PUBLISHED,
                    producer_version="synthetic-1",
                    producer_commit=calendar.producer_commit,
                ),
                payload,
            )
            ticks.append(visible)
            sequence += 1
    materials = []
    for path in sorted(source.rglob("*")):
        if not path.is_file() or any(
            part.startswith(".") for part in path.relative_to(source).parts
        ):
            continue
        payload = path.read_bytes()
        materials.append(
            MinuteReplayMaterial(
                relative_path=path.relative_to(source).as_posix(),
                content_base64=base64.b64encode(payload).decode("ascii"),
                content_sha256=hashlib.sha256(payload).hexdigest(),
            )
        )
    policy = NATIVE_PHASE_VISIBILITY_POLICY
    original_manifests = [
        json.loads(item.payload())
        for item in materials
        if item.relative_path.startswith("market/batches/") and item.relative_path.endswith(".json")
    ]
    origins_list = [
        _origin("synthetic-market-bars", _parquet(original_market_rows), "parquet"),
        _origin(
            "synthetic-market-manifests",
            json.dumps(
                {"synthetic": True, "rows": original_manifests},
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            "json",
        ),
    ]
    derivations_list = []
    proof_origins = {}
    factory_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    for index, item in enumerate(materials):
        name = item.relative_path
        if name.startswith("market/batches/"):
            if name.endswith(".payload"):
                parents = ("synthetic-market-bars", "synthetic-market-manifests")
                operation = "exact-batch-row-partition-and-parquet-encoding"
            else:
                parents = ("synthetic-market-manifests",)
                operation = "exact-batch-envelope-json-encoding"
            method = "research_derivative"
            fingerprint = canonical_sha256(
                {
                    "factory_sha256": factory_sha,
                    "operation": operation,
                    "material_path": name,
                    "output_sha256": item.content_sha256,
                }
            )
            proof_origins[name] = "synthetic-market-manifests"
        else:
            origin = _origin(
                f"synthetic-original:{index}",
                item.payload(),
                "parquet" if name.endswith(".parquet") else "json",
            )
            origins_list.append(origin)
            parents = (origin.object_key,)
            method, fingerprint = "retained_research_archive", policy.fingerprint
            proof_origins[name] = origin.object_key
        derivations_list.append(
            MinuteDerivation(
                material_path=name,
                origin_object_keys=parents,
                method=method,
                transformation_fingerprint=fingerprint,
                time_basis="modeled",
            )
        )
    origins_list.extend(references.values())
    origins_list.append(_origin("synthetic-security-listings", listing_bytes, "json"))
    origins = tuple(origins_list)
    derivations = tuple(derivations_list)
    proofs = []
    for item in materials:
        name = item.relative_path
        if name.startswith("market/batches/") and name.endswith(".json"):
            envelope = BatchEnvelope.model_validate_json(item.payload())
            kind, seq, visible = "market", envelope.sequence, envelope.available_at
        elif name.startswith("candidates/generations/"):
            candidate = StrategyCandidateSnapshot.model_validate_json(item.payload())
            kind, seq, visible = "candidate", candidate.sequence, candidate.captured_at
        elif name.startswith("constraint-publications/"):
            pointer = PaperExecutionConstraintPointer.model_validate_json(item.payload())
            kind, seq, visible = "constraint", pointer.sequence, pointer.published_at
        else:
            continue
        proofs.append(
            MinutePublicationEvidence(
                kind=kind,
                sequence=seq,
                material_path=name,
                origin_object_key=proof_origins[name],
                published_at=visible,
                time_basis="modeled",
            )
        )
    code_files = tuple(
        MinuteCodeFile(
            logical_name=name,
            content_sha256=hashlib.sha256((_REPOSITORY / name).read_bytes()).hexdigest(),
        )
        for name in _DEPENDENCY_FILES
    )
    code_files += (
        MinuteCodeFile(
            logical_name=_ORIGINAL_FIXTURE.relative_to(_REPOSITORY).as_posix(),
            content_sha256=ORIGINAL_FIXTURE_SHA256,
        ),
    )
    source_index = {
        "synthetic": True,
        "python_version": sys.version,
        "native_binding": binding.model_dump(mode="json"),
        "read": None if read is None else read.model_dump(mode="json"),
        "originals": [
            {
                "object_key": item.object_key,
                "content_sha256": item.content_sha256,
                "format": item.format,
                "physical_path": f"origins/{index:06d}"
                + (".parquet" if item.format == "parquet" else ".json"),
            }
            for index, item in enumerate(origins)
        ],
    }
    replay_available = replay_available_at or datetime.combine(window.start_date, time(), UTC)
    warmup_available = datetime.combine(window.start_date, time(), UTC)
    provenance = MinuteProvenance(
        source_kind="reconstructed",
        capture_lineage=tuple(
            MinuteCaptureLineage(
                object_key=item.object_key,
                content_sha256=item.content_sha256,
                acquisition_commit=calendar.producer_commit,
                captured_at=None,
                timing_evidence_object_key=None,
                collector_id="synthetic-native-phase-factory",
            )
            for item in origins
        ),
        publication_evidence=tuple(proofs),
        extracted_at=published_at,
        published_at=published_at,
        extractor_code_commit=calendar.producer_commit,
        extractor_fingerprint=canonical_sha256(code_files),
        research_code_commit=calendar.producer_commit,
        source_index_sha256=canonical_sha256(source_index),
        code_files=code_files,
        replay_start=ticks[0],
        replay_end=ticks[-1],
        native_definition_replay_available_at=replay_available,
        visibility_policy=policy,
    )
    work = MinuteReplayWork(
        raw_rows=sequence * len(codes),
        warmup_rows=len(history),
        static_rows=len(replay_dates) * len(codes) * 2 + len(calendar.open_dates) + len(ticks),
        market_batches=sequence,
        union_codes=len(codes),
        daily_observations=len(replay_dates),
    )
    runtime = MinuteRuntimeContent(
        source_key=source_key,
        source_version=source_version,
        owner_id=owner_id,
        producer_commit=calendar.producer_commit,
        available_at=replay_available,
        start_date=window.start_date,
        end_date=window.end_date,
        complete_through=ticks[-1],
        warmup_available_at=warmup_available,
        warmup_complete=True,
        holding_tail_complete=True,
        strategy=binding,
        feature_config=features,
        market_calendar=calendar,
        execution_profile=profile,
        work=work,
        result_budget={},
        tick_times=tuple(ticks),
        materials=tuple(materials),
    )
    formal_work = measure_minute_formal_work(
        work, origins=origins, provenance=provenance, derivations=derivations
    )
    seed = MinuteSourceContentSeed(
        runtime=runtime,
        native_registration=native,
        wrapper_registration=wrapper,
        provenance=provenance,
        origin_materials=origins,
        derivations=derivations,
        formal_work=formal_work,
        result_budget=runtime.result_budget,
    )
    for index, item in enumerate(origins):
        suffix = ".parquet" if item.format == "parquet" else ".json"
        _write_new(root / "origins" / f"{index:06d}{suffix}", item.payload())
    _write_new(
        root / "source-index.json",
        json.dumps(source_index, sort_keys=True, separators=(",", ":")).encode(),
    )
    _write_new(root / "source-seed.json", seed.model_dump_json().encode())
    manifest = NativePhaseSourceManifest(
        original_fixture_sha256=ORIGINAL_FIXTURE_SHA256,
        python_version=sys.version,
        read=read,
        seed_hash=seed.seed_hash,
        calendar_sha256=calendar.content_sha256,
        profile_hash=profile.profile_hash,
        native_registration_fingerprint=native.fingerprint,
        native_executable_fingerprint=native.executable_fingerprint,
        modeled_replay_available_at=replay_available,
        actual_research_published_at=published_at,
        warmup_dates=warmup_dates,
        replay_dates=replay_dates,
        materials=tuple(
            _PhysicalMaterial(
                relative_path=item.relative_path,
                content_sha256=item.content_sha256,
                size_bytes=len(item.payload()),
            )
            for item in materials
        ),
        origins=tuple(
            _PhysicalMaterial(
                relative_path=f"origins/{index:06d}"
                + (".parquet" if item.format == "parquet" else ".json"),
                content_sha256=item.content_sha256,
                size_bytes=len(item.payload()),
            )
            for index, item in enumerate(origins)
        ),
        source_code=code_files,
        limitations=policy.limitations,
    )
    _write_new(root / "source-manifest.json", manifest.model_dump_json(indent=2).encode())
    return seed


def build_native_phase_base_seed(
    root: Path,
    *,
    native_id: NativeMinuteStrategyId,
    calendar: MarketCalendarAuthority,
    window: DateRange,
    published_at: datetime,
    owner_id: str = "fixture-owner",
    source_key: str = "synthetic.native-minute",
    source_version: int = 1,
    account_id: str | None = None,
) -> MinuteSourceContentSeed:
    """Bootstrap fresh current definitions; retain only the old raw shape reference."""
    if native_id not in {"n_shape", "auction_gap", "growth_board_surge"}:
        raise ValueError("synthetic phase requires an original native minute definition")
    published_at = normalize_aware_utc(published_at)
    profile, features = _blueprint()
    profile_data = profile.model_dump(mode="python")
    profile_data["paper_policy"] |= {
        "account_id": account_id or "native-" + native_id,
        "producer_commit": calendar.producer_commit,
    }
    profile = MinuteReplayExecutionProfile.model_validate(profile_data)
    features = IntradayFeatureConfig.model_validate(
        features.model_dump(mode="python") | {"producer_commit": calendar.producer_commit}
    )
    _dates(calendar, window, published_at, features.lookback_sessions)
    _new_root(root)
    definitions = root / "definitions"
    plan = plan_builtin_definitions(producer_commit=calendar.producer_commit)
    bootstrap_builtin_definitions(
        definitions,
        producer_commit=calendar.producer_commit,
        registered_at=published_at,
        available_at=published_at,
        expected_plan_id=plan.plan_id,
    )
    wrapper = bootstrap_minute_definition(
        definitions, producer_commit=calendar.producer_commit, now=published_at
    )
    registry = ImmutableDefinitionRegistry(
        definitions,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=calendar.producer_commit
        ).trusted_executable_registry(),
    )
    native = registry.latest_strategy_spec(native_id, as_of=published_at)
    if native is None:
        raise PermissionError("original native bootstrap produced no exact definition")
    binding = next(item for item in plan.strategies if item.strategy_id == native_id)
    return _build_source(
        root,
        native=native,
        wrapper=wrapper,
        binding=binding,
        profile=profile,
        features=features,
        calendar=calendar,
        window=window,
        source_key=source_key,
        source_version=source_version,
        owner_id=owner_id,
        published_at=published_at,
        read=None,
    )


def build_native_phase_seed(
    root: Path,
    *,
    read: NativeMinutePhaseRead,
    base_seed: MinuteSourceContentSeed,
    calendar: MarketCalendarAuthority,
    published_at: datetime,
) -> MinuteSourceContentSeed:
    """Build an exact new phase archive, preserving its complete native/profile identity."""
    base_seed = MinuteSourceContentSeed.model_validate(base_seed.model_dump(mode="python"))
    read = NativeMinutePhaseRead.model_validate(read.model_dump(mode="python"))
    published_at = normalize_aware_utc(published_at)
    native, runtime, selection = (
        base_seed.native_registration,
        base_seed.runtime,
        read.configuration.selection,
    )
    target = selection.target
    if (
        read.owner,
        read.source_key,
        read.source_version,
        selection.source_key,
        selection.source_version,
        selection.profile_hash,
        target.owner_id,
        target.strategy_id,
        target.head.version,
        target.head.registration_fingerprint,
        target.head.record_hash,
        target.head.spec_fingerprint,
        target.parameter_fingerprint,
        target.cost_fingerprint,
    ) != (
        runtime.owner_id,
        runtime.source_key,
        runtime.source_version,
        runtime.source_key,
        runtime.source_version,
        runtime.execution_profile.profile_hash,
        runtime.owner_id,
        native.logical_id,
        native.version,
        native.fingerprint,
        native.record_hash,
        native.spec.spec_fingerprint,
        native.spec.parameter_fingerprint,
        canonical_sha256(runtime.execution_profile.execution_costs),
    ):
        raise PermissionError(
            "synthetic phase read differs from the complete native owner/source/profile"
        )
    if (read.configuration.start_date, read.configuration.end_date) != (
        read.window.start_date,
        read.window.end_date,
    ):
        raise PermissionError("synthetic phase read configuration and exact window differ")
    if (
        calendar != runtime.market_calendar
        or calendar.producer_commit != runtime.producer_commit
        or runtime.execution_profile.timestamp_semantics != "bar_end"
    ):
        raise PermissionError("synthetic phase calendar/code or timestamp profile differs")
    if published_at < max(native.available_at, base_seed.wrapper_registration.available_at):
        raise PermissionError("synthetic phase publication precedes its real native registration")
    if runtime.available_at > _at(read.window.start_date, 30):
        raise PermissionError("synthetic native replay bootstrap is after the exact phase start")
    _dates(calendar, read.window, published_at, runtime.feature_config.lookback_sessions)
    _new_root(root)
    return _build_source(
        root,
        native=native,
        wrapper=base_seed.wrapper_registration,
        binding=runtime.strategy,
        profile=runtime.execution_profile,
        features=runtime.feature_config,
        calendar=calendar,
        window=read.window,
        source_key=read.publication_source_key,
        source_version=read.source_version,
        owner_id=read.owner,
        published_at=published_at,
        read=read,
        replay_available_at=runtime.available_at,
    )
