#!/usr/bin/env python3
"""Build the production runtime inputs document and the authority files it names.

`docs/production-release.md` names `data/runtime-production-inputs.json` as the input of
`rquant runtime-production-prerequisites` / `runtime-production-profile`, and
`scripts/deploy-production.sh` refuses to run without it — but nothing in this repository
has ever written one. Every consumer exists; this is the missing producer.

What it emits, all under one private output root plus the inputs document itself:

`market-calendar-authority.json` (0600)
    `runtime_market_session.load_market_calendar_authority`, read at build time by
    `install_production_runtime_prerequisites` — the one input read before rollout.
`trade-calendar.json` (0600)
    `runtime_paper_quote._load_sse_open_dates`, the paper broker's PIT calendar.
`signal-routing-policy.json` (0444)
    `runtime_routing_policy.load_frozen_routing_policy`, the signal router.
`artifact-descriptor-schema.json` (0600)
    `runtime_builder_retention.TrustedDescriptorSchemaResolver`.
`n-shape-candidates.json` / `growth-board-surge-candidates.json` (0600)
    `runtime_builder_candidate.load_candidate_input`, the sealed_document input mode.
the inputs document itself (0600)
    `runtime_production_profile.load_production_runtime_profile_inputs`.

The routing policy is the one file that must **not** be 0600: its loader rejects any file
carrying a write bit (`_read_frozen_policy`, "routing policy file must be read-only"), so it
is written 0444. Everything else is rejected unless it is private to its owner.

Determinism is a contract, not a nicety: the operator reruns this to prove the document on
the host matches the document that was reviewed. Nothing here reads the wall clock. The
calendar's `generated_at` defaults to the newest `updated_at` of the SSE rows the query
returned, and the sealed candidate documents take their trade date from the calendar. Pass
`--generated-at` to pin it explicitly.

Every sha256 in the inputs document is computed here from the bytes this script just wrote,
except `historical_minutes_snapshot_id`, whose parquet is produced separately by
`scripts/export_intraday_snapshot.py` on the machine that holds the minute history.

Usage (the host layout of 82.156.0.68):

    python scripts/build_runtime_production_inputs.py \\
        --checkout /home/lighthouse/rquant \\
        --calendar-database /home/lighthouse/rquant/data/rquant_ro.duckdb \\
        --output-root /home/lighthouse/rquant/data/runtime-inputs \\
        --inputs-output /home/lighthouse/rquant/data/runtime-production-inputs.json \\
        --minutes-snapshot /home/lighthouse/rquant/data/runtime-inputs/minute-history.parquet \\
        --minutes-snapshot-sha256 <64 hex> \\
        --canvas-keyring /etc/rquant/canvas-publication-trusted-keys.json \\
        --shadow-report-keyring /etc/rquant/shadow-report-trusted-keys.json \\
        --shadow-completion-keyring /etc/rquant/shadow-completion-trusted-keys.json \\
        --runtime-mode linux-production
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
if str(_REPOSITORY_ROOT / "src") not in sys.path:  # pragma: no cover - import bootstrap
    sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))

from rquant.live_contracts import BatchQualityStatus  # noqa: E402
from rquant.runtime_builder_candidate import serialize_candidate_input  # noqa: E402
from rquant.runtime_builder_retention import (  # noqa: E402
    DescriptorSchemaBinding,
    TrustedDescriptorSchemaAuthority,
)
from rquant.runtime_contracts import canonical_sha256  # noqa: E402
from rquant.runtime_definition_bootstrap import plan_builtin_definitions  # noqa: E402
from rquant.runtime_deployment_profile import (  # noqa: E402
    RuntimeRecoveryArtifactRoleBinding,
    RuntimeRecoveryProductionConfig,
)
from rquant.runtime_market_session import MarketCalendarAuthority  # noqa: E402
from rquant.runtime_production_profile import (  # noqa: E402
    ProductionRuntimeProfileInputs,
    ProductionStrategyBinding,
)
from rquant.runtime_recovery_artifacts import RealRecoveryArtifactKind  # noqa: E402
from rquant.runtime_routing_policy import RoutingPolicyDocument  # noqa: E402
from rquant.strategy_candidate_producers import (  # noqa: E402
    PublishedCandidateInputAuthority,
)
from rquant.strategy_candidate_publish_service import (  # noqa: E402
    GrowthBoardCandidateBatch,
    NShapeCandidateBatch,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads  # noqa: E402

SHANGHAI = ZoneInfo("Asia/Shanghai")

#: Coordinator ruling 8: the market calendar must reach at least this date, and the operator
#: takes whatever further the exchange calendar table can supply. The expiry goes into
#: DEPLOY.md with a renewal step.
DEFAULT_COVERAGE_FLOOR = date(2027, 12, 31)
#: Coordinator ruling 4.
DEFAULT_ARTIFACT_LOCATION_ID = "tencent-lighthouse-82-156-0-68"
DEFAULT_ARTIFACT_FAILURE_DOMAIN = "tencent-lighthouse-single-host"
#: Coordinator ruling 7: three strategies, every action, one admin recipient on one channel.
DEFAULT_ROUTING_RECIPIENT_ID = "admin"
DEFAULT_ROUTING_CHANNEL = "pushdeer"
DEFAULT_NO_TARGET_REASON = "no_matching_route"
#: `NotifierSettings.pushdeer_recipient_id` defaults to "admin" and the production profile
#: never overrides it, so a rule naming any other recipient would route to nobody.
ROUTING_ACTIONS = ("watch", "b_intent", "reduce", "s_intent", "cancel")
STRATEGY_IDS = ("auction_gap", "growth_board_surge", "n_shape")
#: `strategy_live` stamps `strategy_version=str(spec.version)` and every built-in binding is
#: version 1, so the router looks up the literal "1".
ROUTING_STRATEGY_VERSION = "1"

#: Coordinator ruling 5, "minimal and conservative": the retention schema authority must
#: carry at least one binding (`bindings: Field(min_length=1)`), and a binding is a promise
#: that a descriptor with this exact (content_sha256, size_bytes) has that schema. Binding a
#: pair no real artifact can present is how a one-binding document says "nothing here is
#: managed by schema, so nothing gets deleted on schema grounds". A zero-byte artifact
#: hashes to e3b0c442…, never to this sentinel.
RETENTION_SENTINEL_CONTENT = b"rquant/route-a/retention-schema-authority/unbound-sentinel"
RETENTION_SENTINEL_SCHEMA = b"rquant/route-a/retention-schema-authority/no-managed-schema"

#: The twelve recovery artifact roles `validate_complete_recovery_artifact_graph` demands,
#: as POSIX paths relative to `backup_source_root` (= `<runtime_root>/..`). The production
#: and paper-ledger roles are filled in from the runtime layout because
#: `_validate_recovery_artifact_bindings` compares them against
#: `operational_database_path` and the broker's own `broker.sqlite3`.
RESEARCH_CATALOG_RELATIVE = "research.duckdb"
RESEARCH_CATALOG_READONLY_RELATIVE = "research_ro.duckdb"
RESEARCH_LAKE_MANIFEST_RELATIVE = "research-lake/current.json"
RESEARCH_LAKE_OBJECT_RELATIVE = "research-lake/objects/current.parquet"


class GeneratorError(RuntimeError):
    """The inputs document cannot be produced from what the operator supplied."""


# ---------------------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------------------


def _require_absolute(value: str, *, label: str) -> Path:
    path = Path(value)
    normalized = Path(os.path.abspath(path))
    if not path.is_absolute() or path != normalized:
        raise GeneratorError(f"{label} must be an absolute normalized path: {value}")
    return path


def write_private_file(path: Path, payload: bytes, *, mode: int) -> str:
    """Write `payload` at `path` with exactly `mode`, replacing any previous version.

    Returns the sha256 of the bytes written. The write goes to a sibling temporary name and
    is renamed into place so a reader never observes a half-written authority, and the mode
    is set on the descriptor before the rename so it is never briefly world-readable.
    """

    if not path.is_absolute():
        raise GeneratorError(f"output path must be absolute: {path}")
    staging = path.with_name(f".{path.name}.staging")
    descriptor = os.open(
        staging,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fchmod(descriptor, mode)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(staging, path)
    parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
    return hashlib.sha256(payload).hexdigest()


def prepare_output_root(root: Path) -> None:
    """Create the output root as a private directory the strict loaders will accept.

    `TrustedDescriptorSchemaResolver._read_bound_authority` rejects a trust root whose mode
    carries any group or other bit, and it is the parent of the retention authority file, so
    0700 is the mode every generated document has to live under.
    """

    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    observed = root.stat()
    if not stat.S_ISDIR(observed.st_mode) or stat.S_IMODE(observed.st_mode) & 0o077:
        raise GeneratorError(f"output root must be a private directory: {root}")


# ---------------------------------------------------------------------------------------
# Trading calendar
# ---------------------------------------------------------------------------------------


def read_sse_calendar(
    database: Path,
    *,
    allow_primary_database: bool,
) -> tuple[tuple[date, bool, datetime], ...]:
    """Read every SSE row of `trade_calendar`, newest `updated_at` last.

    The connection is read-only and the primary database is refused by default. DuckDB takes
    a single file lock: while `rquant-monitor` holds the write lock, *any* new connection to
    the primary fails, `read_only=True` included (CLAUDE.md, the 2026-05-20 incident). The
    replica `rquant_ro.duckdb` is the supported reader path.
    """

    if database.name == "rquant.duckdb" and not allow_primary_database:
        raise GeneratorError(
            "refusing to open the primary DuckDB; point --calendar-database at the "
            "read-only replica (rquant_ro.duckdb) or pass --allow-primary-database"
        )
    import duckdb

    connection = duckdb.connect(str(database), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT cal_date, is_open, updated_at
            FROM trade_calendar
            WHERE exchange = 'SSE'
            ORDER BY cal_date
            """
        ).fetchall()
    finally:
        connection.close()
    if not rows:
        raise GeneratorError(f"trade_calendar holds no SSE rows: {database}")
    return tuple(
        (
            _as_date(row[0]),
            bool(row[1]),
            _as_utc(row[2]),
        )
        for row in rows
    )


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise GeneratorError(f"trade_calendar cal_date is not a date: {value!r}")


def _as_utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise GeneratorError(f"trade_calendar updated_at is not a timestamp: {value!r}")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def build_market_calendar_authority(
    rows: Sequence[tuple[date, bool, datetime]],
    *,
    producer_commit: str,
    generated_at: datetime,
    coverage_floor: date,
) -> MarketCalendarAuthority:
    """The `MarketCalendarAuthority` the runtime root's calendar generation is built from."""

    coverage_start = min(row[0] for row in rows)
    coverage_end = max(row[0] for row in rows)
    if coverage_end < coverage_floor:
        raise GeneratorError(
            "trade_calendar coverage ends "
            f"{coverage_end.isoformat()}, before the required floor "
            f"{coverage_floor.isoformat()}; extend the calendar table first"
        )
    open_dates = tuple(sorted(row[0] for row in rows if row[1]))
    if not open_dates:
        raise GeneratorError("trade_calendar has no open SSE dates")
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=producer_commit,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        open_dates=open_dates,
        generated_at=generated_at,
    )


def build_pit_trade_calendar_payload(rows: Sequence[tuple[date, bool, datetime]]) -> bytes:
    """The paper broker's PIT calendar: a different file format from the same table.

    `runtime_paper_quote._calendar_frame` reads a JSON list (or an object carrying `rows` /
    `trade_calendar`) into a DataFrame and needs exactly `exchange`, `cal_date`, `is_open`.
    `_strict_bool` refuses 0/1, so `is_open` must be a JSON boolean, and `pd.to_datetime`
    must see naive dates, so `cal_date` is a bare `YYYY-MM-DD`.
    """

    payload = [
        {
            "exchange": "SSE",
            "cal_date": cal_date.isoformat(),
            "is_open": bool(is_open),
        }
        for cal_date, is_open, _updated_at in sorted(rows, key=lambda row: row[0])
    ]
    return canonical_json_bytes({"rows": payload})


# ---------------------------------------------------------------------------------------
# Routing policy, retention authority, sealed candidates
# ---------------------------------------------------------------------------------------


def build_routing_policy_payload(
    *,
    recipient_id: str,
    channel: str,
    default_no_target_reason: str,
) -> bytes:
    """Ruling 7's minimal policy: every strategy and action routed to one admin channel."""

    document = RoutingPolicyDocument.model_validate(
        {
            "default_no_target_reason": default_no_target_reason,
            "rules": [
                {
                    "strategy_id": strategy_id,
                    "strategy_version": ROUTING_STRATEGY_VERSION,
                    "action": action,
                    "recipient_id": recipient_id,
                    "channel": channel,
                    "enabled": True,
                }
                for strategy_id in STRATEGY_IDS
                for action in ROUTING_ACTIONS
            ],
        }
    )
    return canonical_json_bytes(document.model_dump(mode="json"))


def build_retention_schema_authority_payload() -> bytes:
    """Ruling 5's minimal authority: one binding that no real descriptor can match."""

    binding = DescriptorSchemaBinding(
        content_sha256=hashlib.sha256(RETENTION_SENTINEL_CONTENT).hexdigest(),
        size_bytes=0,
        schema_sha256=hashlib.sha256(RETENTION_SENTINEL_SCHEMA).hexdigest(),
    )
    authority_id = canonical_sha256(
        {
            "schema_version": 1,
            "bindings": [binding.model_dump(mode="python")],
        }
    )
    authority = TrustedDescriptorSchemaAuthority(
        schema_version=1,
        bindings=(binding,),
        authority_id=authority_id,
    )
    return canonical_json_bytes(authority.model_dump(mode="json"))


def build_sealed_candidate_payload(
    *,
    strategy_id: str,
    producer_commit: str,
    trade_date: date,
    captured_at: datetime,
) -> bytes:
    """Ruling 6's empty sealed document, in the shape `load_candidate_input` accepts.

    `serialize_candidate_input` is the loader's own inverse, so the bytes are canonical typed
    JSON by construction rather than by imitation. `authority_snapshot_id` addresses the
    (empty) fact list, so re-sealing the same emptiness yields the same id.
    """

    authority = PublishedCandidateInputAuthority(
        trade_date=trade_date,
        captured_at=captured_at,
        quality_status=BatchQualityStatus.PUBLISHED,
        authority_snapshot_id=canonical_sha256(
            {
                "contract": "route-a/sealed-candidate-input/v1",
                "strategy_id": strategy_id,
                "trade_date": trade_date,
                "facts": [],
            }
        ),
        producer_commit=producer_commit,
    )
    if strategy_id == "n_shape":
        return serialize_candidate_input(NShapeCandidateBatch(authority=authority, facts=()))
    if strategy_id == "growth_board_surge":
        return serialize_candidate_input(GrowthBoardCandidateBatch(authority=authority, facts=()))
    raise GeneratorError(f"no sealed candidate document is defined for {strategy_id}")


def sealed_candidate_trade_date(
    open_dates: Sequence[date],
    *,
    generated_at: datetime,
) -> tuple[date, datetime]:
    """The newest open date at or before `generated_at`, and 15:00 CST on it.

    `PublishedCandidateInputAuthority` requires `captured_at` to fall on `trade_date` in
    Asia/Shanghai; the close is the honest capture instant for a document sealed from a
    finished session. Both values are functions of the inputs, so reruns match.
    """

    reference = generated_at.astimezone(SHANGHAI).date()
    eligible = [item for item in open_dates if item <= reference]
    if not eligible:
        raise GeneratorError("the calendar has no open date at or before the generation instant")
    trade_date = max(eligible)
    captured_at = datetime.combine(trade_date, time(15, 0), tzinfo=SHANGHAI).astimezone(UTC)
    return trade_date, captured_at


# ---------------------------------------------------------------------------------------
# Recovery configuration
# ---------------------------------------------------------------------------------------


def build_recovery_config(
    *,
    runtime_root: Path,
    operational_database_path: Path,
    broker_instance: str,
    backup_config_path: Path,
    credential_file: Path,
    signer_key_id: str,
) -> RuntimeRecoveryProductionConfig:
    """The twelve-role recovery graph, with every path the runtime root already fixes.

    `validate_trusted_runtime_root` pins five of the roots to exact derivations of the
    runtime root and the `/var/lib/rquant/runtime-recovery` sandbox, so there is nothing for
    an operator to choose there. What is left free is the two read-only inputs and the
    research artefact locations.
    """

    backup_source_root = runtime_root.parent
    try:
        production_relative = operational_database_path.relative_to(backup_source_root)
    except ValueError as exc:
        raise GeneratorError(
            f"the operational database must live under the backup source root {backup_source_root}"
        ) from exc
    paper_relative = Path("runtime") / "live" / "paper-brokers" / broker_instance / "broker.sqlite3"
    roles = (
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="paper_ledger",
            kind=RealRecoveryArtifactKind.STATE_SQLITE,
            source_path=paper_relative.as_posix(),
            restore_path="state/paper.sqlite3",
            schema_version="paper-ledger-v5",
            relations=(
                "paper_ledger_attestation",
                "paper_ledger_head_marker",
                "paper_ledger_schema",
            ),
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="production",
            kind=RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
            source_path=production_relative.as_posix(),
            restore_path="production/rquant.duckdb",
            schema_version="v1",
            relations=("auction_bar", "daily_bar", "minute_bar"),
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="runtime_state",
            kind=RealRecoveryArtifactKind.STATE_SQLITE,
            source_path="runtime/control/recovery/service.sqlite3",
            restore_path="state/runtime-recovery.sqlite3",
            schema_version="runtime-recovery-service-v1",
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="research_catalog",
            kind=RealRecoveryArtifactKind.RESEARCH_CATALOG,
            source_path=RESEARCH_CATALOG_RELATIVE,
            restore_path="research/research.duckdb",
            schema_version="research-catalog-v1",
            references={"lake:current": "research_lake_manifest"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="research_catalog_readonly",
            kind=RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
            source_path=RESEARCH_CATALOG_READONLY_RELATIVE,
            restore_path="research/research_ro.duckdb",
            schema_version="research-catalog-v1",
            references={"authority": "research_catalog"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="research_lake_manifest",
            kind=RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST,
            source_path=RESEARCH_LAKE_MANIFEST_RELATIVE,
            restore_path="research-lake/current.json",
            schema_version="research-lake-manifest-v1",
            references={"parquet": "research_lake_object"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="research_lake_object",
            kind=RealRecoveryArtifactKind.RESEARCH_LAKE_OBJECT,
            source_path=RESEARCH_LAKE_OBJECT_RELATIVE,
            restore_path="research-lake/objects/current.parquet",
            schema_version="parquet-v1",
            references={"manifest": "research_lake_manifest"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="lab_artifact_manifest",
            kind=RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST,
            source_path="runtime/research/final-artifacts/current.json",
            restore_path="lab-artifacts/current.json",
            schema_version="lab-artifact-manifest-v1",
            references={"file:current.json": "lab_artifact_object"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="lab_artifact_object",
            kind=RealRecoveryArtifactKind.LAB_ARTIFACT_OBJECT,
            source_path="runtime/research/final-artifacts/objects/current.json",
            restore_path="lab-artifacts/objects/current.json",
            schema_version="lab-artifact-object-v1",
            references={"manifest": "lab_artifact_manifest"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="serving_current",
            kind=RealRecoveryArtifactKind.SERVING_CURRENT,
            source_path="runtime/serving/current.json",
            restore_path="serving/current.json",
            schema_version="serving-current-v1",
            references={"manifest": "serving_manifest"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="serving_manifest",
            kind=RealRecoveryArtifactKind.SERVING_MANIFEST,
            source_path="runtime/serving/current/manifest.json",
            restore_path="serving/current/manifest.json",
            schema_version="serving-manifest-v3",
            references={"database": "serving_database", "reference": "reference_slow"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="serving_database",
            kind=RealRecoveryArtifactKind.SERVING_DATABASE,
            source_path="runtime/serving/current/serving.duckdb",
            restore_path="serving/current/serving.duckdb",
            schema_version="serving-v3",
            references={"manifest": "serving_manifest"},
        ),
        RuntimeRecoveryArtifactRoleBinding(
            logical_role="reference_slow",
            kind=RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
            source_path="runtime/authorities/reference-slow/reference.sqlite3",
            restore_path="reference/reference.sqlite3",
            schema_version="reference-slow-v1",
        ),
    )
    return RuntimeRecoveryProductionConfig(
        backup_source_root=backup_source_root,
        backup_publication_root=Path("/var/lib/rquant/runtime-recovery/backups"),
        isolated_restore_root=Path("/var/lib/rquant/runtime-recovery/restores"),
        service_state_path=runtime_root / "control" / "recovery" / "service.sqlite3",
        service_receipt_root=runtime_root / "control" / "recovery" / "receipts",
        backup_config_path=backup_config_path,
        credential_file=credential_file,
        artifact_roles=roles,
        production_artifact_role="production",
        paper_ledger_artifact_role="paper_ledger",
        signer_key_id=signer_key_id,
        max_rpo_seconds=1800,
        max_rto_seconds=900,
        max_rehearsal_age_seconds=604800,
        recovery_lease_seconds=300,
        recovery_max_attempts=3,
        recovery_retry_delay_seconds=60,
        recovery_deadline_seconds=3600,
        rehearsal_interval_seconds=604800,
    )


# ---------------------------------------------------------------------------------------
# Signing keyrings
# ---------------------------------------------------------------------------------------


def read_active_public_key(path: Path) -> tuple[str, str]:
    """The active key id and PEM of one `/etc/rquant` trusted keyring.

    The keyrings B-2 and B-3 install are canonical JSON documents naming an active key; this
    reads the pair the inputs document has to restate and nothing else. No private material
    is touched — these files hold public keys only.
    """

    try:
        document = json.loads(path.read_bytes())
    except OSError as exc:
        raise GeneratorError(f"trusted keyring is unreadable: {path}") from exc
    except ValueError as exc:
        raise GeneratorError(f"trusted keyring is not JSON: {path}") from exc
    if not isinstance(document, dict):
        raise GeneratorError(f"trusted keyring is not an object: {path}")
    key_id = document.get("active_key_id")
    public_key = document.get("active_public_key_pem", document.get("active_public_key"))
    if not isinstance(key_id, str) or not key_id:
        raise GeneratorError(f"trusted keyring has no active_key_id: {path}")
    if not isinstance(public_key, str) or not public_key:
        raise GeneratorError(f"trusted keyring has no active public key: {path}")
    return key_id, public_key


# ---------------------------------------------------------------------------------------
# The inputs document
# ---------------------------------------------------------------------------------------


def _instance_name(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()


def build_inputs_payload(
    *,
    producer_commit: str,
    runtime_mode: str,
    runtime_root: Path,
    operational_database_path: Path,
    definition_registry_root: Path,
    n_shape_candidate_input_path: Path,
    growth_board_candidate_input_path: Path,
    historical_minutes_snapshot_path: Path,
    historical_minutes_snapshot_id: str,
    market_calendar_authority_path: Path,
    market_calendar_content_sha256: str,
    market_calendar_producer_commit: str,
    trade_calendar_path: Path,
    trade_calendar_sha256: str,
    routing_policy_path: Path,
    routing_policy_fingerprint: str,
    artifact_location_id: str,
    artifact_failure_domain: str,
    artifact_retention_schema_authority_path: Path,
    artifact_retention_schema_authority_sha256: str,
    artifact_retention_recovery_target_manifest_id: str,
    artifact_retention_full_recovery_receipt_id: str,
    recovery: RuntimeRecoveryProductionConfig,
    canvas_publication_active_key_id: str | None,
    canvas_publication_active_public_key_pem: str | None,
    shadow_completion_active_key_id: str,
    shadow_completion_active_public_key_pem: str,
    shadow_report_active_key_id: str,
    shadow_report_active_public_key_pem: str,
) -> dict[str, Any]:
    """Validate the whole document as a model, then hand back its canonical dump.

    The model is built in `local-test` mode even when the target is `linux-production`,
    because the linux branch of `validate_complete_authority_set` demands the Daily receipt
    authority already be hydrated from `/etc/rquant/daily-receipt-trusted-keys.json` — a file
    that exists only on the production host, and one the loader fills in for itself
    (`_hydrate_daily_receipt_authority_from_fixed_keyring`). Everything the local mode does
    check is checked here: path normalization, the immutable inputs staying outside the
    runtime owner root, the twelve recovery roles, the three strategy bindings and their
    fingerprints, the Shadow keyrings. The three `daily_receipt_*` fields are emitted as
    null/empty because the loader refuses a document that fills them in itself.
    """

    strategies = tuple(
        ProductionStrategyBinding.model_validate(binding.model_dump(mode="python"))
        for binding in plan_builtin_definitions(producer_commit=producer_commit).strategies
    )
    inputs = ProductionRuntimeProfileInputs(
        producer_commit=producer_commit,
        runtime_mode="local-test",
        runtime_root=runtime_root,
        operational_database_path=operational_database_path,
        definition_registry_root=definition_registry_root,
        n_shape_candidate_input_path=n_shape_candidate_input_path,
        growth_board_candidate_input_path=growth_board_candidate_input_path,
        historical_minutes_snapshot_path=historical_minutes_snapshot_path,
        historical_minutes_snapshot_id=historical_minutes_snapshot_id,
        market_calendar_authority_path=market_calendar_authority_path,
        market_calendar_content_sha256=market_calendar_content_sha256,
        market_calendar_producer_commit=market_calendar_producer_commit,
        trade_calendar_path=trade_calendar_path,
        trade_calendar_sha256=trade_calendar_sha256,
        routing_policy_path=routing_policy_path,
        routing_policy_fingerprint=routing_policy_fingerprint,
        strategies=strategies,
        artifact_location_id=artifact_location_id,
        artifact_failure_domain=artifact_failure_domain,
        artifact_retention_schema_authority_path=artifact_retention_schema_authority_path,
        artifact_retention_schema_authority_sha256=artifact_retention_schema_authority_sha256,
        artifact_retention_recovery_target_manifest_id=(
            artifact_retention_recovery_target_manifest_id
        ),
        artifact_retention_full_recovery_receipt_id=artifact_retention_full_recovery_receipt_id,
        recovery=recovery,
        canvas_publication_active_key_id=canvas_publication_active_key_id,
        canvas_publication_active_public_key_pem=canvas_publication_active_public_key_pem,
        shadow_completion_active_key_id=shadow_completion_active_key_id,
        shadow_completion_active_public_key_pem=shadow_completion_active_public_key_pem,
        shadow_report_active_key_id=shadow_report_active_key_id,
        shadow_report_active_public_key_pem=shadow_report_active_public_key_pem,
    )
    payload = inputs.model_dump(mode="json")
    payload["runtime_mode"] = runtime_mode
    # `profile_generation` is recomputed from the recovery content on every load
    # (`RuntimeRecoveryProductionConfig.validate_identity_and_policy`), so the document does
    # not restate a hash that can only ever disagree.
    payload["recovery"].pop("profile_generation", None)
    payload["daily_receipt_active_key_id"] = None
    payload["daily_receipt_active_public_key_pem"] = None
    payload["daily_receipt_previous_public_key_pems"] = {}
    return payload


def content_addressed_recovery_id(*, label: str, producer_commit: str, location: str) -> str:
    """Ruling 5: the two retention recovery ids are content-addressed rather than invented.

    Nothing has produced a recovery target manifest or a full-recovery receipt yet, and the
    profile builder only checks the 64-hex shape. Deriving them from what the deployment
    actually is keeps two different deployments from claiming the same receipt.
    """

    return canonical_sha256(
        {
            "contract": "route-a/retention-recovery-identity/v1",
            "label": label,
            "producer_commit": producer_commit,
            "artifact_location_id": location,
        }
    )


# ---------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------


def _git_head(checkout: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise GeneratorError(f"cannot read the checkout HEAD: {checkout}") from exc
    return result.stdout.strip()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--checkout", default=str(_REPOSITORY_ROOT))
    parser.add_argument("--producer-commit", default=None)
    parser.add_argument("--calendar-database", required=True)
    parser.add_argument("--allow-primary-database", action="store_true")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--inputs-output", required=True)
    parser.add_argument("--runtime-root", default="/home/lighthouse/rquant/data/runtime")
    parser.add_argument(
        "--operational-database-path",
        default="/home/lighthouse/rquant/data/rquant.duckdb",
    )
    parser.add_argument(
        "--definition-registry-root",
        default="/home/lighthouse/rquant/data/runtime-inputs/definitions",
    )
    parser.add_argument("--minutes-snapshot", required=True)
    parser.add_argument("--minutes-snapshot-sha256", required=True)
    parser.add_argument("--runtime-mode", default="linux-production")
    parser.add_argument("--generated-at", default=None)
    parser.add_argument("--coverage-floor", default=DEFAULT_COVERAGE_FLOOR.isoformat())
    parser.add_argument("--artifact-location-id", default=DEFAULT_ARTIFACT_LOCATION_ID)
    parser.add_argument("--artifact-failure-domain", default=DEFAULT_ARTIFACT_FAILURE_DOMAIN)
    parser.add_argument("--routing-recipient-id", default=DEFAULT_ROUTING_RECIPIENT_ID)
    parser.add_argument("--routing-channel", default=DEFAULT_ROUTING_CHANNEL)
    parser.add_argument("--routing-default-reason", default=DEFAULT_NO_TARGET_REASON)
    parser.add_argument(
        "--recovery-backup-config",
        default="/home/lighthouse/rquant/data/recovery/runtime-recovery-backup.json",
    )
    parser.add_argument(
        "--recovery-credential-file",
        default="/home/lighthouse/rquant/data/recovery/runtime-recovery.json",
    )
    parser.add_argument("--recovery-signer-key-id", default="production-recovery-v1")
    parser.add_argument("--canvas-keyring", default=None)
    parser.add_argument("--canvas-active-key-id", default=None)
    parser.add_argument("--canvas-active-public-key-pem", default=None)
    parser.add_argument("--shadow-report-keyring", default=None)
    parser.add_argument("--shadow-report-active-key-id", default=None)
    parser.add_argument("--shadow-report-active-public-key-pem", default=None)
    parser.add_argument("--shadow-completion-keyring", default=None)
    parser.add_argument("--shadow-completion-active-key-id", default=None)
    parser.add_argument("--shadow-completion-active-public-key-pem", default=None)
    return parser


def _resolve_key(
    *,
    label: str,
    keyring: str | None,
    key_id: str | None,
    public_key: str | None,
    required: bool,
) -> tuple[str | None, str | None]:
    if keyring is not None:
        return read_active_public_key(_require_absolute(keyring, label=f"{label} keyring"))
    if key_id is not None and public_key is not None:
        return key_id, public_key
    if key_id is not None or public_key is not None:
        raise GeneratorError(f"{label} needs both an active key id and an active public key")
    if required:
        raise GeneratorError(f"{label} is required: pass its keyring or its active key pair")
    return None, None


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    arguments = parser.parse_args(argv)
    try:
        return _run(arguments)
    except GeneratorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


def _run(arguments: argparse.Namespace) -> int:
    checkout = _require_absolute(arguments.checkout, label="checkout")
    producer_commit = arguments.producer_commit or _git_head(checkout)
    runtime_root = _require_absolute(arguments.runtime_root, label="runtime root")
    output_root = _require_absolute(arguments.output_root, label="output root")
    inputs_output = _require_absolute(arguments.inputs_output, label="inputs output")
    calendar_database = _require_absolute(arguments.calendar_database, label="calendar database")
    coverage_floor = date.fromisoformat(arguments.coverage_floor)

    prepare_output_root(output_root)

    rows = read_sse_calendar(
        calendar_database,
        allow_primary_database=arguments.allow_primary_database,
    )
    if arguments.generated_at is None:
        generated_at = max(row[2] for row in rows)
    else:
        generated_at = datetime.fromisoformat(arguments.generated_at)
        if generated_at.tzinfo is None:
            raise GeneratorError("--generated-at must carry a timezone offset")
        generated_at = generated_at.astimezone(UTC)

    calendar = build_market_calendar_authority(
        rows,
        producer_commit=producer_commit,
        generated_at=generated_at,
        coverage_floor=coverage_floor,
    )
    calendar_path = output_root / "market-calendar-authority.json"
    write_private_file(
        calendar_path,
        canonical_json_bytes(calendar.model_dump(mode="json")),
        mode=0o600,
    )

    trade_calendar_path = output_root / "trade-calendar.json"
    trade_calendar_sha256 = write_private_file(
        trade_calendar_path,
        build_pit_trade_calendar_payload(rows),
        mode=0o600,
    )

    routing_policy_path = output_root / "signal-routing-policy.json"
    routing_policy_fingerprint = write_private_file(
        routing_policy_path,
        build_routing_policy_payload(
            recipient_id=arguments.routing_recipient_id,
            channel=arguments.routing_channel,
            default_no_target_reason=arguments.routing_default_reason,
        ),
        mode=0o444,
    )

    retention_path = output_root / "artifact-descriptor-schema.json"
    retention_sha256 = write_private_file(
        retention_path,
        build_retention_schema_authority_payload(),
        mode=0o600,
    )

    trade_date, captured_at = sealed_candidate_trade_date(
        calendar.open_dates,
        generated_at=generated_at,
    )
    n_shape_path = output_root / "n-shape-candidates.json"
    write_private_file(
        n_shape_path,
        build_sealed_candidate_payload(
            strategy_id="n_shape",
            producer_commit=producer_commit,
            trade_date=trade_date,
            captured_at=captured_at,
        ),
        mode=0o600,
    )
    growth_board_path = output_root / "growth-board-surge-candidates.json"
    write_private_file(
        growth_board_path,
        build_sealed_candidate_payload(
            strategy_id="growth_board_surge",
            producer_commit=producer_commit,
            trade_date=trade_date,
            captured_at=captured_at,
        ),
        mode=0o600,
    )

    canvas_key_id, canvas_public_key = _resolve_key(
        label="canvas publication key",
        keyring=arguments.canvas_keyring,
        key_id=arguments.canvas_active_key_id,
        public_key=arguments.canvas_active_public_key_pem,
        required=arguments.runtime_mode == "linux-production",
    )
    report_key_id, report_public_key = _resolve_key(
        label="shadow report key",
        keyring=arguments.shadow_report_keyring,
        key_id=arguments.shadow_report_active_key_id,
        public_key=arguments.shadow_report_active_public_key_pem,
        required=True,
    )
    completion_key_id, completion_public_key = _resolve_key(
        label="shadow completion key",
        keyring=arguments.shadow_completion_keyring,
        key_id=arguments.shadow_completion_active_key_id,
        public_key=arguments.shadow_completion_active_public_key_pem,
        required=True,
    )
    assert report_key_id is not None and report_public_key is not None
    assert completion_key_id is not None and completion_public_key is not None

    recovery = build_recovery_config(
        runtime_root=runtime_root,
        operational_database_path=_require_absolute(
            arguments.operational_database_path,
            label="operational database path",
        ),
        broker_instance=_instance_name("paper-broker.shadow-main.v1"),
        backup_config_path=_require_absolute(
            arguments.recovery_backup_config,
            label="recovery backup config",
        ),
        credential_file=_require_absolute(
            arguments.recovery_credential_file,
            label="recovery credential file",
        ),
        signer_key_id=arguments.recovery_signer_key_id,
    )

    payload = build_inputs_payload(
        producer_commit=producer_commit,
        runtime_mode=arguments.runtime_mode,
        runtime_root=runtime_root,
        operational_database_path=_require_absolute(
            arguments.operational_database_path,
            label="operational database path",
        ),
        definition_registry_root=_require_absolute(
            arguments.definition_registry_root,
            label="definition registry root",
        ),
        n_shape_candidate_input_path=n_shape_path,
        growth_board_candidate_input_path=growth_board_path,
        historical_minutes_snapshot_path=_require_absolute(
            arguments.minutes_snapshot,
            label="historical minutes snapshot",
        ),
        historical_minutes_snapshot_id=arguments.minutes_snapshot_sha256,
        market_calendar_authority_path=calendar_path,
        market_calendar_content_sha256=calendar.content_sha256,
        market_calendar_producer_commit=producer_commit,
        trade_calendar_path=trade_calendar_path,
        trade_calendar_sha256=trade_calendar_sha256,
        routing_policy_path=routing_policy_path,
        routing_policy_fingerprint=routing_policy_fingerprint,
        artifact_location_id=arguments.artifact_location_id,
        artifact_failure_domain=arguments.artifact_failure_domain,
        artifact_retention_schema_authority_path=retention_path,
        artifact_retention_schema_authority_sha256=retention_sha256,
        artifact_retention_recovery_target_manifest_id=content_addressed_recovery_id(
            label="recovery-target-manifest",
            producer_commit=producer_commit,
            location=arguments.artifact_location_id,
        ),
        artifact_retention_full_recovery_receipt_id=content_addressed_recovery_id(
            label="full-recovery-receipt",
            producer_commit=producer_commit,
            location=arguments.artifact_location_id,
        ),
        recovery=recovery,
        canvas_publication_active_key_id=canvas_key_id,
        canvas_publication_active_public_key_pem=canvas_public_key,
        shadow_completion_active_key_id=completion_key_id,
        shadow_completion_active_public_key_pem=completion_public_key,
        shadow_report_active_key_id=report_key_id,
        shadow_report_active_public_key_pem=report_public_key,
    )
    document = canonical_json_bytes(payload)
    strict_canonical_json_loads(document)
    inputs_sha256 = write_private_file(inputs_output, document, mode=0o600)

    print(f"producer_commit {producer_commit}")
    print(f"inputs {inputs_output} sha256={inputs_sha256}")
    print(f"market_calendar {calendar_path} content_sha256={calendar.content_sha256}")
    print(f"  coverage {calendar.coverage_start.isoformat()}..{calendar.coverage_end.isoformat()}")
    print(
        f"  open_dates {len(calendar.open_dates)} generated_at {calendar.generated_at.isoformat()}"
    )
    print(f"trade_calendar {trade_calendar_path} sha256={trade_calendar_sha256}")
    print(f"routing_policy {routing_policy_path} fingerprint={routing_policy_fingerprint}")
    print(f"retention_schema_authority {retention_path} sha256={retention_sha256}")
    print(f"sealed_candidates trade_date={trade_date.isoformat()} n_shape={n_shape_path}")
    print(f"sealed_candidates growth_board_surge={growth_board_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
