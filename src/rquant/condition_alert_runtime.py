"""Original screen conditions and atomic state in the original alert ledger."""

from __future__ import annotations

import sqlite3
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StrictInt, StrictStr, model_validator

from rquant.alert_rule_contracts import ConditionAlertScopeEvidence, OwnedConditionAlertRule
from rquant.condition_alert_runtime_contracts import (
    ConditionAlertEventEnvelope,
    ConditionAlertProducerEventRecord,
    ConditionAlertRuntimeActivation,
    ConditionAlertSourceDescriptor,
    ConditionRuntimeModel,
    parse_condition_alert_event,
    require_condition_alert_activation,
)
from rquant.manual_watchlist import OwnerId, TsCode
from rquant.price_alert_runtime_contracts import PriceAlertCapacityExceeded, PriceSha256, utc_text
from rquant.price_alert_runtime_store import PriceAlertRuntimeStore
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256, normalize_aware_utc

if TYPE_CHECKING:
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.screen.dynamic_rsi import VerifiedDynamicRsiProjection
    from rquant.screen.replica_source import VerifiedReplicaScreenSource
    from rquant.web.serving import BorrowedGeneration

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class ConditionFrequencyPolicy(ConditionRuntimeModel):
    protocol: Literal["condition-frequency/v1"] = "condition-frequency/v1"
    truth_policy: Literal["unknown_breaks_recovery"] = "unknown_breaks_recovery"
    event_kinds: tuple[Literal["matched", "recovered"], ...] = ("matched", "recovered")


class ConditionSourceFacts(ConditionRuntimeModel):
    source_identity: PriceSha256
    raw_batch_id: PriceSha256
    feature_snapshot_id: PriceSha256
    daily_anchor_date: date
    trade_date: date
    cutoff: AwareUtcDatetime
    feature_contract_version: Literal[3, 4]
    universe_codes: tuple[TsCode, ...] = Field(min_length=1, max_length=8000)
    missing_codes: tuple[TsCode, ...] = ()

    @model_validator(mode="after")
    def closed_anchor(self) -> Self:
        if (
            self.daily_anchor_date >= self.trade_date
            or self.cutoff.astimezone(_SHANGHAI).date() != self.trade_date
        ):
            raise ValueError("condition input has a future daily anchor or another session")
        if self.missing_codes != tuple(sorted(set(self.missing_codes))) or set(
            self.missing_codes
        ) - set(self.universe_codes):
            raise ValueError("condition source coverage differs")
        if self.universe_codes != tuple(sorted(set(self.universe_codes))):
            raise ValueError("condition input universe is not complete and unique")
        return self


class ConditionRoundRule(ConditionRuntimeModel):
    owned: OwnedConditionAlertRule
    scope: ConditionAlertScopeEvidence | None
    ranking_digest: PriceSha256 | None = None

    @model_validator(mode="after")
    def owned_scope(self) -> Self:
        if self.scope is not None and (self.scope.owner_id, self.scope.scope) != (
            self.owned.owner_id,
            self.owned.rule.scope,
        ):
            raise ValueError("condition runtime scope differs from the actual owner and rule")
        return self


class ConditionEvaluationRecord(ConditionRuntimeModel):
    owner_id: OwnerId
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    ts_code: TsCode | None
    stock_name: StrictStr | None = Field(default=None, max_length=80)
    truth: Literal["true", "false", "unknown"]
    reason: StrictStr = Field(min_length=1, max_length=80)
    event_time: AwareUtcDatetime | None


class ConditionRoundInput(ConditionRuntimeModel):
    evaluated_at: AwareUtcDatetime
    serving_generation_id: PriceSha256 | None
    serving_manifest_sha256: PriceSha256 | None
    source: ConditionSourceFacts | None
    rules: tuple[ConditionRoundRule, ...] = Field(max_length=3200)
    records: tuple[ConditionEvaluationRecord, ...] = Field(max_length=800000)

    @model_validator(mode="after")
    def complete_domain(self) -> Self:
        bindings = {(r.owned.owner_id, r.owned.rule.rule_id): r for r in self.rules}
        keys = tuple(bindings)
        if len(keys) != len(self.rules) or keys != tuple(sorted(keys)):
            raise ValueError("condition round needs all sorted unique owned rules")
        counts = Counter(owner for owner, _ in keys)
        if len(counts) > 32 or any(value > 100 for value in counts.values()):
            raise PriceAlertCapacityExceeded("condition rule domain exceeds its owner capacity")
        record_keys = tuple((r.owner_id, r.rule_id, r.ts_code or "") for r in self.records)
        if record_keys != tuple(sorted(set(record_keys))) or {
            key[:2] for key in record_keys
        } != set(keys):
            raise ValueError("condition decisions omit a rule or repeat a member")
        if self.source is not None and (
            self.source.cutoff > self.evaluated_at
            or self.evaluated_at - self.source.cutoff > timedelta(seconds=90)
        ):
            raise ValueError("condition input cutoff is future or stale")
        for record in self.records:
            bound = bindings[record.owner_id, record.rule_id]
            scope = bound.scope
            if bound.owned.updated_at > self.evaluated_at or (
                scope is not None and scope.available_at > self.evaluated_at
            ):
                raise ValueError("condition rule or membership was not visible")
            if record.truth != "unknown" and (
                self.source is None
                or self.serving_generation_id is None
                or self.serving_manifest_sha256 is None
                or scope is None
                or record.ts_code in self.source.missing_codes
                or record.ts_code not in scope.member_codes
                or record.ts_code not in self.source.universe_codes
                or record.event_time is None
                or record.event_time > self.source.cutoff
                or self.source.feature_contract_version
                < bound.owned.rule.source_policy.minimum_intraday_contract_version
            ):
                raise ValueError("confirmed condition truth lacks its actual member or source")
        for key, bound in bindings.items():
            codes = tuple(
                record.ts_code
                for record in self.records
                if (record.owner_id, record.rule_id) == key
            )
            expected_codes = (
                (None,)
                if bound.scope is None or not bound.scope.member_codes
                else bound.scope.member_codes
            )
            if codes != expected_codes:
                raise ValueError("condition evaluation omits actual scope members")
        if len(self.wire_bytes()) > 64 * 1024 * 1024:
            raise PriceAlertCapacityExceeded("condition round exceeds the full input capacity")
        return self


class ConditionRoundReceipt(ConditionRuntimeModel):
    round_id: PriceSha256
    input_sha256: PriceSha256
    evaluated_at: AwareUtcDatetime
    source_high_watermark: StrictInt = Field(ge=0)
    decision_count: StrictInt = Field(ge=0)
    unknown_count: StrictInt = Field(ge=0)
    suppressed_count: StrictInt = Field(ge=0)
    events: tuple[ConditionAlertProducerEventRecord, ...]
    input: ConditionRoundInput

    @model_validator(mode="after")
    def complete_receipt(self) -> Self:
        if (
            self.round_id != self.input.sha256
            or self.input_sha256 != self.input.sha256
            or self.evaluated_at != self.input.evaluated_at
            or self.decision_count != len(self.input.records)
            or self.unknown_count != sum(r.truth == "unknown" for r in self.input.records)
        ):
            raise ValueError("condition receipt differs from its full actual input")
        seq = tuple(event.sequence for event in self.events)
        if seq and (seq != tuple(range(seq[0], self.source_high_watermark + 1)) or seq[0] < 1):
            raise ValueError("condition receipt event prefix differs")
        keys = {(b.owned.owner_id, b.owned.rule.rule_id): b for b in self.input.rules}
        for item in self.events:
            event = item.event
            bound = keys.get((event.owner_id, event.rule_id))
            if (
                bound is None
                or bound.scope is None
                or (
                    event.rule_version,
                    event.rule_body_hash,
                    event.scope_version,
                    event.member_digest,
                )
                != (
                    bound.owned.version,
                    bound.owned.rule.rule_body_hash,
                    bound.scope.scope_version,
                    bound.scope.member_digest,
                )
                or event.decision_time != self.evaluated_at
            ):
                raise ValueError("condition event differs from its actual rule decision")
        return self


CONDITION_RUNTIME_TABLES = frozenset(
    {
        "condition_alert_runtime_identity",
        "condition_alert_truth_state",
        "condition_alert_frequency_state",
        "condition_alert_evaluation_head",
        "condition_alert_round_receipt",
        "condition_alert_event_log",
    }
)
CONDITION_RUNTIME_SQL = (
    "CREATE TABLE condition_alert_runtime_identity(key TEXT PRIMARY K"
    "EY,body BLOB NOT NULL,high_watermark INTEGER NOT NULL,last_evalu"
    "ated_at TEXT,latest_round_id TEXT)",
    "CREATE TABLE condition_alert_truth_state(owner_id TEXT,rule_id T"
    "EXT,scope_version TEXT,ts_code TEXT,rule_version INTEGER,rule_bo"
    "dy_hash TEXT,truth TEXT NOT NULL,observed_at TEXT NOT NULL,PRIMA"
    "RY KEY(owner_id,rule_id,scope_version,ts_code))",
    "CREATE TABLE condition_alert_frequency_state(owner_id TEXT,rule_"
    "id TEXT,rule_version INTEGER,scope_version TEXT,ts_code TEXT,pol"
    "icy_sha256 TEXT,event_kind TEXT,bucket TEXT NOT NULL,next_allowe"
    "d_at TEXT NOT NULL,last_event_time TEXT NOT NULL,PRIMARY KEY(own"
    "er_id,rule_id,rule_version,scope_version,ts_code,policy_sha256,e"
    "vent_kind))",
    "CREATE TABLE condition_alert_evaluation_head(owner_id TEXT,rule_"
    "id TEXT,ts_code TEXT,body BLOB NOT NULL,evaluated_at TEXT NOT NU"
    "LL,round_id TEXT NOT NULL,PRIMARY KEY(owner_id,rule_id,ts_code))",
    "CREATE TABLE condition_alert_round_receipt(round_id TEXT PRIMARY"
    " KEY,input_sha256 TEXT NOT NULL,body BLOB NOT NULL,evaluated_at "
    "TEXT NOT NULL)",
    "CREATE TABLE condition_alert_event_log(sequence INTEGER PRIMARY "
    "KEY,event_id TEXT UNIQUE NOT NULL,payload BLOB NOT NULL,payload_"
    "sha256 TEXT NOT NULL)",
)


def verify_condition_runtime_namespace(
    connection: sqlite3.Connection,
) -> ConditionAlertSourceDescriptor:
    schema = {
        row[0]
        for row in connection.execute(
            "SELECT sql FROM sqlite_master WHERE name LIKE 'condition_alert_%' AND sql IS NOT NULL"
        )
    }
    if schema != set(CONDITION_RUNTIME_SQL):
        raise ValueError("condition runtime namespace is not explicitly installed")
    row = connection.execute(
        "SELECT body,high_watermark FROM condition_alert_runtime_identity WHERE key='current'"
    ).fetchone()
    if row is None:
        raise ValueError("condition runtime has no installed source identity")
    source = ConditionAlertSourceDescriptor.model_validate_json(bytes(row[0]))
    if source.wire_bytes() != bytes(row[0]) or source.high_watermark != 0:
        raise ValueError("condition runtime installed identity changed")
    maximum, count = connection.execute(
        "SELECT COALESCE(MAX(sequence),0),COUNT(*) FROM condition_alert_event_log"
    ).fetchone()
    if maximum != count or row[1] != maximum:
        raise ValueError("condition runtime source has a gap or regressed")
    return ConditionAlertSourceDescriptor(
        **source.model_dump(exclude={"high_watermark"}), high_watermark=maximum
    )


class ConditionAlertRuntimeStore:
    def __init__(
        self, ledger: PriceAlertRuntimeStore, *, activation: ConditionAlertRuntimeActivation
    ) -> None:
        from rquant.price_alert_runtime_store import ReadonlyPriceAlertRuntimeStore

        if type(ledger) not in {PriceAlertRuntimeStore, ReadonlyPriceAlertRuntimeStore}:
            raise TypeError("condition state must borrow the original alert ledger")
        self.ledger = ledger
        self.activation = activation
        self.binding = require_condition_alert_activation(activation, "evaluation")
        self.failpoint: Callable[[str], None] = lambda _: None
        if self.binding.frequency_policy_sha256 != ConditionFrequencyPolicy().sha256:
            raise ValueError("condition frequency contract differs from its installed role")
        with ledger._connection() as connection:
            source = verify_condition_runtime_namespace(connection)
        expected = {
            key: getattr(self.binding, key)
            for key in ConditionAlertSourceDescriptor.model_fields
            if key not in {"first_sequence", "high_watermark"}
        }
        if source.model_dump(exclude={"first_sequence", "high_watermark"}) != expected:
            raise ValueError("condition runtime binding differs from its installed source")

    @classmethod
    def install(
        cls, ledger: PriceAlertRuntimeStore, *, activation: ConditionAlertRuntimeActivation
    ) -> ConditionAlertRuntimeStore:
        if type(ledger) is not PriceAlertRuntimeStore:
            raise TypeError("condition installation requires the original single writer")
        binding = require_condition_alert_activation(activation, "evaluation")
        source = ConditionAlertSourceDescriptor(
            **{
                key: getattr(binding, key)
                for key in ConditionAlertSourceDescriptor.model_fields
                if key not in {"first_sequence", "high_watermark"}
            },
            high_watermark=0,
        )
        with ledger._connection(write=True) as connection:
            present = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE "
                    "'condition_alert_%'"
                )
            }
            if not present:
                for ddl in CONDITION_RUNTIME_SQL:
                    connection.execute(ddl)
                connection.execute(
                    "INSERT INTO condition_alert_runtime_identity VALUES(?,?,0,NULL,NULL)",
                    ("current", source.wire_bytes()),
                )
            verify_condition_runtime_namespace(connection)
        return cls(ledger, activation=activation)

    def source_descriptor(self) -> ConditionAlertSourceDescriptor:
        with self.ledger._connection() as connection:
            return verify_condition_runtime_namespace(connection)

    @staticmethod
    def _event(row: sqlite3.Row) -> ConditionAlertProducerEventRecord:
        payload = bytes(row["payload"])
        return ConditionAlertProducerEventRecord(
            sequence=row["sequence"],
            event=parse_condition_alert_event(payload),
            payload_json=payload.decode(),
            payload_sha256=row["payload_sha256"],
        )

    def events_after(
        self, after: int, *, inspected_at: datetime, limit: int = 100
    ) -> tuple[ConditionAlertProducerEventRecord, ...]:
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("condition event read exceeds the original route budget")
        with self.ledger._connection() as connection:
            source = verify_condition_runtime_namespace(connection)
            if after > source.high_watermark:
                raise ValueError("condition source regressed")
            rows = connection.execute(
                "SELECT * FROM condition_alert_event_log WHERE sequence>? ORDER B"
                "Y sequence LIMIT ?",
                (after, limit),
            ).fetchall()
            events = tuple(self._event(row) for row in rows)
            if any(
                event.sequence != sequence or event.event.available_at > inspected_at
                for sequence, event in enumerate(events, after + 1)
            ):
                raise ValueError("condition event prefix is not visible or contiguous")
            return events

    def latest_round(self) -> ConditionRoundReceipt | None:
        with self.ledger._connection() as connection:
            row = connection.execute(
                "SELECT r.body FROM condition_alert_runtime_identity i JOIN condi"
                "tion_alert_round_receipt r ON r.round_id=i.latest_round_id WHERE"
                " i.key='current'"
            ).fetchone()
            return None if row is None else ConditionRoundReceipt.model_validate_json(row[0])

    def record_unavailable(self, *, evaluated_at: datetime, reason: str) -> ConditionRoundReceipt:
        last = self.latest_round()
        rules = (
            ()
            if last is None
            else tuple(ConditionRoundRule(owned=b.owned, scope=None) for b in last.input.rules)
        )
        records = tuple(
            ConditionEvaluationRecord(
                owner_id=b.owned.owner_id,
                rule_id=b.owned.rule.rule_id,
                ts_code=None,
                truth="unknown",
                reason=reason,
                event_time=None,
            )
            for b in rules
        )
        value = ConditionRoundInput(
            evaluated_at=evaluated_at,
            serving_generation_id=None if last is None else last.input.serving_generation_id,
            serving_manifest_sha256=None if last is None else last.input.serving_manifest_sha256,
            source=None,
            rules=rules,
            records=records,
        )
        return self.commit_round(value)

    def commit_round(
        self, round_input: ConditionRoundInput, *, current_scope: Callable[[], bool] | None = None
    ) -> ConditionRoundReceipt:
        binding = require_condition_alert_activation(self.activation, "evaluation")
        if type(round_input) is not ConditionRoundInput:
            raise TypeError("condition round requires exact typed server facts")
        round_input = ConditionRoundInput.model_validate_json(round_input.wire_bytes())
        digest = round_input.sha256
        now = round_input.evaluated_at
        bound_rules = {(r.owned.owner_id, r.owned.rule.rule_id): r for r in round_input.rules}
        events: list[ConditionAlertProducerEventRecord] = []
        suppressed = 0
        with self.ledger._connection(write=True) as connection:
            source = verify_condition_runtime_namespace(connection)
            prior = connection.execute(
                "SELECT body FROM condition_alert_round_receipt WHERE round_id=?", (digest,)
            ).fetchone()
            if prior is not None:
                return ConditionRoundReceipt.model_validate_json(prior[0])
            clock = connection.execute(
                "SELECT last_evaluated_at FROM condition_alert_runtime_identity WHERE key='current'"
            ).fetchone()[0]
            if clock is not None and now < datetime.fromisoformat(clock):
                raise ValueError("condition evaluation clock regressed")
            if (
                self.ledger.path.stat().st_size + len(round_input.wire_bytes()) * 3
                > 512 * 1024 * 1024
            ):
                raise PriceAlertCapacityExceeded("original alert ledger capacity is exhausted")
            high = source.high_watermark
            old_round = connection.execute(
                "SELECT r.body FROM condition_alert_runtime_identity i JOIN condi"
                "tion_alert_round_receipt r ON r.round_id=i.latest_round_id WHERE"
                " i.key='current'"
            ).fetchone()
            prior_scopes = (
                {}
                if old_round is None
                else {
                    (row.owned.owner_id, row.owned.rule.rule_id): row.scope
                    for row in ConditionRoundReceipt.model_validate_json(old_round[0]).input.rules
                }
            )
            for decision in round_input.records:
                bound = bound_rules[decision.owner_id, decision.rule_id]
                owned, scope, rule = bound.owned, bound.scope, bound.owned.rule
                code = decision.ts_code or ""
                truth_key = (
                    owned.owner_id,
                    rule.rule_id,
                    "" if scope is None else scope.scope_version,
                    code,
                )
                prior_truth = connection.execute(
                    "SELECT rule_version,rule_body_hash,truth FROM condition_alert_tr"
                    "uth_state WHERE owner_id=? AND rule_id=? AND scope_version=? AND"
                    " ts_code=?",
                    truth_key,
                ).fetchone()
                old_scope = prior_scopes.get((owned.owner_id, rule.rule_id))
                previous = (
                    "unknown"
                    if prior_truth is None
                    or old_scope is None
                    or scope is None
                    or old_scope.scope_version != scope.scope_version
                    or (prior_truth[0], prior_truth[1]) != (owned.version, rule.rule_body_hash)
                    else prior_truth[2]
                )
                if code == "" or scope is None:
                    connection.execute(
                        "UPDATE condition_alert_truth_state SET truth='unknown',observed_"
                        "at=? WHERE owner_id=? AND rule_id=?",
                        (utc_text(now), owned.owner_id, rule.rule_id),
                    )
                else:
                    connection.execute(
                        "INSERT INTO condition_alert_truth_state VALUES(?,?,?,?,?,?,?,?) "
                        "ON CONFLICT(owner_id,rule_id,scope_version,ts_code) DO UPDATE SE"
                        "T rule_version=excluded.rule_version,rule_body_hash=excluded.rul"
                        "e_body_hash,truth=excluded.truth,observed_at=excluded.observed_a"
                        "t",
                        (
                            *truth_key,
                            owned.version,
                            rule.rule_body_hash,
                            decision.truth,
                            utc_text(now),
                        ),
                    )
                self.failpoint("truth")
                connection.execute(
                    "INSERT INTO condition_alert_evaluation_head VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(owner_id,rule_id,ts_code) DO UPDATE SET body=exclude"
                    "d.body,evaluated_at=excluded.evaluated_at,round_id=excluded.roun"
                    "d_id",
                    (
                        owned.owner_id,
                        rule.rule_id,
                        code,
                        decision.wire_bytes(),
                        utc_text(now),
                        digest,
                    ),
                )
                local = now.astimezone(_SHANGHAI)
                inside = any(
                    window.start <= local.time().replace(tzinfo=None) < window.end
                    for window in rule.trading_hours.windows
                )
                kind = (
                    "matched"
                    if decision.truth == "true"
                    else "recovered"
                    if decision.truth == "false"
                    and previous == "true"
                    and rule.governance.notify_recovery
                    else None
                )
                if (
                    kind is None
                    or not rule.enabled
                    or not inside
                    or round_input.source is None
                    or scope is None
                ):
                    continue
                if decision.stock_name is None or decision.event_time is None:
                    suppressed += 1
                    continue
                frequency = rule.frequency
                policy_sha = canonical_sha256(
                    {"frequency": frequency, "governance": rule.governance}
                )
                bucket = (
                    utc_text(decision.event_time.replace(second=0, microsecond=0))
                    if frequency.kind == "bar_close"
                    else digest
                )
                cooldown = (
                    frequency.minutes * 60 if frequency.kind == "per_symbol_minutes" else 0
                )
                clock = decision.event_time if frequency.kind == "per_symbol_minutes" else now
                freq_key = (
                    owned.owner_id,
                    rule.rule_id,
                    owned.version,
                    scope.scope_version,
                    code,
                    policy_sha,
                    kind,
                )
                frequency_state = connection.execute(
                    "SELECT bucket,next_allowed_at,last_event_time FROM condition_ale"
                    "rt_frequency_state WHERE owner_id=? AND rule_id=? AND rule_versi"
                    "on=? AND scope_version=? AND ts_code=? AND policy_sha256=? AND e"
                    "vent_kind=?",
                    freq_key,
                ).fetchone()
                if frequency_state is not None and (
                    frequency_state[0] == bucket
                    or clock
                    < (
                        datetime.fromisoformat(frequency_state[2]) + timedelta(seconds=cooldown)
                        if frequency.kind == "per_symbol_minutes"
                        else datetime.fromisoformat(frequency_state[1])
                    )
                    or decision.event_time - datetime.fromisoformat(frequency_state[2])
                    < timedelta(seconds=rule.governance.dedup_window_seconds)
                ):
                    suppressed += 1
                    continue
                try:
                    require_condition_alert_activation(self.activation, "event_write")
                except ValueError:
                    suppressed += 1
                    continue
                if high >= 100000:
                    raise PriceAlertCapacityExceeded("condition event ledger is full")
                expires = min(
                    now + timedelta(minutes=2),
                    *(
                        datetime.combine(local.date(), window.end, tzinfo=_SHANGHAI)
                        for window in rule.trading_hours.windows
                        if window.start <= local.time().replace(tzinfo=None) < window.end
                    ),
                )
                event = ConditionAlertEventEnvelope.create(
                    owner_id=owned.owner_id,
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    priority=rule.priority,
                    channels=rule.governance.channels,
                    rule_version=owned.version,
                    rule_body_hash=rule.rule_body_hash,
                    scope_version=scope.scope_version,
                    member_digest=scope.member_digest,
                    ts_code=code,
                    stock_name=decision.stock_name,
                    trigger_kind=kind,
                    previous_truth=previous,
                    truth=decision.truth,
                    source_identity=round_input.source.source_identity,
                    raw_batch_id=round_input.source.raw_batch_id,
                    feature_snapshot_id=round_input.source.feature_snapshot_id,
                    daily_anchor_date=round_input.source.daily_anchor_date,
                    event_time=decision.event_time,
                    decision_time=now,
                    available_at=now,
                    expires_at=expires,
                    evaluation_contract_sha256=binding.evaluation_contract_sha256,
                    frequency_policy_sha256=binding.frequency_policy_sha256,
                    frequency_bucket=bucket,
                    producer_manifest_sha256=binding.producer_manifest_sha256,
                    producer_commit=binding.producer_commit,
                    source_epoch=binding.source_epoch,
                )
                connection.execute(
                    "INSERT INTO condition_alert_frequency_state VALUES(?,?,?,?,?,?,?"
                    ",?,?,?) ON CONFLICT(owner_id,rule_id,rule_version,scope_version,"
                    "ts_code,policy_sha256,event_kind) DO UPDATE SET bucket=excluded."
                    "bucket,next_allowed_at=excluded.next_allowed_at,last_event_time="
                    "excluded.last_event_time",
                    (
                        *freq_key,
                        bucket,
                        utc_text(clock + timedelta(seconds=cooldown)),
                        utc_text(decision.event_time),
                    ),
                )
                self.failpoint("cooldown")
                high += 1
                connection.execute(
                    "INSERT INTO condition_alert_event_log VALUES(?,?,?,?)",
                    (high, event.event_id, event.wire_bytes(), event.sha256),
                )
                self.failpoint("event")
                events.append(
                    ConditionAlertProducerEventRecord(
                        sequence=high,
                        event=event,
                        payload_json=event.wire_bytes().decode(),
                        payload_sha256=event.sha256,
                    )
                )
            receipt = ConditionRoundReceipt(
                round_id=digest,
                input_sha256=digest,
                evaluated_at=now,
                source_high_watermark=high,
                decision_count=len(round_input.records),
                unknown_count=sum(record.truth == "unknown" for record in round_input.records),
                suppressed_count=suppressed,
                events=tuple(events),
                input=round_input,
            )
            connection.execute(
                "INSERT INTO condition_alert_round_receipt VALUES(?,?,?,?)",
                (digest, digest, receipt.wire_bytes(), utc_text(now)),
            )
            self.failpoint("receipt")
            connection.execute(
                "UPDATE condition_alert_runtime_identity SET high_watermark=?,las"
                "t_evaluated_at=?,latest_round_id=? WHERE key='current'",
                (high, utc_text(now), digest),
            )
            if current_scope is not None and current_scope() is not True:
                raise ValueError("condition rule or source changed before commit")
            self.failpoint("before_commit")
        return receipt


def evaluate_condition_alert_round(
    *,
    activation: ConditionAlertRuntimeActivation,
    borrowed: BorrowedGeneration,
    rules: tuple[OwnedConditionAlertRule, ...],
    scopes: tuple[ConditionAlertScopeEvidence | None, ...],
    calendar: MarketCalendarAuthority | None,
    evaluated_at: datetime,
    replica: VerifiedReplicaScreenSource | None = None,
    rsi: VerifiedDynamicRsiProjection | None = None,
) -> ConditionRoundInput:
    import math

    import pandas as pd

    from rquant.llm.registry import get_rule_spec
    from rquant.screen.core import _collect_aggregates, rule_state
    from rquant.screen.ranking import RankingCondition, rank_screen_results
    from rquant.screen.rules import required_rule_columns
    from rquant.web.screen_intraday import (
        IntradayScreenUnavailableError,
        intraday_screen_context,
        prepare_intraday_screen_frame,
    )

    require_condition_alert_activation(activation, "evaluation")
    now = normalize_aware_utc(evaluated_at)
    if (
        borrowed.pointer is None
        or borrowed.pointer.generation_id != borrowed.manifest.generation_id
        or borrowed.fallback_detail is not None
        or borrowed.manifest.built_at > now
    ):
        raise ValueError("condition evaluation has no pinned current generation")
    if len(rules) != len(scopes):
        raise ValueError("condition rules and scopes must be paired")
    context = None
    with suppress(IntradayScreenUnavailableError, ValueError, RuntimeError):
        context = intraday_screen_context(borrowed, now=now, replica=replica, rsi=rsi)
    actual = None if context is None else context.snapshot.source
    open_session = (
        calendar is not None
        and calendar.generated_at <= now
        and calendar.coverage_start <= now.astimezone(_SHANGHAI).date() <= calendar.coverage_end
        and now.astimezone(_SHANGHAI).date() in calendar.open_dates
    )
    if actual is not None and calendar is not None:
        previous = tuple(day for day in calendar.open_dates if day < actual.trade_date)
        open_session = open_session and bool(previous) and previous[-1] == actual.daily_anchor_date
    records: list[ConditionEvaluationRecord] = []
    bindings: list[ConditionRoundRule] = []
    for owned, scope in zip(rules, scopes, strict=True):
        owned = OwnedConditionAlertRule.model_validate_json(owned.model_dump_json())
        if scope is not None:
            if type(scope) is not ConditionAlertScopeEvidence:
                raise TypeError("condition scope must be exact server evidence")
            scope = ConditionAlertScopeEvidence.model_validate_json(scope.model_dump_json())
            if (
                scope.owner_id != owned.owner_id
                or scope.scope != owned.rule.scope
                or scope.available_at > now
            ):
                raise ValueError("condition scope owner or visibility differs")
        binding = ConditionRoundRule(owned=owned, scope=scope)
        rule = owned.rule
        codes = (None,) if scope is None or not scope.member_codes else scope.member_codes
        local = now.astimezone(_SHANGHAI).time().replace(tzinfo=None)
        eligible = (
            rule.enabled
            and open_session
            and any(window.start <= local < window.end for window in rule.trading_hours.windows)
        )
        reason = (
            "disabled"
            if not rule.enabled
            else "market_session_unknown"
            if not open_session
            else "outside_window"
            if not eligible
            else "scope_unavailable"
            if scope is None
            else "scope_empty"
            if not scope.member_codes
            else "source_unavailable"
        )
        if (
            actual is None
            or not eligible
            or scope is None
            or not scope.member_codes
            or actual.feature_contract_version
            < rule.source_policy.minimum_intraday_contract_version
        ):
            records.extend(
                ConditionEvaluationRecord(
                    owner_id=owned.owner_id,
                    rule_id=rule.rule_id,
                    ts_code=code,
                    truth="unknown",
                    reason=reason,
                    event_time=None,
                )
                for code in codes
            )
            bindings.append(binding)
            continue
        compiled = [
            get_rule_spec(call.name).fn(
                **get_rule_spec(call.name).args_model.model_validate(call.args).model_dump()
            )
            for call in rule.conditions
        ]
        rank_columns = (
            [] if rule.ranking is None else [item.metric for item in rule.ranking.conditions]
        )
        prepared = prepare_intraday_screen_frame(
            context,
            borrowed=borrowed,
            rules=compiled,
            rank_columns=rank_columns,
            replica=replica,
            rsi=rsi,
            closed_bar=rule.frequency.kind == "bar_close",
        )
        frame = (
            prepared.frame[prepared.frame.ts_code.isin(scope.member_codes)]
            .copy()
            .reset_index(drop=True)
        )
        safe, _ = rule_state(frame, compiled)
        matched = pd.Series(True, index=frame.index, dtype="boolean")
        for call in safe:
            matched &= call(frame)
        dependencies = required_rule_columns(compiled) | {
            request.name for request in _collect_aggregates(compiled)
        }
        known = frame.loc[:, sorted(dependencies)].notna().all(axis=1)
        selected_codes = set(frame.loc[matched, "ts_code"])
        if rule.ranking is not None:
            try:
                positive = [item.metric for item in rule.ranking.conditions if item.weight > 0]
                finite = (
                    frame.loc[:, positive]
                    .apply(
                        lambda column: column.map(
                            lambda value: type(value) in {int, float} and math.isfinite(value)
                        )
                    )
                    .all(axis=1)
                )
                candidates = frame.loc[matched].copy()
                ranked = rank_screen_results(
                    candidates,
                    [
                        RankingCondition(
                            column=item.metric, ascending=item.ascending, weight=item.weight
                        )
                        for item in rule.ranking.conditions
                    ],
                    top_n=rule.ranking.top_n,
                )
                selected_codes = set(ranked.ts_code)
                if not finite.loc[matched].all():
                    known.loc[matched] = False
                binding = ConditionRoundRule(
                    owned=owned,
                    scope=scope,
                    ranking_digest=canonical_sha256(
                        {
                            "ranking": rule.ranking,
                            "ordered_members": tuple(
                                (str(row.ts_code), float(row.ranking_score))
                                for row in ranked.itertuples()
                            ),
                        }
                    ),
                )
            except (ValueError, KeyError, TypeError):
                known.loc[matched] = False
                selected_codes = set()
        stocks = {stock.ts_code: stock for stock in context.snapshot.stocks}
        for code in codes:
            indexed = frame[frame.ts_code == code]
            stock = stocks.get(code)
            if (
                indexed.empty
                or stock is None
                or not bool(known.loc[indexed.index[0]])
                or stock.feature_time is None
                or (rule.frequency.kind == "bar_close" and stock.closed_bar is None)
            ):
                truth, why, event_time = "unknown", "source_missing", None
            else:
                truth, why, event_time = (
                    ("true", "conditions_matched", stock.feature_time)
                    if code in selected_codes
                    else ("false", "conditions_not_matched", stock.feature_time)
                )
                if rule.frequency.kind != "bar_close":
                    visible = [
                        fact.source_event_time
                        for fact in stock.fields
                        if fact.name in dependencies and fact.source_event_time is not None
                    ]
                    event_time = max([event_time, *visible])
            records.append(
                ConditionEvaluationRecord(
                    owner_id=owned.owner_id,
                    rule_id=rule.rule_id,
                    ts_code=code,
                    stock_name=code if stock is None else stock.name or code,
                    truth=truth,
                    reason=why,
                    event_time=event_time,
                )
            )
        bindings.append(binding)
    source_facts = (
        None
        if actual is None
        else ConditionSourceFacts(
            source_identity=actual.source_identity,
            raw_batch_id=actual.raw_prefix_digest,
            feature_snapshot_id=actual.feature_payload_sha256,
            daily_anchor_date=actual.daily_anchor_date,
            trade_date=actual.trade_date,
            cutoff=actual.cutoff,
            feature_contract_version=actual.feature_contract_version,
            universe_codes=actual.universe_codes,
            missing_codes=actual.missing_codes,
        )
    )
    return ConditionRoundInput(
        evaluated_at=now,
        serving_generation_id=borrowed.manifest.generation_id,
        serving_manifest_sha256=borrowed.pointer.manifest_sha256,
        source=source_facts,
        rules=tuple(sorted(bindings, key=lambda row: (row.owned.owner_id, row.owned.rule.rule_id))),
        records=tuple(
            sorted(records, key=lambda row: (row.owner_id, row.rule_id, row.ts_code or ""))
        ),
    )


class ConditionRuntimeRuleFact(ConditionRuntimeModel):
    owner_id: OwnerId
    rule_id: str
    rule_version: int
    rule_body_hash: PriceSha256
    scope_version: PriceSha256 | None
    evaluated_at: AwareUtcDatetime
    matched_count: int | None
    unknown_count: int
    ranking_digest: PriceSha256 | None
    status_label: Literal["正常", "注意", "异常", "未运行", "等待开盘"]
    last_triggered_at: AwareUtcDatetime | None


class ConditionProducerRuntimeSnapshot(ConditionRuntimeModel):
    source: ConditionAlertSourceDescriptor
    inspected_at: AwareUtcDatetime
    evaluated_at: AwareUtcDatetime | None
    input_sha256: PriceSha256 | None
    source_facts: ConditionSourceFacts | None
    rules: tuple[ConditionRuntimeRuleFact, ...] = Field(max_length=3200)


def condition_producer_snapshot(
    store: ConditionAlertRuntimeStore, *, observed_at: datetime
) -> ConditionProducerRuntimeSnapshot:
    now = normalize_aware_utc(observed_at)
    with store.ledger._connection() as connection:
        source = verify_condition_runtime_namespace(connection)
        row = connection.execute(
            "SELECT r.body FROM condition_alert_runtime_identity i JOIN condi"
            "tion_alert_round_receipt r ON r.round_id=i.latest_round_id WHERE"
            " i.key='current'"
        ).fetchone()
        last = None if row is None else ConditionRoundReceipt.model_validate_json(bytes(row[0]))
        if last is not None and last.evaluated_at > now:
            raise ValueError("condition producer snapshot is not visible")
        facts = []
        if last is not None:
            for bound in last.input.rules:
                records = tuple(
                    r
                    for r in last.input.records
                    if (r.owner_id, r.rule_id) == (bound.owned.owner_id, bound.owned.rule.rule_id)
                )
                unknown = sum(r.truth == "unknown" for r in records)
                matched = sum(r.truth == "true" for r in records)
                latest = connection.execute(
                    "SELECT payload FROM condition_alert_event_log WHERE json_extract"
                    "(CAST(payload AS TEXT),'$.owner_id')=? AND json_extract(CAST(pay"
                    "load AS TEXT),'$.rule_id')=? ORDER BY sequence DESC LIMIT 1",
                    (bound.owned.owner_id, bound.owned.rule.rule_id),
                ).fetchone()
                event = None if latest is None else parse_condition_alert_event(bytes(latest[0]))
                status = (
                    "未运行"
                    if not bound.owned.rule.enabled
                    else "等待开盘"
                    if records and all(r.reason == "outside_window" for r in records)
                    else "注意"
                    if unknown or matched
                    else "正常"
                )
                facts.append(
                    ConditionRuntimeRuleFact(
                        owner_id=bound.owned.owner_id,
                        rule_id=bound.owned.rule.rule_id,
                        rule_version=bound.owned.version,
                        rule_body_hash=bound.owned.rule.rule_body_hash,
                        scope_version=None if bound.scope is None else bound.scope.scope_version,
                        evaluated_at=last.evaluated_at,
                        matched_count=None if unknown else matched,
                        unknown_count=unknown,
                        ranking_digest=bound.ranking_digest,
                        status_label=status,
                        last_triggered_at=None if event is None else event.available_at,
                    )
                )
        return ConditionProducerRuntimeSnapshot(
            source=source,
            inspected_at=now,
            evaluated_at=None if last is None else last.evaluated_at,
            input_sha256=None if last is None else last.input_sha256,
            source_facts=None if last is None else last.input.source,
            rules=tuple(facts),
        )
