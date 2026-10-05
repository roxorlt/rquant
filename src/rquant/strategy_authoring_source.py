"""Trusted preparation computes entry facts using the original screening factories."""

from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
from typing import Self

import pandas as pd
from pydantic import Field, model_validator

from rquant.llm.registry import get_rule_spec
from rquant.screen import rules as screen_rules
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.backtest.contracts import Sha256
from rquant.strategy_template import ConditionTemplateEntry, PoolTemplateEntry, StrategyTemplate, SignalTemplateEntry
from rquant.strategy_template_execution import TemplateEntryEvidence, TemplateEntryProjection, strategy_template_entry


def template_source_code_identity() -> str:
    from rquant.llm import registry

    return canonical_sha256({"contract": "template-source-code/v1", "screen_registry": hashlib.sha256(Path(registry.__file__).read_bytes()).hexdigest(), "screen_rules": hashlib.sha256(Path(screen_rules.__file__).read_bytes()).hexdigest(), "template_source": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})


class TemplatePoolReference(RuntimeContractModel):
    pool_key: str
    version: int = Field(strict=True, ge=1)
    body_hash: Sha256
    owner_id: str | None
    name: str = Field(min_length=1, max_length=80)


class TemplateSignalReference(RuntimeContractModel):
    strategy_id: str
    version: int = Field(strict=True, ge=1)
    source_hash: Sha256
    actions: tuple[str, ...]
    owner_id: str | None
    name: str = Field(min_length=1, max_length=80)


class StrategySourceCatalog(RuntimeContractModel):
    owner_id: str
    generation_id: str
    pools: tuple[TemplatePoolReference, ...]
    signals: tuple[TemplateSignalReference, ...]

    @model_validator(mode="after")
    def validate_unique_refs(self) -> Self:
        pool_keys = [(item.pool_key, item.version) for item in self.pools]
        signal_keys = [(item.strategy_id, item.version) for item in self.signals]
        if len(set(pool_keys)) != len(pool_keys) or len(set(signal_keys)) != len(signal_keys):
            raise ValueError("strategy source catalog contains duplicate references")
        return self

    def validate_rules(self, rules: StrategyTemplate, *, owner_id: str, generation_id: str) -> None:
        if self.owner_id != owner_id:
            raise PermissionError("strategy source catalog owner differs")
        if self.generation_id != generation_id:
            raise ValueError("strategy source catalog generation differs")
        if isinstance(rules.entry, PoolTemplateEntry):
            entry = rules.entry
            selected = next((item for item in self.pools if (item.pool_key, item.version, item.body_hash) == (entry.pool_key, entry.version, entry.body_hash)), None)
            if selected is None:
                raise ValueError("pool reference is not in trusted catalog")
            if selected.owner_id is not None and selected.owner_id != owner_id:
                raise PermissionError("pool source belongs to another owner")
        elif isinstance(rules.entry, SignalTemplateEntry):
            entry = rules.entry
            selected = next((item for item in self.signals if (item.strategy_id, item.version, item.source_hash) == (entry.strategy_id, entry.version, entry.source_hash)), None)
            if selected is None or entry.action not in selected.actions:
                raise ValueError("signal reference is not in trusted catalog")
            if selected.owner_id is not None and selected.owner_id != owner_id:
                raise PermissionError("signal source belongs to another owner")


def produce_template_entry(rules: StrategyTemplate, evidence: TemplateEntryEvidence, *, decision_time: datetime) -> TemplateEntryProjection:
    evidence = TemplateEntryEvidence.model_validate(evidence.model_dump(mode="python"))
    if evidence.observed_at > decision_time:
        raise ValueError("future entry evidence")
    eligible: tuple[str, ...] = ()
    if isinstance(rules.entry, ConditionTemplateEntry):
        frame = pd.DataFrame(evidence.model_dump(mode="python")["rows"])
        if not frame.empty:
            if "ts_code" not in frame or frame["ts_code"].duplicated().any():
                raise ValueError("condition source has missing or duplicate codes")
            selected = pd.Series(True, index=frame.index)
            for condition in rules.entry.conditions:
                original = get_rule_spec(condition.key)
                selected &= original.fn(**condition.model_dump(mode="json")["args"])(frame)
            eligible = tuple(sorted(frame.loc[selected.fillna(False), "ts_code"].astype(str)))
    projection = TemplateEntryProjection(evidence=evidence, rules_hash=rules.rules_hash, evidence_hash=canonical_sha256(evidence), eligible_codes=eligible)
    strategy_template_entry(rules, projection, decision_time=decision_time)
    return projection
