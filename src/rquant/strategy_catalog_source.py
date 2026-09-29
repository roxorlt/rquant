"""Verified three-strategy definition catalog for an optional Serving authority."""

from __future__ import annotations

import math
import os
from datetime import datetime
from pathlib import Path

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.runtime_builder_strategy import StrategyLiveRuntimeSettings
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.runtime_definition_bootstrap import plan_builtin_definitions
from rquant.runtime_generation_lineage import load_runtime_generation_tree
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
)
from rquant.runtime_serving_snapshot import (
    STRATEGY_CATALOG_DATASET_ID,
    SourceReadResult,
    StrategyCatalogPayload,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionPayload
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

_NAMES = {
    "auction_gap": "集合竞价跳空",
    "growth_board_surge": "科创及创业板放量",
    "n_shape": "N 字形态",
}
_PARAMETER_LABELS = {
    "allowed_boards": "适用板块",
    "auction_ratio_max": "竞价比例上限",
    "auction_ratio_min": "竞价比例下限",
    "break_high_ratio": "突破高点比例",
    "carry_low_ratio": "回踩低点比例",
    "expires_seconds": "信号有效期",
    "min_amount_accel_5m": "五分钟成交额加速下限",
    "min_gap_pct": "跳空幅度下限",
    "min_historical_sessions": "最少历史交易日",
    "min_hold_auction_price_ratio": "维持竞价价格比例",
    "min_price_over_vwap": "相对均价下限",
    "min_rel_cumulative": "累计相对量下限",
    "min_rel_same_minute": "同分钟相对量下限",
    "price_tolerance_ratio": "价格容差",
}
_RATIO_KEYS = {
    "auction_ratio_max",
    "auction_ratio_min",
    "break_high_ratio",
    "carry_low_ratio",
    "min_amount_accel_5m",
    "min_hold_auction_price_ratio",
    "min_price_over_vwap",
    "min_rel_cumulative",
    "min_rel_same_minute",
}
_EXPECTED_IDS = tuple(sorted(_NAMES))


def _display_parameter(key: str, value: object) -> str:
    if key == "allowed_boards" and value == ("gem", "star"):
        return "创业板、科创板"
    if type(value) not in (int, float) or key not in _PARAMETER_LABELS:
        raise ValueError("strategy catalog contains an unsupported parameter")
    if abs(value) > 1_000_000 or not math.isfinite(value):
        raise ValueError("strategy catalog parameter exceeds its numeric bound")
    if key == "expires_seconds":
        rendered = f"{value:g} 秒"
    elif key == "min_historical_sessions":
        rendered = f"{value:g} 个交易日"
    elif key == "price_tolerance_ratio":
        rendered = f"{value * 100:.2f}%"
    elif key == "min_gap_pct":
        rendered = f"{value:g}%"
    elif key in _RATIO_KEYS:
        rendered = f"{value:g} 倍"
    else:
        raise ValueError("strategy catalog contains an unsupported parameter")
    if len(rendered.encode("utf-8")) > 96:
        raise ValueError("strategy catalog parameter display exceeds its bound")
    return rendered


class StrategyCatalogSourceReader:
    """Read a single authenticated runtime generation and its executable definitions."""

    def __init__(self, *, runtime_root: Path) -> None:
        root = Path(runtime_root)
        if not root.is_absolute() or root != Path(os.path.abspath(root)):
            raise ValueError("runtime_root must be absolute and normalized")
        self.runtime_root = root

    def __call__(self, observed_at: datetime, /) -> SourceReadResult:
        return self._read(observed_at, expected_producer_commit=None)

    def _read(
        self,
        observed_at: datetime,
        *,
        expected_producer_commit: str | None,
    ) -> SourceReadResult:
        observed = normalize_aware_utc(observed_at)
        tree = load_runtime_generation_tree(self.runtime_root)
        manifests = []
        settings_by_id = {}
        for strategy_id in _EXPECTED_IDS:
            service_id = f"strategy.{strategy_id}.v1"
            record = tree.lineage(service_id).current
            manifest = record.manifest
            if record.generation_id != tree.current_generation_id:
                raise ValueError("strategy catalog mixed runtime generations")
            if (
                manifest.service_id != service_id
                or manifest.service_kind is not RuntimeServiceKind.STRATEGY_LIVE
                or manifest.plane is not RuntimeServicePlane.LIVE
            ):
                raise ValueError("strategy catalog service identity does not match")
            settings = StrategyLiveRuntimeSettings.model_validate(dict(manifest.settings))
            if settings.strategy_id != strategy_id or settings.strategy_version != 1:
                raise ValueError("strategy catalog settings identity does not match")
            manifests.append(manifest)
            settings_by_id[strategy_id] = settings
        commits = {manifest.producer_commit for manifest in manifests}
        roots = {settings.definition_registry_root for settings in settings_by_id.values()}
        if len(commits) != 1 or len(roots) != 1:
            raise ValueError("strategy catalog manifests do not share a definition authority")
        commit = commits.pop()
        if expected_producer_commit is not None and commit != expected_producer_commit:
            raise ValueError("strategy catalog source commit does not match publisher")
        plan = plan_builtin_definitions(producer_commit=commit)
        evaluator = BuiltinStrategyEvaluatorRegistry(producer_commit=commit)
        registry = ImmutableDefinitionRegistry(
            roots.pop(),
            execution_registry=evaluator.trusted_executable_registry(),
        )
        catalog_rows = []
        parameter_rows = []
        registrations = []
        for binding in plan.strategies:
            settings = settings_by_id[binding.strategy_id]
            if (
                settings.strategy_registration_fingerprint != binding.registration_fingerprint
                or settings.strategy_spec_fingerprint != binding.strategy_spec_fingerprint
                or settings.strategy_executable_fingerprint != binding.executable_fingerprint
                or settings.evaluator_contract_fingerprint != binding.executable_fingerprint
                or settings.candidate_schema_fingerprint != binding.candidate_schema_fingerprint
            ):
                raise ValueError("strategy catalog manifest differs from trusted plan")
            registration = registry.read_strategy_spec(
                binding.registration_fingerprint,
                as_of=observed,
            )
            if registration is None:
                raise ValueError("strategy catalog registration is unavailable")
            definition = evaluator.load_definition(binding.strategy_id, binding.strategy_version)
            spec = registration.spec
            if (
                registration.fingerprint != binding.registration_fingerprint
                or registration.producer_commit != commit
                or registration.logical_id != binding.strategy_id
                or registration.version != binding.strategy_version
                or registration.candidate_schema_fingerprint != binding.candidate_schema_fingerprint
                or registration.executable_fingerprint != binding.executable_fingerprint
                or spec.spec_fingerprint != binding.strategy_spec_fingerprint
                or spec.spec_fingerprint != definition.spec.spec_fingerprint
            ):
                raise ValueError("strategy catalog registration differs from trusted definition")
            registrations.append(registration)
            catalog_rows.append(
                {
                    "strategy_id": binding.strategy_id,
                    "name": _NAMES[binding.strategy_id],
                    "version": binding.strategy_version,
                    "registered_at": registration.registered_at.isoformat(),
                }
            )
            for key, value in sorted(spec.parameters.items()):
                parameter_rows.append(
                    {
                        "strategy_id": binding.strategy_id,
                        "parameter_key": key,
                        "label": _PARAMETER_LABELS[key],
                        "display_value": _display_parameter(key, value),
                    }
                )
        if len(catalog_rows) != 3 or len(parameter_rows) > 32:
            raise ValueError("strategy catalog exceeds its fixed publication bound")
        available_at = max(item.available_at for item in registrations)
        projections = (
            ServingProjectionPayload(
                table_name="strategy_catalog",
                available_at=observed,
                rows=tuple(catalog_rows),
            ),
            ServingProjectionPayload(
                table_name="strategy_catalog_parameter",
                available_at=observed,
                rows=tuple(parameter_rows),
            ),
        )
        source_digest = canonical_sha256(
            {
                "runtime_generation_id": tree.current_generation_id,
                "registration_fingerprints": tuple(item.fingerprint for item in registrations),
                "registered_at": tuple(item.registered_at for item in registrations),
            }
        )
        self.assert_current(tree.current_generation_id)
        values: dict[str, object] = {
            "dataset_id": STRATEGY_CATALOG_DATASET_ID,
            "sequence": 0,
            "event_time": available_at,
            "published_at": available_at,
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": StrategyCatalogPayload(
                source_digest=source_digest,
                runtime_generation_id=tree.current_generation_id,
                projections=projections,
            ),
        }
        values["generation_id"] = canonical_sha256(values)
        return SourceReadResult.model_validate(values)

    def assert_current(self, generation_id: str) -> None:
        if load_runtime_generation_tree(self.runtime_root).current_generation_id != generation_id:
            raise ValueError("strategy catalog runtime generation changed during publication")


class StrategyCatalogAuthorityPublisher:
    """Publish only the source read for the still-current authenticated generation."""

    def __init__(
        self,
        *,
        reader: StrategyCatalogSourceReader,
        publisher: ServingSourceAuthorityPublisher,
    ) -> None:
        if publisher.dataset_id != STRATEGY_CATALOG_DATASET_ID:
            raise ValueError("publisher must own the strategy catalog dataset")
        if publisher.payload_kind != "strategy_catalog":
            raise ValueError("publisher must own the strategy catalog payload")
        self.reader = reader
        self.publisher = publisher

    def publish(self, observed_at: datetime) -> ServingSourceAuthorityPointer:
        result = self.reader._read(
            observed_at,
            expected_producer_commit=self.publisher.producer_commit,
        )
        assert isinstance(result.payload, StrategyCatalogPayload)
        assert result.payload.runtime_generation_id is not None
        self.reader.assert_current(result.payload.runtime_generation_id)
        return self.publisher.publish(result)


__all__ = ["StrategyCatalogAuthorityPublisher", "StrategyCatalogSourceReader"]
