"""Trusted tracking toggles target their captured registry and research state."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, JsonValue

from rquant.factor.capability import historical_daily_capabilities
from rquant.factor.daily_feature_source import open_factor_daily_feature_source
from rquant.factor.registry import FactorDefinitionRegistry, FactorRegistryIdentity
from rquant.factor.run_configuration import FactorRunFileReference, open_factor_run_configuration
from rquant.factor.run_request import RUN_IMMUTABLE
from rquant.factor.tracking import (
    FactorTrackingIdentity,
    FactorTrackingRequest,
    FactorTrackingStore,
)

if TYPE_CHECKING:
    from rquant.page_control import _OwnedSetFactorTracked


class FrozenFactorTrackingToggle(BaseModel):
    model_config = RUN_IMMUTABLE
    request: FactorTrackingRequest
    registry_identity: FactorRegistryIdentity
    tracking_identity: FactorTrackingIdentity


class FactorTrackingPageControlBackend:
    def __init__(
        self,
        root: Path,
        reference: FactorRunFileReference,
        tracking_identity: FactorTrackingIdentity,
        *,
        enabled: bool = False,
        tracking_users: frozenset[str] = frozenset(),
    ) -> None:
        if len(tracking_users) > 16:
            raise ValueError("tracking allowlist exceeds its capacity")
        self.root, self.reference = root, reference
        self.enabled, self.tracking_users = enabled, tracking_users
        self.tracking_identity = FactorTrackingIdentity.model_validate(tracking_identity)
        if FactorTrackingStore(Path(tracking_identity.path)).identity() != tracking_identity:
            raise ValueError("tracking authority differs from configuration")
        with open_factor_run_configuration(root, reference) as loaded:
            self.registry_identity = loaded.configuration.registry_identity

    def authorize(self, actor_id: str) -> None:
        if not self.enabled or actor_id not in self.tracking_users:
            raise PermissionError("当前账号不能管理因子跟踪")

    def compile(
        self, request: FactorTrackingRequest, *, verified_registry_instance_id: str
    ) -> FrozenFactorTrackingToggle:
        request = FactorTrackingRequest.model_validate(request)
        with open_factor_run_configuration(self.root, self.reference) as loaded:
            identity = loaded.configuration.registry_identity
            if (
                identity != self.registry_identity
                or identity.instance_id != verified_registry_instance_id
            ):
                raise ValueError("定义来源已变化，请刷新")
            record = FactorDefinitionRegistry(Path(identity.path)).get_head(
                request.factor_id, expected_identity=identity
            )
            if (
                record is None
                or record.head.version != request.expected_head.version
                or record.content_sha256 != request.expected_head.content_sha256
                or (request.tracked and record.head.archived)
            ):
                raise ValueError("定义版本已变化，请刷新")
            current = FactorTrackingStore(Path(self.tracking_identity.path)).get(
                request.factor_id, expected_identity=self.tracking_identity
            )
            if request.expected_tracking_generation != (
                None if current is None else current.generation
            ):
                raise ValueError("跟踪状态已变化，请刷新")
            if request.tracked:
                columns = tuple(
                    field.column
                    for field in historical_daily_capabilities(
                        daily_features_available=True,
                        stock_features_available=True,
                        stock_base_daily_available=True,
                    ).fields
                    if field.column not in ("open", "high", "low", "close", "vol", "amount")
                    and field.column in record.definition.dependency_columns
                )
                if columns:
                    source = loaded.daily_features
                    if source is None:
                        raise ValueError("缺少可核验的库存日线事实，暂不能加入跟踪。")
                    source.require_prepared(loaded.source)
                    source.select(columns)
                    with open_factor_daily_feature_source(
                        source, lake_root=loaded.configuration.lake_root
                    ):
                        loaded.recheck()
            loaded.recheck()
            return FrozenFactorTrackingToggle(
                request=request,
                registry_identity=identity,
                tracking_identity=self.tracking_identity,
            )

    def validate(self, command: _OwnedSetFactorTracked) -> None:
        identity = command.registry_identity
        record = FactorDefinitionRegistry(Path(identity.path)).get_version(
            command.request.factor_id,
            command.request.expected_head.version,
            expected_identity=identity,
        )
        if record is None or record.content_sha256 != command.request.expected_head.content_sha256:
            raise ValueError("原定义版本无法核验")
        if (
            FactorTrackingStore(Path(command.tracking_identity.path)).identity()
            != command.tracking_identity
        ):
            raise ValueError("原跟踪状态库无法核验")

    def submit(self, command: _OwnedSetFactorTracked) -> JsonValue:
        self.authorize(command.actor_id)
        self.validate(command)
        receipt = FactorTrackingStore(Path(command.tracking_identity.path)).set_tracked(
            command.request,
            actor_id=command.actor_id,
            expected_identity=command.tracking_identity,
            registry_identity=command.registry_identity,
        )
        return receipt.model_dump(mode="json")

    def recover(self, command: _OwnedSetFactorTracked) -> JsonValue | None:
        self.authorize(command.actor_id)
        self.validate(command)
        receipt = FactorTrackingStore(Path(command.tracking_identity.path)).lookup(
            command.request, actor_id=command.actor_id, expected_identity=command.tracking_identity
        )
        return None if receipt is None else receipt.model_dump(mode="json")
