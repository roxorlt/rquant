"""PageControl queues a frozen run; it does not compute or initialize authorities."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import JsonValue

from rquant.factor.job_ledger import FactorEvaluationJobLedger, FactorJobRecord
from rquant.factor.member_archive import _READ_FLAGS, _check_identities, _load_manifest
from rquant.factor.registry import FactorDefinitionRegistry, FactorHeadRef
from rquant.factor.result_artifact import _file_identity, _open_private_root, _require_named_regular
from rquant.factor.run_configuration import (
    FactorRunConfiguration,
    FactorRunFileReference,
    open_factor_run_configuration,
)
from rquant.factor.run_plan import FrozenFactorRunPlan, compile_factor_run_plan
from rquant.factor.run_request import FactorRunAvailability, FactorRunPoolOption, FactorRunRequest

if TYPE_CHECKING:
    from rquant.page_control import _OwnedSubmitFactorRun

_LABELS = {
    "all": "全市场（剔除北交所、ST）",
    "hs300": "沪深300",
    "zz1000": "中证1000",
    "gem": "创业板 + 科创板",
}


class FactorRunPageControlBackend:
    def __init__(
        self,
        root: Path,
        reference: FactorRunFileReference,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.root, self.reference, self.clock = root, reference, clock
        config = self.configuration()
        self.enabled, self.run_users = config.enabled, config.factor_run_users

    def configuration(self) -> FactorRunConfiguration:
        with open_factor_run_configuration(self.root, self.reference) as loaded:
            return loaded.configuration

    def authorize(self, actor_id: str) -> None:
        if not self.enabled or actor_id not in self.run_users:
            raise PermissionError("当前账号不能运行检验")

    def availability(self, actor_id: str) -> FactorRunAvailability:
        self.authorize(actor_id)
        with open_factor_run_configuration(self.root, self.reference) as loaded:
            config, scope = loaded.configuration, loaded.source.admission_request.scope
            selections = set()
            for item in config.members:
                descriptor = None
                try:
                    descriptor = _open_private_root(config.member_root)
                    manifest, identity = _load_manifest(descriptor, item.archive)
                    if (
                        manifest.request.selection != item.selection
                        or manifest.request.computation_stock_codes != scope.stock_codes
                        or manifest.request.as_of > scope.as_of_time
                    ):
                        continue
                    identities = {item.archive.filename: identity}
                    for day in manifest.days:
                        file_fd = os.open(day.filename, _READ_FLAGS, dir_fd=descriptor)
                        try:
                            observed = _require_named_regular(descriptor, day.filename, file_fd)
                            if observed.st_size != day.byte_count:
                                raise ValueError("历史归档日文件不完整")
                            identities[day.filename] = _file_identity(observed)
                        finally:
                            os.close(file_fd)
                    _check_identities(config.member_root, descriptor, identities)
                    selections.add(item.selection)
                except (OSError, ValueError):
                    pass
                finally:
                    if descriptor is not None:
                        os.close(descriptor)
            return FactorRunAvailability(
                enabled=True,
                pools=tuple(
                    FactorRunPoolOption(
                        selection=selection,
                        label=label,
                        available=selection in selections,
                        reason=None if selection in selections else "缺少该股票池的历史归档",
                    )
                    for selection, label in _LABELS.items()
                ),
                start_date=scope.start_date,
                end_date=scope.end_date,
            )

    def compile(
        self,
        request: FactorRunRequest,
        *,
        verified_registry_instance_id: str,
    ) -> FrozenFactorRunPlan:
        return compile_factor_run_plan(
            self.root,
            self.reference,
            request,
            verified_registry_instance_id=verified_registry_instance_id,
            clock=self.clock,
        )

    def confirm_unsubmitted(
        self, request: FactorRunRequest, *, verified_registry_instance_id: str
    ) -> None:
        with open_factor_run_configuration(self.root, self.reference) as loaded:
            config = loaded.configuration
            if config.registry_identity.instance_id != verified_registry_instance_id:
                raise ValueError("定义来源已变化，请刷新")
            registry = FactorDefinitionRegistry(Path(config.registry_identity.path))
            record = registry.get_head(
                request.parameters.factor_id, expected_identity=config.registry_identity
            )
            if (
                record is None
                or record.head.archived
                or FactorHeadRef(
                    version=record.head.version, content_sha256=record.head.content_sha256
                )
                != request.parameters.expected_head
            ):
                raise ValueError("定义版本已变化，请刷新")
            if loaded.open_ledger(clock=self.clock).command_exists(request.command_id):
                raise ValueError("原操作已有任务记录，请继续核对原请求")
            loaded.recheck()

    def validate(self, command: _OwnedSubmitFactorRun) -> None:
        registry = FactorDefinitionRegistry(Path(command.registry_identity.path))
        definition = command.spec.adapter_request.formula.definition
        record = registry.get_version(
            definition.factor_id, definition.version, expected_identity=command.registry_identity
        )
        if (
            record is None
            or record.definition != definition
            or record.content_sha256 != command.spec.definition_content_sha256
        ):
            raise ValueError("原定义版本无法核验")
        FactorEvaluationJobLedger.open_existing(command.ledger_identity, clock=self.clock)

    @staticmethod
    def _receipt(record: FactorJobRecord) -> JsonValue:
        return {
            "status": "queued",
            "job_id": record.job_id,
            "spec_sha256": record.spec_sha256,
            "definition_version": record.spec.adapter_request.formula.definition.version,
        }

    def submit(self, command: _OwnedSubmitFactorRun) -> JsonValue:
        self.authorize(command.actor_id)
        self.validate(command)
        ledger = FactorEvaluationJobLedger.open_existing(command.ledger_identity, clock=self.clock)
        return self._receipt(ledger.submit(command.command_id, command.spec))

    def recover(self, command: _OwnedSubmitFactorRun) -> JsonValue | None:
        self.authorize(command.actor_id)
        self.validate(command)
        ledger = FactorEvaluationJobLedger.open_existing(command.ledger_identity, clock=self.clock)
        record = ledger.lookup_command(command.command_id, command.spec.spec_sha256)
        if record is not None and record.spec != command.spec:
            raise ValueError("原任务输入无法核验")
        return None if record is None else self._receipt(record)
