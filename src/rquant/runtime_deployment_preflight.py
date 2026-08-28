"""Read-only health inspection for the current isolated runtime deployment."""

from __future__ import annotations

import re
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from pydantic import model_validator

from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_deployment_bundle import (
    RuntimeDeploymentReceipt,
    load_current_runtime_deployment_receipt_unbound,
)
from rquant.runtime_deployment_rollout import build_runtime_generation_health_probe

InspectionStatus = Literal["ok", "warn", "fail", "skip"]
ReceiptLoader = Callable[[Path], RuntimeDeploymentReceipt]
UnitStateProbe = Callable[[str], str | None]
GenerationHealthProbe = Callable[[RuntimeDeploymentReceipt, str], bool]
_DYNAMIC_UNIT = re.compile(r"^rquant-runtime-.+@svc-[0-9a-f]{64}\.service$")


class RuntimeDeploymentInspection(RuntimeContractModel):
    runtime_root: Path
    generation_hash: str | None = None
    inventory_units: tuple[str, ...] = ()
    strict_authority_units: tuple[str, ...] = ()
    watchlist_quote_units: tuple[str, ...] = ()
    status: InspectionStatus
    summary: str

    @model_validator(mode="after")
    def validate_unit_partition(self) -> RuntimeDeploymentInspection:
        inventory = set(self.inventory_units)
        strict = set(self.strict_authority_units)
        advisory = set(self.watchlist_quote_units)
        if (
            len(inventory) != len(self.inventory_units)
            or len(strict) != len(self.strict_authority_units)
            or len(advisory) != len(self.watchlist_quote_units)
            or strict & advisory
            or inventory != strict | advisory
        ):
            raise ValueError("runtime receipt units must form a strict/advisory partition")
        return self


def _systemd_unit_state(unit: str) -> str | None:
    if not shutil.which("systemctl"):
        return None
    try:
        completed = subprocess.run(
            ["systemctl", "is-active", unit],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "timeout"
    return completed.stdout.strip() or "unknown"


def inspect_runtime_deployment(
    runtime_root: Path,
    *,
    receipt_loader: ReceiptLoader = load_current_runtime_deployment_receipt_unbound,
    unit_state_probe: UnitStateProbe = _systemd_unit_state,
    generation_health_probe: GenerationHealthProbe | None = None,
) -> RuntimeDeploymentInspection:
    """Enumerate receipt-bound units and evaluate the quote candidate without authority cutover."""

    root = Path(runtime_root)
    try:
        receipt = receipt_loader(root)
    except (OSError, ValueError) as exc:
        current = root / "current"
        declared = current.is_symlink() or current.exists()
        return RuntimeDeploymentInspection(
            runtime_root=root,
            status="fail" if declared else "warn",
            summary=(
                f"current deployment receipt is invalid: {type(exc).__name__}"
                if declared
                else "no current deployment receipt; isolated quote candidate is not installed"
            ),
        )

    inventory_units = tuple(
        sorted(unit for unit in receipt.unit_mapping.values() if _DYNAMIC_UNIT.fullmatch(unit))
    )
    quote_units = tuple(
        unit for unit in inventory_units if unit.startswith("rquant-runtime-watchlist-quote@")
    )
    strict_units = tuple(unit for unit in inventory_units if unit not in quote_units)
    if not quote_units:
        return RuntimeDeploymentInspection(
            runtime_root=root,
            generation_hash=receipt.generation_hash,
            inventory_units=inventory_units,
            strict_authority_units=strict_units,
            status="warn",
            summary="current deployment receipt has no watchlist quote candidate unit",
        )

    states = {unit: unit_state_probe(unit) for unit in quote_units}
    if any(state is None for state in states.values()):
        return RuntimeDeploymentInspection(
            runtime_root=root,
            generation_hash=receipt.generation_hash,
            inventory_units=inventory_units,
            strict_authority_units=strict_units,
            watchlist_quote_units=quote_units,
            status="skip",
            summary="systemctl unavailable; watchlist quote candidate health was not verified",
        )
    inactive = tuple(unit for unit, state in states.items() if state != "active")
    if inactive:
        details = ", ".join(f"{unit}={states[unit]}" for unit in inactive)
        return RuntimeDeploymentInspection(
            runtime_root=root,
            generation_hash=receipt.generation_hash,
            inventory_units=inventory_units,
            strict_authority_units=strict_units,
            watchlist_quote_units=quote_units,
            status="warn",
            summary=f"watchlist quote candidate unit is not active: {details}",
        )

    probe = generation_health_probe or build_runtime_generation_health_probe()
    unhealthy = tuple(unit for unit in quote_units if not probe(receipt, unit))
    if unhealthy:
        return RuntimeDeploymentInspection(
            runtime_root=root,
            generation_hash=receipt.generation_hash,
            inventory_units=inventory_units,
            strict_authority_units=strict_units,
            watchlist_quote_units=quote_units,
            status="warn",
            summary="watchlist quote heartbeat is missing or stale for current generation",
        )
    return RuntimeDeploymentInspection(
        runtime_root=root,
        generation_hash=receipt.generation_hash,
        inventory_units=inventory_units,
        strict_authority_units=strict_units,
        watchlist_quote_units=quote_units,
        status="ok",
        summary="watchlist quote candidate unit and generation-bound heartbeat are healthy",
    )


__all__ = [
    "RuntimeDeploymentInspection",
    "inspect_runtime_deployment",
]
