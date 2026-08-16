"""Phase-A no-activation fences for current-family signal transport."""

from __future__ import annotations

import ast
import inspect
from collections.abc import Iterator
from pathlib import Path
from types import FunctionType

import rquant.signal_route_spool as spool
from rquant.runtime_service_main import build_builtin_registry


def test_r07_exports_no_current_family_writer() -> None:
    forbidden = {"append", "create", "cursor", "cutover", "drain", "publish_v3"}
    assert not (forbidden & set(spool.__all__))


def _closure_values(function: FunctionType) -> Iterator[object]:
    yield from function.__defaults__ or ()
    yield from function.__kwdefaults__.values() if function.__kwdefaults__ else ()
    for cell in function.__closure__ or ():
        try:
            yield cell.cell_contents
        except ValueError:
            continue


def test_production_builders_have_no_r07_writer_symbols_or_reachable_capability() -> None:
    root = Path(__file__).parents[2]
    production_modules = (
        "runtime_service_main.py",
        "runtime_service_builtin.py",
        "runtime_builder_strategy.py",
        "runtime_builder_signal.py",
        "runtime_builder_shadow.py",
        "runtime_builder_paper.py",
        "runtime_builder_serving.py",
        "runtime_builder_daily_orchestrator.py",
    )
    forbidden = {
        "CurrentSignalRouteSpoolWriter",
        "SignalRouteSpoolV3Writer",
        "publish_v3",
        "r07_capability",
        "r07_cursor",
        "r07_cutover",
        "r07_drain",
    }
    for name in production_modules:
        tree = ast.parse((root / "src" / "rquant" / name).read_text())
        identifiers = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        identifiers.update(node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute))
        assert not (identifiers & forbidden), name

    registry = build_builtin_registry(runtime_capabilities={})
    builders = vars(registry)["_builders"]
    reachable: list[object] = [registry]
    for builder in builders.values():
        reachable.append(builder)
        reachable.extend(_closure_values(builder))
    assert all(value is not spool.CurrentSignalRouteSpoolRecord for value in reachable)
    assert all(value is not spool.CurrentSignalBusRoutedRecord for value in reachable)
    assert all(
        "v3" not in value.__name__.lower() for value in reachable if inspect.isfunction(value)
    )
