from __future__ import annotations

import importlib
import sys

import pytest


def test_import_is_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_load(*args: object, **kwargs: object) -> None:
        raise AssertionError("dashboard import must not read serving")

    monkeypatch.setattr(
        "rquant.dashboard.runtime_console_data.load_runtime_console",
        forbidden_load,
    )
    sys.modules.pop("rquant.dashboard.runtime_console", None)

    module = importlib.import_module("rquant.dashboard.runtime_console")

    assert callable(module.main)
    assert callable(module.render_runtime_console)
