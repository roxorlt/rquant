"""市场全景辅助函数被导入时不得启动后台行情拉取器。"""

from __future__ import annotations

import importlib
import sys

import pytest
import streamlit as st


def test_importing_panorama_does_not_start_source_poller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.panorama_poller as poller_module

    module_name = "rquant.dashboard.market_panorama"
    package = importlib.import_module("rquant.dashboard")
    original_module = sys.modules.get(module_name)
    had_package_attr = hasattr(package, "market_panorama")
    original_package_attr = getattr(package, "market_panorama", None)
    starts: list[str] = []

    def fail_if_started(_: object) -> None:
        starts.append("start")
        raise AssertionError("module import must not start SourcePoller")

    monkeypatch.setattr(poller_module.SourcePoller, "start", fail_if_started)
    monkeypatch.setattr(st, "fragment", lambda **_: lambda fn: fn)
    sys.modules.pop(module_name, None)
    package.__dict__.pop("market_panorama", None)

    try:
        importlib.import_module(module_name)
        assert starts == []
    finally:
        sys.modules.pop(module_name, None)
        if original_module is not None:
            sys.modules[module_name] = original_module
        if had_package_attr:
            package.market_panorama = original_package_attr
        else:
            package.__dict__.pop("market_panorama", None)

    assert sys.modules.get(module_name) is original_module
    assert hasattr(package, "market_panorama") is had_package_attr
    if had_package_attr:
        assert package.market_panorama is original_package_attr
