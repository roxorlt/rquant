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

    starts: list[str] = []

    def fail_if_started(_: object) -> None:
        starts.append("start")
        raise AssertionError("module import must not start SourcePoller")

    monkeypatch.setattr(poller_module.SourcePoller, "start", fail_if_started)
    monkeypatch.setattr(st, "fragment", lambda **_: lambda fn: fn)
    sys.modules.pop("rquant.dashboard.market_panorama", None)

    importlib.import_module("rquant.dashboard.market_panorama")

    assert starts == []
