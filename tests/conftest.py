"""全局测试配置：默认禁用真实 PushDeer 推送，避免测试副作用。"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _disable_real_pushdeer(monkeypatch):
    """所有测试默认 keys=空，PushDeerClient.push() 立即 return []。

    显式想测推送的用例（如 test_notify_client.py 直接传 keys 给 client，
    或 test_cli notify-test 用 monkeypatch.setattr 覆盖）不受影响。
    """
    import rquant.config as cfg
    monkeypatch.setattr(cfg.settings, "pushdeer_keys", "")
