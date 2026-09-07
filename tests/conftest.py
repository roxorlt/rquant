"""全局测试配置：默认禁用真实 PushDeer 推送，避免测试副作用。"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _disable_real_pushdeer(monkeypatch):
    """所有测试默认禁用真实推送通道（PushDeer + PushPlus），避免刷手机。

    显式想测推送的用例（如 test_notify_client.py 直接传 keys/tokens 给
    client，或 test_cli notify-test 用 monkeypatch.setattr 覆盖）不受影响。
    """
    import rquant.config as cfg
    monkeypatch.setattr(cfg.settings, "pushdeer_keys", "")
    monkeypatch.setattr(cfg.settings, "pushplus_tokens", "")


@pytest.fixture(autouse=True)
def _the_suite_is_not_a_runtime_unit(monkeypatch):
    """Make "this process is not a systemd runtime unit" true on every platform.

    `runtime_capabilities._undelivered_credential_reason` decides whether a missing
    capability credential is worth accusing a unit over by reading `/proc/self/cgroup` and
    taking the leaf that ends in `.service`. Its `None` branch is documented as "a bare
    diagnostic run, or the suite", and on Darwin that holds for free because there is no
    `/proc`. On a GitHub Linux runner the whole job lives inside
    `hosted-compute-agent.service`, so the leaf matches and every role a test starts
    without a credential was accused of a unit that has nothing to do with rQuant —
    six cases across three files failed on Linux while passing on macOS.

    Pointing the probe at a path that does not exist makes the premise true everywhere
    rather than only on Darwin. The cases that are about the probe itself set the same
    attribute for themselves in `tests/unit/test_credstore_capability_delivery.py`, and
    monkeypatch inside the test wins over this fixture.
    """

    from pathlib import Path

    import rquant.runtime_capabilities as runtime_capabilities

    monkeypatch.setattr(
        runtime_capabilities,
        "_SYSTEMD_CGROUP_PATH",
        Path("/nonexistent/rquant-suite-is-not-a-unit/cgroup"),
    )
