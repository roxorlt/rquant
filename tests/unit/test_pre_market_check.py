"""pre_market_check 单测（主要覆盖 tushare 检查 5/25 事故修复的 warn 降级）。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestCheckTushareCredits:
    def test_no_token_skips(self) -> None:
        from rquant.pre_market_check import check_tushare_credits

        r = check_tushare_credits(token=None)
        assert r.status == "skip"
        assert "未配置" in r.message

    def test_api_exception_is_warn_not_fail(self) -> None:
        """5/25 事故：tushare 服务端禁用 user 接口报「请指定正确的接口名」，
        本检查必须返回 warn（不让体检整体 fail，每天推噪音 alert）。"""
        from rquant.pre_market_check import check_tushare_credits

        mock_ts = MagicMock()
        mock_pro = MagicMock()
        mock_pro.user.side_effect = Exception("请指定正确的接口名")
        mock_ts.pro_api.return_value = mock_pro

        with patch.dict("sys.modules", {"tushare": mock_ts}):
            r = check_tushare_credits(token="abc" * 20)

        assert r.status == "warn"
        assert "请指定正确的接口名" in r.message

    def test_ok_when_credits_above_threshold(self) -> None:
        import pandas as pd
        from rquant.pre_market_check import check_tushare_credits

        mock_ts = MagicMock()
        mock_pro = MagicMock()
        mock_pro.user.return_value = pd.DataFrame([
            {"到期积分": 2000, "到期时间": "2027-04-16"},
            {"到期积分": 50, "到期时间": "2026-12-31"},
        ])
        mock_ts.pro_api.return_value = mock_pro

        with patch.dict("sys.modules", {"tushare": mock_ts}):
            r = check_tushare_credits(token="abc" * 20, warn_threshold=500)

        assert r.status == "ok"
        assert "2050 积分" in r.message
        assert "2026-12-31 到期" in r.message  # 取最早到期时间


class TestRunAllChecksExitCode:
    """run_all_checks 整体不应该因为 tushare 故障 exit 1。"""

    def test_tushare_warn_does_not_propagate_to_fail(self) -> None:
        """模拟 tushare 接口报错，整套 results 中应该只有 warn 没有 fail。"""
        import pandas as pd
        from rquant.pre_market_check import run_all_checks

        mock_ts = MagicMock()
        mock_pro = MagicMock()
        mock_pro.user.side_effect = Exception("请指定正确的接口名")
        mock_ts.pro_api.return_value = mock_pro

        # 跳过 systemd / journalctl 检查（mac 本地没有）— 它们已经 skip
        with patch.dict("sys.modules", {"tushare": mock_ts}):
            results = run_all_checks()

        tushare_results = [r for r in results if r.name == "tushare"]
        assert len(tushare_results) == 1
        assert tushare_results[0].status == "warn"
        # 整套结果没有 fail（mac 本地跑 systemd / journalctl 是 skip 不是 fail）
        fails = [r for r in results if r.status == "fail"]
        assert fails == [], f"不该有 fail 项: {[(r.name, r.message) for r in fails]}"
