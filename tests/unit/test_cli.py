"""CLI 入口单测 —— 仅验证 argparse 解析，不启动调度器。"""

from __future__ import annotations

import subprocess
import sys

from rquant.cli import build_parser


class TestBuildParser:
    def test_serve_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve"])
        assert args.command == "serve"
        assert args.hour == 17

    def test_serve_custom_hour(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve", "--hour", "16"])
        assert args.hour == 16

    def test_run_daily_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run-daily"])
        assert args.command == "run-daily"
        assert args.date is None
        assert args.preset is None
        assert not args.skip_minute_backfill
        assert args.minute_lookback_days == 90

    def test_run_daily_with_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "run-daily",
            "--date", "2026-04-18",
            "--preset", "n-shape-pool1",
            "--skip-minute-backfill",
            "--minute-lookback-days", "60",
        ])
        assert args.date == "2026-04-18"
        assert args.preset == "n-shape-pool1"
        assert args.skip_minute_backfill
        assert args.minute_lookback_days == 60

    def test_no_command_returns_none(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None


class TestCLISmoke:
    def test_help_exits_0(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "rquant" in result.stdout

    def test_run_daily_help(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "rquant.cli", "run-daily", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "--date" in result.stdout


class TestMonitorParser:
    def test_default_interval(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["monitor"])
        assert args.command == "monitor"
        assert args.interval == 5

    def test_custom_interval(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["monitor", "--interval", "10"])
        assert args.interval == 10


class TestRtMinuteFetchParser:
    def test_rt_minute_fetch_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-fetch",
            "--ts-code", "605366.SH,301051.SZ",
        ])
        assert args.command == "rt-minute-fetch"
        assert args.ts_code == ["605366.SH,301051.SZ"]
        assert args.freq == "1min"

    def test_rt_minute_fetch_accepts_repeated_codes(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-fetch",
            "--ts-code", "605366.SH",
            "--ts-code", "301051.SZ",
            "--freq", "5min",
        ])
        assert args.ts_code == ["605366.SH", "301051.SZ"]
        assert args.freq == "5min"


class TestRtMinuteDailyFetchParser:
    def test_rt_minute_daily_fetch_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-daily-fetch",
            "--ts-code", "605366.SH,301051.SZ",
        ])
        assert args.command == "rt-minute-daily-fetch"
        assert args.ts_code == ["605366.SH,301051.SZ"]
        assert args.freq == "1min"

    def test_rt_minute_daily_fetch_accepts_repeated_codes(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "rt-minute-daily-fetch",
            "--ts-code", "605366.SH",
            "--ts-code", "301051.SZ",
            "--freq", "5min",
        ])
        assert args.ts_code == ["605366.SH", "301051.SZ"]
        assert args.freq == "5min"


class TestGrowthBoardSurgeReplayParser:
    def test_growth_board_surge_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "growth-board-surge-replay",
            "--start-date", "2026-06-25",
            "--end-date", "2026-06-26",
        ])
        assert args.command == "growth-board-surge-replay"
        assert args.freq == "1min"
        assert args.min_signal_time == "09:33"
        assert args.lookback_days == 20
        assert args.min_hist_days == 10
        assert args.min_cum_amount_ratio == 1.4
        assert args.min_same_minute_amount_ratio == 2.0
        assert args.max_hold_days == 1
        assert args.output is None

    def test_growth_board_surge_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "growth-board-surge-replay",
            "--start-date", "2026-06-25",
            "--end-date", "2026-06-26",
            "--freq", "5min",
            "--min-signal-time", "09:35",
            "--lookback-days", "30",
            "--min-hist-days", "15",
            "--min-cum-amount-ratio", "1.8",
            "--min-same-minute-amount-ratio", "3.0",
            "--max-hold-days", "2",
            "--output", "/tmp/growth.csv",
        ])
        assert args.freq == "5min"
        assert args.min_signal_time == "09:35"
        assert args.lookback_days == 30
        assert args.min_hist_days == 15
        assert args.min_cum_amount_ratio == 1.8
        assert args.min_same_minute_amount_ratio == 3.0
        assert args.max_hold_days == 2
        assert args.output == "/tmp/growth.csv"


class TestMoneyflowBackfillParser:
    def test_moneyflow_backfill_requires_date(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["moneyflow-backfill", "--date", "2026-06-26"])
        assert args.command == "moneyflow-backfill"
        assert args.date == "2026-06-26"


class TestMinuteBackfillParser:
    def test_minute_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["minute-backfill", "--date", "2026-06-24"])
        assert args.command == "minute-backfill"
        assert args.date == "2026-06-24"
        assert args.lookback_days == 90
        assert args.freq == "1min"
        assert args.preset == "n-shape-pool1"
        assert args.ts_code is None
        assert not args.dry_run

    def test_minute_backfill_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-backfill",
            "--date", "2026-06-24",
            "--lookback-days", "90",
            "--freq", "5min",
            "--preset", "n-shape-pool2",
            "--ts-code", "600000.SH",
            "--dry-run",
        ])
        assert args.lookback_days == 90
        assert args.freq == "5min"
        assert args.preset == "n-shape-pool2"
        assert args.ts_code == "600000.SH"
        assert args.dry_run


class TestMinuteReplayParser:
    def test_minute_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-replay",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
        ])
        assert args.command == "minute-replay"
        assert args.start_date == "2026-06-01"
        assert args.end_date == "2026-06-24"
        assert args.preset == "n-shape-pool1"
        assert args.freq == "1min"
        assert args.entry_mode == "first_break"
        assert args.max_hold_days == 5
        assert not args.volume_profile
        assert args.volume_profile_lookbacks == [90]
        assert args.output is None

    def test_minute_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-replay",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
            "--preset", "n-shape-pool2",
            "--freq", "5min",
            "--entry-mode", "amount_surge",
            "--max-hold-days", "3",
            "--volume-profile",
            "--volume-profile-lookbacks", "90",
            "--output", "/private/tmp/replay.csv",
        ])
        assert args.preset == "n-shape-pool2"
        assert args.freq == "5min"
        assert args.entry_mode == "amount_surge"
        assert args.max_hold_days == 3
        assert args.volume_profile
        assert args.volume_profile_lookbacks == [90]
        assert args.output == "/private/tmp/replay.csv"


class TestMinuteReplayBackfillParser:
    def test_minute_replay_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-replay-backfill",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
        ])
        assert args.command == "minute-replay-backfill"
        assert args.start_date == "2026-06-01"
        assert args.end_date == "2026-06-24"
        assert args.preset == "n-shape-pool1"
        assert args.freq == "1min"
        assert args.max_hold_days == 5
        assert args.ts_code is None
        assert not args.dry_run

    def test_minute_replay_backfill_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "minute-replay-backfill",
            "--start-date", "2026-06-01",
            "--end-date", "2026-06-24",
            "--preset", "n-shape-pool2",
            "--freq", "5min",
            "--max-hold-days", "3",
            "--ts-code", "600000.SH",
            "--dry-run",
        ])
        assert args.preset == "n-shape-pool2"
        assert args.freq == "5min"
        assert args.max_hold_days == 3
        assert args.ts_code == "600000.SH"
        assert args.dry_run


class TestAuctionBackfillParser:
    def test_auction_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-backfill",
            "--start-date", "2025-01-01",
            "--end-date", "2025-02-18",
        ])
        assert args.command == "auction-backfill"
        assert args.start_date == "2025-01-01"
        assert args.end_date == "2025-02-18"
        assert not args.dry_run

    def test_auction_backfill_dry_run(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-backfill",
            "--start-date", "2025-01-01",
            "--end-date", "2025-02-18",
            "--dry-run",
        ])
        assert args.dry_run


class TestAuctionMinuteFallbackParser:
    def test_auction_minute_fallback_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-minute-fallback",
            "--date", "2026-06-26",
        ])
        assert args.command == "auction-minute-fallback"
        assert args.date == "2026-06-26"
        assert not args.dry_run


class TestAuctionGapReplayParser:
    def test_auction_gap_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-gap-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
        ])
        assert args.command == "auction-gap-replay"
        assert args.start_date == "2025-01-16"
        assert args.end_date == "2026-06-24"
        assert args.gap_mode == "close"
        assert args.st_filter == "case_insensitive"
        assert args.min_ratio == 0.15
        assert args.max_ratio == 5.0
        assert args.output is None

    def test_auction_gap_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-gap-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
            "--gap-mode", "strict_high",
            "--st-filter", "literal_lower",
            "--min-ratio", "0.2",
            "--max-ratio", "2",
            "--output", "/private/tmp/auction-gap.csv",
        ])
        assert args.gap_mode == "strict_high"
        assert args.st_filter == "literal_lower"
        assert args.min_ratio == 0.2
        assert args.max_ratio == 2.0
        assert args.output == "/private/tmp/auction-gap.csv"


class TestAuctionGapMinuteReplayParser:
    def test_auction_gap_minute_replay_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-gap-minute-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
        ])
        assert args.command == "auction-gap-minute-replay"
        assert args.start_date == "2025-01-16"
        assert args.end_date == "2026-06-24"
        assert args.gap_mode == "close"
        assert args.st_filter == "case_insensitive"
        assert args.min_ratio == 0.15
        assert args.max_ratio == 5.0
        assert args.max_hold_days == 1
        assert args.seal_hold_days is None
        assert args.seal_hold_max_open_times == 0
        assert args.output is None

    def test_auction_gap_minute_replay_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-gap-minute-replay",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
            "--gap-mode", "strict_high",
            "--st-filter", "literal_lower",
            "--min-ratio", "0.2",
            "--max-ratio", "2",
            "--max-hold-days", "2",
            "--seal-hold-days", "3",
            "--seal-hold-max-open-times", "1",
            "--output", "/private/tmp/auction-gap-minute.csv",
        ])
        assert args.gap_mode == "strict_high"
        assert args.st_filter == "literal_lower"
        assert args.min_ratio == 0.2
        assert args.max_ratio == 2.0
        assert args.max_hold_days == 2
        assert args.seal_hold_days == 3
        assert args.seal_hold_max_open_times == 1
        assert args.output == "/private/tmp/auction-gap-minute.csv"


class TestAuctionGapMinuteBackfillParser:
    def test_auction_gap_minute_backfill_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "auction-gap-minute-backfill",
            "--start-date", "2025-01-16",
            "--end-date", "2026-06-24",
        ])
        assert args.command == "auction-gap-minute-backfill"
        assert args.start_date == "2025-01-16"
        assert args.end_date == "2026-06-24"
        assert args.gap_mode == "close"
        assert args.st_filter == "case_insensitive"
        assert args.max_hold_days == 1
        assert not args.dry_run


class TestCmdAuctionGapReplay:
    def test_uses_readonly_store(self, monkeypatch, tmp_path) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_auction_gap_replay

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        calls = []
        monkeypatch.setattr(
            "rquant.cli.open_readonly_store",
            lambda **kwargs: calls.append(kwargs) or store,
        )
        replay_mock = MagicMock(return_value=pd.DataFrame())
        monkeypatch.setattr(
            "rquant.auction_gap_strategy.run_auction_gap_replay",
            replay_mock,
        )

        args = MagicMock(
            start_date="2025-01-16",
            end_date="2026-06-24",
            persist_positions=False,
            run_id=None,
            gap_mode="close",
            min_ratio=0.15,
            max_ratio=5.0,
            st_filter="case_insensitive",
            output=None,
        )

        rc = cmd_auction_gap_replay(args)

        assert rc == 0
        assert calls == [{"required_tables": ["auction_bar", "daily_bar", "daily_state"]}]
        replay_mock.assert_called_once()
        assert replay_mock.call_args.args[0] is store


class TestCmdAuctionGapMinuteReplay:
    def test_uses_readonly_store_with_minute_table(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_auction_gap_minute_replay

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        calls = []
        monkeypatch.setattr(
            "rquant.cli.open_readonly_store",
            lambda **kwargs: calls.append(kwargs) or store,
        )
        candidate_mock = MagicMock(return_value=pd.DataFrame({"ts_code": ["600000.SH"]}))
        replay_mock = MagicMock(return_value=pd.DataFrame())
        monkeypatch.setattr(
            "rquant.auction_gap_strategy.run_auction_gap_replay",
            candidate_mock,
        )
        monkeypatch.setattr(
            "rquant.auction_gap_strategy.run_auction_gap_minute_replay",
            replay_mock,
        )

        args = MagicMock(
            start_date="2025-01-16",
            end_date="2026-06-24",
            persist_positions=False,
            run_id=None,
            gap_mode="close",
            min_ratio=0.15,
            max_ratio=5.0,
            st_filter="case_insensitive",
            max_hold_days=1,
            seal_hold_days=None,
            seal_hold_max_open_times=0,
            output=None,
        )

        rc = cmd_auction_gap_minute_replay(args)

        assert rc == 0
        assert calls == [{
            "required_tables": [
                "auction_bar",
                "daily_bar",
                "daily_state",
                "minute_bar",
            ]
        }]
        candidate_mock.assert_called_once()
        replay_mock.assert_called_once()


class TestCmdAuctionGapMinuteBackfill:
    def test_writes_main_store(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        from rquant.cli import cmd_auction_gap_minute_backfill

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        adapter = MagicMock()
        summary = MagicMock(failed_requests=0)
        summary.model_dump.return_value = {"planned_requests": 1}
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))
        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        backfill_mock = MagicMock(return_value=summary)
        monkeypatch.setattr(
            "rquant.intraday_backfill.backfill_auction_gap_minute_replay_window",
            backfill_mock,
        )

        args = MagicMock(
            start_date="2025-01-16",
            end_date="2026-06-24",
            persist_positions=False,
            run_id=None,
            gap_mode="close",
            min_ratio=0.15,
            max_ratio=5.0,
            st_filter="case_insensitive",
            max_hold_days=1,
            freq="1min",
            ts_code=None,
            dry_run=False,
        )

        rc = cmd_auction_gap_minute_backfill(args)

        assert rc == 0
        backfill_mock.assert_called_once()


class TestCmdAuctionMinuteFallback:
    def test_writes_fallback_rows(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        from rquant.cli import cmd_auction_minute_fallback

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        summary = MagicMock()
        summary.model_dump.return_value = {"rows_written": 1}

        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))
        fallback_mock = MagicMock(return_value=summary)
        monkeypatch.setattr(
            "rquant.auction_backfill.synthesize_open_auction_from_minute",
            fallback_mock,
        )

        args = MagicMock(date="2026-06-26", dry_run=False)
        rc = cmd_auction_minute_fallback(args)

        assert rc == 0
        fallback_mock.assert_called_once_with(
            store,
            "2026-06-26",
            dry_run=False,
        )


class TestCmdRtMinuteFetch:
    def test_fetches_and_writes_minute_bar(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_rt_minute_fetch

        adapter = MagicMock()
        adapter.rt_min.return_value = pd.DataFrame([
            {
                "ts_code": "605366.SH",
                "trade_time": pd.Timestamp("2026-07-01 15:00:00"),
                "freq": "1min",
                "open": 12.85,
                "high": 12.85,
                "low": 12.85,
                "close": 12.85,
                "vol": 1110200.0,
                "amount": 14266070.0,
                "source": "tushare_rt",
            }
        ])
        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        store.upsert_minute_bars.return_value = 1

        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))

        args = MagicMock(ts_code=["605366.SH,301051.SZ"], freq="1min")
        rc = cmd_rt_minute_fetch(args)

        assert rc == 0
        adapter.rt_min.assert_called_once_with(
            ["605366.SH", "301051.SZ"],
            freq="1min",
        )
        store.upsert_minute_bars.assert_called_once()


class TestCmdRtMinuteDailyFetch:
    def test_fetches_and_writes_open_to_now_minute_bar(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_rt_minute_daily_fetch

        adapter = MagicMock()
        adapter.rt_min_daily.return_value = pd.DataFrame([
            {
                "ts_code": "605366.SH",
                "trade_time": pd.Timestamp("2026-07-01 09:30:00"),
                "freq": "1min",
                "open": 12.80,
                "high": 12.80,
                "low": 12.80,
                "close": 12.80,
                "vol": 777300.0,
                "amount": 9949440.0,
                "source": "tushare_rt_daily",
            }
        ])
        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        store.upsert_minute_bars.return_value = 1

        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))

        args = MagicMock(ts_code=["605366.SH,301051.SZ"], freq="1min")
        rc = cmd_rt_minute_daily_fetch(args)

        assert rc == 0
        adapter.rt_min_daily.assert_called_once_with(
            ["605366.SH", "301051.SZ"],
            freq="1min",
        )
        store.upsert_minute_bars.assert_called_once()


class TestCmdMoneyflowBackfill:
    def test_fetches_and_writes_moneyflow_daily(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_moneyflow_backfill

        adapter = MagicMock()
        adapter.moneyflow.return_value = pd.DataFrame([
            {
                "ts_code": "300001.SZ",
                "trade_date": pd.Timestamp("2026-06-26").date(),
                "buy_lg_vol": 1200.0,
                "sell_lg_vol": 700.0,
                "buy_elg_vol": 500.0,
                "sell_elg_vol": 100.0,
                "large_net_vol": 900.0,
                "large_net_amount": 1234.56,
                "source": "tushare",
            }
        ])
        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        store.upsert_moneyflow_daily.return_value = 1

        monkeypatch.setattr(
            "rquant.adapter.tushare.TushareAdapter",
            MagicMock(return_value=adapter),
        )
        monkeypatch.setattr("rquant.cli.DuckDBStore", MagicMock(return_value=store))

        args = MagicMock(date="2026-06-26")
        rc = cmd_moneyflow_backfill(args)

        assert rc == 0
        adapter.moneyflow.assert_called_once()
        store.upsert_moneyflow_daily.assert_called_once()


class TestCmdGrowthBoardSurgeReplay:
    def test_runs_replay_with_readonly_store(self, monkeypatch) -> None:
        from datetime import time
        from unittest.mock import MagicMock

        import pandas as pd

        from rquant.cli import cmd_growth_board_surge_replay

        store = MagicMock()
        store.__enter__.return_value = store
        store.__exit__.return_value = None
        trades = pd.DataFrame([{
            "signal_date": "2026-06-25",
            "ts_code": "300001.SZ",
            "name": "创业样本",
            "entry_time": "2026-06-25 09:34:00",
            "entry_price": 10.8,
            "exit_time": "2026-06-26 15:00:00",
            "exit_price": 11.6,
            "exit_reason": "time_1d",
            "ret_pct": 7.4074,
        }])

        open_store = MagicMock(return_value=store)
        replay = MagicMock(return_value=trades)
        monkeypatch.setattr("rquant.cli.open_readonly_store", open_store)
        monkeypatch.setattr(
            "rquant.growth_board_surge_strategy.run_growth_board_surge_replay",
            replay,
        )

        args = MagicMock(
            start_date="2026-06-25",
            end_date="2026-06-26",
            freq="1min",
            min_signal_time="09:33",
            lookback_days=20,
            min_hist_days=10,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
            max_hold_days=1,
            output=None,
        )
        rc = cmd_growth_board_surge_replay(args)

        assert rc == 0
        open_store.assert_called_once_with(
            required_tables=[
                "daily_bar",
                "daily_indicator",
                "daily_state",
                "stock_basic",
                "minute_bar",
            ]
        )
        replay.assert_called_once()
        config = replay.call_args.kwargs["config"]
        assert config.min_signal_time == time(9, 33)
        assert config.min_cum_amount_ratio == 1.4


class TestPool2Parser:
    def test_list(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["pool2", "list"])
        assert args.command == "pool2"
        assert args.pool2_action == "list"

    def test_remove(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["pool2", "remove", "002415.SZ"])
        assert args.pool2_action == "remove"
        assert args.ts_code == "002415.SZ"


class TestNotifyTestParser:
    def test_notify_test_registered(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["notify-test"])
        assert args.command == "notify-test"


class TestCmdNotifyTest:
    def test_no_keys_returns_1(self, monkeypatch) -> None:
        from unittest.mock import MagicMock

        # Empty key_list -> should fail fast
        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "")

        rc = cmd_notify_test(MagicMock())
        assert rc == 1

    def test_pushes_and_returns_0_on_success(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1,k2")

        with patch("rquant.notify.client.requests.post") as mock_post:
            mock_post.return_value = MagicMock(json=lambda: {"code": 0})
            rc = cmd_notify_test(MagicMock())

        assert rc == 0
        assert mock_post.call_count == 2  # 两个 key 都推

    def test_partial_failure_returns_0_when_any_success(
        self, monkeypatch
    ) -> None:
        from unittest.mock import MagicMock, patch

        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1,k2")

        responses = [
            MagicMock(json=lambda: {"code": 0}),
            MagicMock(json=lambda: {"code": 1, "error": "bad key"}),
        ]
        with patch("rquant.notify.client.requests.post", side_effect=responses):
            rc = cmd_notify_test(MagicMock())
        assert rc == 0

    def test_all_fail_returns_1(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        import rquant.config as cfg_mod
        from rquant.cli import cmd_notify_test
        monkeypatch.setattr(cfg_mod.settings, "pushdeer_keys", "k1")

        with patch("rquant.notify.client.requests.post") as mock_post:
            mock_post.return_value = MagicMock(json=lambda: {"code": 1, "error": "x"})
            rc = cmd_notify_test(MagicMock())
        assert rc == 1


class TestMainErrorReporting:
    def test_main_catches_run_daily_error_and_notifies(self, monkeypatch) -> None:
        from unittest.mock import patch

        from rquant.cli import main

        # Force run-daily to raise
        def boom(_args):
            raise ValueError("test boom")

        monkeypatch.setattr("rquant.cli.cmd_run_daily", boom)

        with (
            patch("sys.argv", ["rquant", "run-daily", "--no-ingest"]),
            patch("rquant.notify.notify") as mock_notify,
        ):
            rc = main()

        assert rc == 1
        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args.kwargs
        assert mock_notify.call_args.args[0] == "error"
        assert call_kwargs["component"] == "cli:run-daily"
        assert isinstance(call_kwargs["exc"], ValueError)

    def test_main_does_not_wrap_serve(self, monkeypatch) -> None:
        """serve 内部已自处理异常，main 不再加 try/except。"""
        from unittest.mock import patch

        from rquant.cli import main

        def boom(_args):
            raise ValueError("inner")

        monkeypatch.setattr("rquant.cli.cmd_serve", boom)
        with (
            patch("sys.argv", ["rquant", "serve"]),
            patch("rquant.notify.notify") as mock_notify,
        ):
            # main 让 serve 异常直接冒出来
            import pytest

            with pytest.raises(ValueError):
                main()
            mock_notify.assert_not_called()


class TestIngestWithRetry:
    """_ingest_with_retry 的网络异常重试（6/4 真实事故：tushare ReadTimeout）。"""

    def test_network_error_retries_then_succeeds(self, monkeypatch) -> None:
        from unittest.mock import patch

        import requests

        from rquant import cli

        # 前两次抛 ReadTimeout，第三次成功返回 5000 行
        calls = []

        def flaky(_date):
            calls.append(1)
            if len(calls) < 3:
                raise requests.exceptions.ReadTimeout("boom")
            return 5000

        monkeypatch.setattr("rquant.ingest.ingest_daily", flaky)
        with patch("rquant.cli.time.sleep"):  # 跳过真实 sleep
            result = cli._ingest_with_retry("2026-06-04")

        assert result == 5000
        assert len(calls) == 3

    def test_network_error_exhausted_reraises(self, monkeypatch) -> None:
        from unittest.mock import patch

        import pytest
        import requests

        from rquant import cli

        def always_timeout(_date):
            raise requests.exceptions.ReadTimeout("persistent")

        monkeypatch.setattr("rquant.ingest.ingest_daily", always_timeout)
        with patch("rquant.cli.time.sleep"), pytest.raises(requests.exceptions.ReadTimeout):
            cli._ingest_with_retry("2026-06-04")

    def test_data_not_ready_retries_then_zero(self, monkeypatch) -> None:
        from unittest.mock import patch

        from rquant import cli

        # 始终返回 0（数据未就绪），重试用尽后返回 0（不抛）
        monkeypatch.setattr("rquant.ingest.ingest_daily", lambda _d: 0)
        with patch("rquant.cli.time.sleep"):
            result = cli._ingest_with_retry("2026-06-04")

        assert result == 0


class TestIngestRetryBusinessError:
    """_ingest_with_retry 也重试 tushare 服务端业务错误（裸 Exception，非 RequestException）。"""

    def test_business_exception_retries_then_succeeds(self, monkeypatch) -> None:
        from unittest.mock import patch

        from rquant import cli

        calls = []

        def flaky(_date):
            calls.append(1)
            if len(calls) < 2:
                # tushare 客户端业务错误抛裸 Exception（如限频/接口临时故障）
                raise Exception("抱歉，您每分钟最多访问该接口600次")
            return 5000

        monkeypatch.setattr("rquant.ingest.ingest_daily", flaky)
        with patch("rquant.cli.time.sleep"):
            result = cli._ingest_with_retry("2026-06-04")

        assert result == 5000
        assert len(calls) == 2

    def test_business_exception_exhausted_reraises(self, monkeypatch) -> None:
        from unittest.mock import patch

        import pytest

        from rquant import cli

        def always_fail(_date):
            raise Exception("接口下线")

        monkeypatch.setattr("rquant.ingest.ingest_daily", always_fail)
        with patch("rquant.cli.time.sleep"), pytest.raises(Exception, match="接口下线"):
            cli._ingest_with_retry("2026-06-04")
