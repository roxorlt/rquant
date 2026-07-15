"""历史分钟数据适配器测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd


class _FakePro:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def stk_mins(
        self,
        *,
        ts_code: str,
        freq: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        self.calls.append({
            "ts_code": ts_code,
            "freq": freq,
            "start_date": start_date,
            "end_date": end_date,
        })
        return pd.DataFrame([
            {
                "ts_code": "600000.SH",
                "trade_time": "2023-08-25 09:31:00",
                "close": 7.02,
                "open": 6.99,
                "high": 7.02,
                "low": 6.97,
                "vol": 807500.0,
                "amount": 5649956.0,
            },
            {
                "ts_code": "600000.SH",
                "trade_time": "2023-08-25 09:30:00",
                "close": 6.99,
                "open": 6.99,
                "high": 6.99,
                "low": 6.99,
                "vol": 103700.0,
                "amount": 724863.0,
            },
        ])

    def stk_auction(self, *, trade_date: str, fields: str) -> pd.DataFrame:
        self.calls.append({
            "trade_date": trade_date,
            "fields": fields,
        })
        return pd.DataFrame([
            {
                "ts_code": "600000.SH",
                "trade_date": "20250218",
                "vol": 28355900.0,
                "price": 9.81,
                "amount": 278113479.0,
                "turnover_rate": 0.1,
                "volume_ratio": 3.2,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20250218",
                "vol": 1200000.0,
                "price": 11.23,
                "amount": 13476000.0,
                "turnover_rate": 0.03,
                "volume_ratio": 1.1,
            },
        ])

    def rt_min(self, *, ts_code: str, freq: str) -> pd.DataFrame:
        self.calls.append({
            "ts_code": ts_code,
            "freq": freq,
        })
        return pd.DataFrame([
            {
                "ts_code": "600000.SH",
                "freq": "1MIN",
                "time": "2026-07-01 14:59:00",
                "open": 8.85,
                "close": 8.86,
                "high": 8.87,
                "low": 8.84,
                "vol": 120000.0,
                "amount": 1063200.0,
            },
            {
                "ts_code": "000001.SZ",
                "freq": "1MIN",
                "time": "2026-07-01 14:59:00",
                "open": 10.2,
                "close": 10.22,
                "high": 10.23,
                "low": 10.18,
                "vol": 220000.0,
                "amount": 2248400.0,
            },
        ])

    def rt_min_daily(self, *, ts_code: str, freq: str) -> pd.DataFrame:
        self.calls.append({
            "rt_min_daily_ts_code": ts_code,
            "freq": freq,
        })
        return pd.DataFrame([
            {
                "code": ts_code,
                "freq": "1MIN",
                "time": "2026-07-01 09:30:00",
                "open": 12.80,
                "close": 12.80,
                "high": 12.80,
                "low": 12.80,
                "vol": 777300.0,
                "amount": 9949440.0,
            },
            {
                "code": ts_code,
                "freq": "1MIN",
                "time": "2026-07-01 09:31:00",
                "open": 12.83,
                "close": 12.82,
                "high": 12.88,
                "low": 12.72,
                "vol": 1811000.0,
                "amount": 23181497.0,
            },
        ])

    def moneyflow(self, *, trade_date: str, fields: str) -> pd.DataFrame:
        self.calls.append({
            "trade_date": trade_date,
            "fields": fields,
        })
        return pd.DataFrame([
            {
                "ts_code": "300001.SZ",
                "trade_date": "20260626",
                "buy_lg_vol": 1200.0,
                "sell_lg_vol": 700.0,
                "buy_elg_vol": 500.0,
                "sell_elg_vol": 100.0,
                "net_mf_vol": 900.0,
                "net_mf_amount": 1234.56,
            }
        ])


def test_stk_mins_normalizes_tushare_rows(monkeypatch) -> None:
    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    fake = _FakePro()
    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: fake)

    adapter = TushareAdapter(token="x" * 32)
    df = adapter.stk_mins(
        "600000.SH",
        "1min",
        datetime(2023, 8, 25, 9, 30),
        datetime(2023, 8, 25, 15, 0),
    )

    assert fake.calls == [{
        "ts_code": "600000.SH",
        "freq": "1min",
        "start_date": "2023-08-25 09:30:00",
        "end_date": "2023-08-25 15:00:00",
    }]
    assert df["trade_time"].tolist() == [
        pd.Timestamp("2023-08-25 09:30:00"),
        pd.Timestamp("2023-08-25 09:31:00"),
    ]
    assert df["freq"].tolist() == ["1min", "1min"]
    assert df.iloc[0]["vol"] == 103700.0
    assert df.iloc[1]["amount"] == 5649956.0


def test_stk_mins_retries_transient_failures_and_normalizes(monkeypatch) -> None:
    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    class _FlakyPro(_FakePro):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        def stk_mins(
            self,
            *,
            ts_code: str,
            freq: str,
            start_date: str,
            end_date: str,
        ) -> pd.DataFrame:
            self.attempts += 1
            if self.attempts == 1:
                raise Exception("频率超限")
            if self.attempts == 2:
                raise Exception("temporary network error")
            return super().stk_mins(
                ts_code=ts_code,
                freq=freq,
                start_date=start_date,
                end_date=end_date,
            )

    fake = _FlakyPro()
    tokens: list[str] = []
    sleeps: list[float] = []

    def fake_pro_api(token: str) -> _FlakyPro:
        tokens.append(token)
        return fake

    monkeypatch.setattr(tushare_module.settings, "tushare_token_backup", "backup")
    monkeypatch.setattr(tushare_module.ts, "pro_api", fake_pro_api)
    monkeypatch.setattr(tushare_module.time, "sleep", sleeps.append)

    adapter = TushareAdapter(token="primary")
    df = adapter.stk_mins(
        "600000.SH",
        "1min",
        datetime(2023, 8, 25, 9, 30),
        datetime(2023, 8, 25, 15, 0),
    )

    assert fake.attempts == 3
    assert tokens == ["primary"]
    assert sleeps == [25.0, 5.0]
    assert df["trade_time"].tolist() == [
        pd.Timestamp("2023-08-25 09:30:00"),
        pd.Timestamp("2023-08-25 09:31:00"),
    ]
    assert df["freq"].tolist() == ["1min", "1min"]
    assert df["source"].tolist() == ["tushare", "tushare"]


def test_stk_mins_rejects_unsupported_freq(monkeypatch) -> None:
    import pytest

    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: _FakePro())
    adapter = TushareAdapter(token="x" * 32)

    with pytest.raises(ValueError, match="freq"):
        adapter.stk_mins(
            "600000.SH",
            "2min",
            datetime(2023, 8, 25, 9, 30),
            datetime(2023, 8, 25, 15, 0),
        )


def test_rt_min_normalizes_latest_realtime_minute(monkeypatch) -> None:
    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    fake = _FakePro()
    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: fake)

    adapter = TushareAdapter(token="x" * 32)
    df = adapter.rt_min(["600000.SH", "000001.SZ"], freq="1min")

    assert fake.calls[-1] == {
        "ts_code": "600000.SH,000001.SZ",
        "freq": "1MIN",
    }
    assert df["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    assert df["trade_time"].tolist() == [
        pd.Timestamp("2026-07-01 14:59:00"),
        pd.Timestamp("2026-07-01 14:59:00"),
    ]
    assert df["freq"].tolist() == ["1min", "1min"]
    assert df["source"].tolist() == ["tushare_rt", "tushare_rt"]


def test_rt_min_rejects_unsupported_freq(monkeypatch) -> None:
    import pytest

    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: _FakePro())
    adapter = TushareAdapter(token="x" * 32)

    with pytest.raises(ValueError, match="freq"):
        adapter.rt_min(["600000.SH"], freq="2min")


def test_rt_min_daily_normalizes_open_to_now_minutes(monkeypatch) -> None:
    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    fake = _FakePro()
    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: fake)

    adapter = TushareAdapter(token="x" * 32)
    df = adapter.rt_min_daily(["605366.SH", "301051.SZ"], freq="1min")

    assert fake.calls[-2:] == [
        {"rt_min_daily_ts_code": "605366.SH", "freq": "1MIN"},
        {"rt_min_daily_ts_code": "301051.SZ", "freq": "1MIN"},
    ]
    assert df["ts_code"].tolist() == [
        "301051.SZ",
        "301051.SZ",
        "605366.SH",
        "605366.SH",
    ]
    assert df["trade_time"].iloc[0] == pd.Timestamp("2026-07-01 09:30:00")
    assert df["freq"].unique().tolist() == ["1min"]
    assert df["source"].unique().tolist() == ["tushare_rt_daily"]


def test_rt_min_daily_rejects_unsupported_freq(monkeypatch) -> None:
    import pytest

    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: _FakePro())
    adapter = TushareAdapter(token="x" * 32)

    with pytest.raises(ValueError, match="freq"):
        adapter.rt_min_daily(["600000.SH"], freq="2min")


def test_moneyflow_normalizes_daily_fund_flow(monkeypatch) -> None:
    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    fake = _FakePro()
    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: fake)

    adapter = TushareAdapter(token="x" * 32)
    df = adapter.moneyflow(date(2026, 6, 26))

    assert fake.calls[-1]["trade_date"] == "20260626"
    assert "net_mf_amount" in fake.calls[-1]["fields"]
    assert df.iloc[0]["trade_date"] == date(2026, 6, 26)
    assert df.iloc[0]["large_net_vol"] == 900.0
    assert df.iloc[0]["large_net_amount"] == 1234.56
    assert df.iloc[0]["source"] == "tushare"


def test_stk_mins_does_not_switch_to_backup_token(monkeypatch) -> None:
    import pytest

    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    class _FailingPro:
        def stk_mins(self, **_kwargs) -> pd.DataFrame:
            raise Exception("rate limit")

    tokens: list[str] = []

    def fake_pro_api(token: str):
        tokens.append(token)
        return _FailingPro()

    monkeypatch.setattr(tushare_module.settings, "tushare_token_backup", "backup")
    monkeypatch.setattr(tushare_module.ts, "pro_api", fake_pro_api)
    sleeps: list[float] = []
    monkeypatch.setattr(tushare_module.time, "sleep", sleeps.append)

    adapter = TushareAdapter(token="primary")
    with pytest.raises(RuntimeError, match="stk_mins"):
        adapter.stk_mins(
            "600000.SH",
            "1min",
            datetime(2023, 8, 25, 9, 30),
            datetime(2023, 8, 25, 15, 0),
        )

    assert tokens == ["primary"]
    assert sleeps == [5.0] * 5


def test_stk_auction_normalizes_tushare_rows(monkeypatch) -> None:
    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    fake = _FakePro()
    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: fake)

    adapter = TushareAdapter(token="x" * 32)
    df = adapter.stk_auction(date(2025, 2, 18))

    assert fake.calls[-1] == {
        "trade_date": "20250218",
        "fields": "ts_code,trade_date,vol,price,amount,turnover_rate,volume_ratio",
    }
    assert df["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    assert df["trade_date"].tolist() == [
        date(2025, 2, 18),
        date(2025, 2, 18),
    ]
    assert df["auction_type"].tolist() == ["open_realtime", "open_realtime"]
    assert df["source"].tolist() == ["tushare", "tushare"]
    assert df.iloc[0]["price"] == 11.23


def test_stk_auction_rejects_missing_required_columns(monkeypatch) -> None:
    import pytest

    from rquant.adapter import tushare as tushare_module
    from rquant.adapter.tushare import TushareAdapter

    class _BadPro:
        def stk_auction(self, *, trade_date: str, fields: str) -> pd.DataFrame:
            return pd.DataFrame([{"ts_code": "600000.SH"}])

    monkeypatch.setattr(tushare_module.ts, "pro_api", lambda token: _BadPro())
    adapter = TushareAdapter(token="x" * 32)

    with pytest.raises(RuntimeError, match="stk_auction 返回缺字段"):
        adapter.stk_auction(date(2025, 2, 18))
