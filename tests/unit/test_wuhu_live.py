from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _load_wuhu_live() -> types.ModuleType:
    module_path = Path(__file__).resolve().parents[2] / "notch_app" / "server" / "wuhu_live.py"
    restore_pyarrow = False
    if "pyarrow" not in sys.modules:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pa = types.ModuleType("pyarrow")
            pq = types.ModuleType("pyarrow.parquet")
            pa.parquet = pq  # type: ignore[attr-defined]
            sys.modules["pyarrow"] = pa
            sys.modules["pyarrow.parquet"] = pq
            restore_pyarrow = True
    try:
        spec = importlib.util.spec_from_file_location("wuhu_live_under_test", module_path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        if restore_pyarrow:
            sys.modules.pop("pyarrow.parquet", None)
            sys.modules.pop("pyarrow", None)


class TestWuhuLiveSparklineScale(unittest.TestCase):
    def test_session_min_maps_full_240_minute_axis(self) -> None:
        mod = _load_wuhu_live()
        self.assertEqual(mod.session_min("09:25"), 0)
        self.assertEqual(mod.session_min("09:30"), 0)
        self.assertEqual(mod.session_min("10:00"), 30)
        self.assertEqual(mod.session_min("10:30"), 60)
        self.assertEqual(mod.session_min("11:30"), 120)
        # Lunch break maps to exact half-width (120 / 240 = 0.5).
        self.assertEqual(mod.session_min("12:15"), 120)
        self.assertEqual(mod.session_min("13:00"), 120)
        self.assertEqual(mod.session_min("14:00"), 180)
        self.assertEqual(mod.session_min("15:00"), 240)
        self.assertEqual(mod.session_min("15:05"), 240)

    def test_payload_emits_aligned_spark_and_spark_m_for_morning_and_lunch(self) -> None:
        mod = _load_wuhu_live()
        st = mod.State()
        st.reset("2026-11-12")
        st.picks = [{"rank": 1, "name": "测试虎", "ts_code": "000001.SZ", "gap": 0.0612}]
        st.codes = ["000001.SZ"]
        st._ingest(
            [
                {"ts_code": "000001.SZ", "time": "09:30:03", "price": 10.60, "open": 10.60, "pre_close": 10.00},
                {"ts_code": "000001.SZ", "time": "10:00:05", "price": 10.80, "open": 10.60, "pre_close": 10.00},
                {"ts_code": "000001.SZ", "time": "11:30:00", "price": 11.00, "open": 10.60, "pre_close": 10.00},
            ],
            True,
        )
        body = st.payload(replay=False)
        self.assertEqual(len(body["wuhu"]), 1)
        row = body["wuhu"][0]
        self.assertEqual(row["spark"], [10.60, 10.80, 11.00])
        self.assertEqual(row["spark_m"], [0, 30, 120])
        # At morning close / lunch break, last point sits at 120 / 240 = 50% of full-day width.
        self.assertAlmostEqual(row["spark_m"][-1] / 240.0, 0.5)

    def test_payload_replay_truncates_spark_and_spark_m_together(self) -> None:
        mod = _load_wuhu_live()
        st = mod.State()
        st.reset("2026-11-12")
        st.picks = [{"rank": 1, "name": "测试虎", "ts_code": "000001.SZ", "gap": 0.05}]
        st.codes = ["000001.SZ"]
        st._ingest(
            [
                {"ts_code": "000001.SZ", "time": "09:30:03", "price": 10.50, "open": 10.50, "pre_close": 10.00},
                {"ts_code": "000001.SZ", "time": "10:00:05", "price": 10.70, "open": 10.50, "pre_close": 10.00},
                {"ts_code": "000001.SZ", "time": "14:00:00", "price": 10.90, "open": 10.50, "pre_close": 10.00},
            ],
            True,
        )
        st.replay_alerts = lambda: [  # type: ignore[method-assign]
            {"time": "10:00:12", "ts_code": "000001.SZ", "name": "测试虎", "type": "突破阻力", "level": 1, "text": ""}
        ]
        body = st.payload(replay=True)
        row = body["wuhu"][0]
        self.assertTrue(body["replay"])
        self.assertEqual(row["spark"], [10.50, 10.70])
        self.assertEqual(row["spark_m"], [0, 30])


if __name__ == "__main__":
    unittest.main()
