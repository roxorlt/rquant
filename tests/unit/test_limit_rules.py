from datetime import date

import numpy as np

from rquant.limit_rules import abnormal_threshold_3d, board_of, limit_pct, limit_pct_vec


def test_main_st_switch_2026_07_06() -> None:
    assert limit_pct(True, "main", date(2026, 7, 3)) == 0.05
    assert limit_pct(True, "main", date(2026, 7, 6)) == 0.10
    assert limit_pct(True, "gem", date(2026, 7, 3)) == 0.20
    assert limit_pct(True, "gem", date(2020, 8, 21)) == 0.05
    assert limit_pct(False, "bj", date(2026, 7, 3)) == 0.30


def test_vec_matches_scalar() -> None:
    codes = ["600000.SH", "000001.SZ", "300001.SZ", "688001.SH", "830001.BJ", "600001.SH"]
    names = ["浦发", "*ST平安", "ST特锐", "ST科创", "北交", "SST某"]
    for d in (date(2026, 7, 3), date(2026, 7, 6), date(2019, 1, 2)):
        got = limit_pct_vec(codes, np.array(names, dtype=object), d)
        exp = [limit_pct("ST" in n, board_of(c), d) for c, n in zip(codes, names, strict=True)]
        assert got.tolist() == exp


def test_abnormal_threshold() -> None:
    assert abnormal_threshold_3d("main", True, date(2026, 7, 3)) == 12.0
    assert abnormal_threshold_3d("main", True, date(2026, 7, 6)) == 20.0
    assert abnormal_threshold_3d("star", False) == 30.0
