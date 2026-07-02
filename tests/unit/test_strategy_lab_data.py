"""策略实验室数据辅助函数测试。"""

from __future__ import annotations

from datetime import date

import pandas as pd


def test_safe_replay_end_date_keeps_full_exit_window() -> None:
    from rquant.dashboard.strategy_lab_data import safe_replay_end_date

    calendar = [
        date(2026, 6, 15),
        date(2026, 6, 16),
        date(2026, 6, 17),
        date(2026, 6, 18),
        date(2026, 6, 19),
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
    ]

    assert safe_replay_end_date(calendar, date(2026, 6, 24), max_hold_days=1) == date(
        2026,
        6,
        22,
    )
    assert safe_replay_end_date(calendar, date(2026, 6, 24), max_hold_days=3) == date(
        2026,
        6,
        18,
    )
    assert safe_replay_end_date(calendar, date(2026, 6, 24), max_hold_days=5) == date(
        2026,
        6,
        16,
    )


def test_safe_replay_end_date_respects_pool_max_date() -> None:
    from rquant.dashboard.strategy_lab_data import safe_replay_end_date

    calendar = [
        date(2026, 6, 15),
        date(2026, 6, 16),
        date(2026, 6, 17),
        date(2026, 6, 18),
        date(2026, 6, 19),
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
    ]

    assert safe_replay_end_date(calendar, date(2026, 6, 17), max_hold_days=1) == date(
        2026,
        6,
        17,
    )


def test_format_tushare_catalog_display_decodes_json_fields() -> None:
    from rquant.dashboard.strategy_lab_data import format_tushare_catalog_display

    df = pd.DataFrame([
        {
            "doc_id": 374,
            "title": "A股实时分钟",
            "api_name": "rt_min",
            "priority": 1,
            "integration_status": "recommended",
            "integration_stage": "stage_1_realtime",
            "update_cadence": "intraday_realtime",
            "target_table_hint": "minute_bar",
            "permission_level": "official_permission",
            "strategy_value": "盘中监控、实时触发、模拟盘",
            "category_path": '["股票数据", "行情数据", "实时分钟"]',
            "capability_tags": '["intraday_realtime"]',
            "limit_note": "单次最大1000行",
            "permission_note": "正式权限请参阅 权限说明",
        }
    ])

    display = format_tushare_catalog_display(df)

    assert display.iloc[0]["阶段"] == "1 盘中/竞价"
    assert display.iloc[0]["路径"] == "股票数据 > 行情数据 > 实时分钟"
    assert display.iloc[0]["能力"] == "intraday_realtime"
    assert display.iloc[0]["权限"] == "正式权限"


def test_format_tushare_catalog_display_shows_quote_and_points_gap() -> None:
    from rquant.dashboard.strategy_lab_data import format_tushare_catalog_display

    df = pd.DataFrame([
        {
            "doc_id": 374,
            "title": "A股实时分钟",
            "api_name": "rt_min",
            "priority": 1,
            "integration_status": "recommended",
            "integration_stage": "stage_1_realtime",
            "update_cadence": "intraday_realtime",
            "target_table_hint": "minute_bar",
            "permission_level": "official_permission",
            "strategy_value": "盘中监控、实时触发、模拟盘",
            "category_path": '["股票数据", "行情数据", "实时分钟"]',
            "capability_tags": '["intraday_realtime"]',
            "limit_note": "单次可同时请求300个公司",
            "permission_note": "需 5000 积分或单独购买权限",
            "permission_good_name": "A股分钟RT",
            "permission_price": 1000.0,
            "permission_duration_unit": "1M",
            "permission_api_count": 2,
            "required_points": 5000,
            "current_points": 2173,
            "points_gap": 2827,
            "estimated_points_cost": 282.7,
        }
    ])

    display = format_tushare_catalog_display(df)

    assert display.iloc[0]["报价"] == "A股分钟RT ¥1,000/1M（2个接口）"
    assert display.iloc[0]["积分缺口"] == "缺 2,827（约 ¥282.70）"


def test_auction_gap_metric_rows_compares_baseline_and_minute_replay() -> None:
    from rquant.dashboard.strategy_lab_data import auction_gap_metric_rows

    baseline = pd.DataFrame({
        "ts_code": ["600000.SH", "000001.SZ"],
        "next_open_ret_pct": [2.0, -1.0],
        "hit_limit_up_today": [True, False],
        "intraday_high_ret_pct": [8.0, 1.0],
        "day_close_ret_pct": [5.0, -2.0],
    })
    minute = pd.DataFrame({
        "ts_code": ["600000.SH"],
        "ret_pct": [3.0],
        "b_hit_limit_up_today": [True],
        "exit_reason": ["next_auction_weak"],
    })

    rows = auction_gap_metric_rows(baseline, minute)

    assert rows.iloc[0]["策略"] == "竞价直接B/次日开盘S"
    assert rows.iloc[0]["候选"] == 2
    assert rows.iloc[0]["交易"] == 2
    assert rows.iloc[0]["触发率%"] == 100.0
    assert rows.iloc[0]["当日上板率%"] == 50.0
    assert rows.iloc[0]["当日最高均值%"] == 4.5
    assert rows.iloc[0]["当日收盘均值%"] == 1.5
    assert rows.iloc[0]["平均收益%"] == 0.5
    assert rows.iloc[1]["策略"] == "竞价候选/分钟B/S"
    assert rows.iloc[1]["候选"] == 2
    assert rows.iloc[1]["交易"] == 1
    assert rows.iloc[1]["触发率%"] == 50.0
    assert rows.iloc[1]["当日上板率%"] == 100.0
    assert rows.iloc[1]["弱竞价退出%"] == 100.0


def test_growth_board_metric_rows_summarizes_minute_replay() -> None:
    from rquant.dashboard.strategy_lab_data import growth_board_metric_rows

    trades = pd.DataFrame({
        "ts_code": ["300001.SZ", "688001.SH", "300002.SZ"],
        "ret_pct": [10.0, -2.0, 1.0],
        "hit_limit_up_today": [True, False, True],
    })

    rows = growth_board_metric_rows(trades)

    assert rows.iloc[0]["策略"] == "科创/创业放量追击"
    assert rows.iloc[0]["交易"] == 3
    assert rows.iloc[0]["当日上板率%"] == 66.6667
    assert rows.iloc[0]["平均收益%"] == 3.0
    assert rows.iloc[0]["中位收益%"] == 1.0
    assert rows.iloc[0]["胜率%"] == 66.6667


def test_growth_board_metric_rows_accepts_strategy_name() -> None:
    from rquant.dashboard.strategy_lab_data import growth_board_metric_rows

    rows = growth_board_metric_rows(pd.DataFrame(), strategy_name="去掉VWAP")

    assert rows.iloc[0]["策略"] == "去掉VWAP"
    assert rows.iloc[0]["交易"] == 0


def test_growth_board_ablation_specs_include_independent_filters() -> None:
    from rquant.dashboard.strategy_lab_data import growth_board_ablation_specs

    specs = growth_board_ablation_specs()
    by_key = {spec.key: spec for spec in specs}

    assert list(by_key) == [
        "full",
        "no_vwap",
        "no_same_minute",
        "no_accel_5m",
        "cum_only",
    ]
    assert by_key["full"].require_vwap_strength is True
    assert by_key["no_vwap"].require_vwap_strength is False
    assert by_key["no_same_minute"].use_same_minute_surge is False
    assert by_key["no_accel_5m"].use_accel_surge is False
    assert by_key["cum_only"].use_same_minute_surge is False
    assert by_key["cum_only"].use_accel_surge is False


def test_estimate_growth_board_workload_counts_ablation_variants() -> None:
    from rquant.dashboard.strategy_lab_data import estimate_growth_board_workload

    estimate = estimate_growth_board_workload(
        candidate_count=100,
        variant_count=5,
        seconds_per_candidate_pass=0.02,
        seconds_per_variant=0.5,
    )

    assert estimate.candidate_count == 100
    assert estimate.variant_count == 5
    assert estimate.candidate_passes == 500
    assert estimate.estimated_seconds == 12.5
    assert estimate.estimated_label == "约 12 秒"


def test_dataframe_preview_marks_truncated_rows() -> None:
    from rquant.dashboard.strategy_lab_data import dataframe_preview

    df = pd.DataFrame({"x": range(5)})

    preview, truncated = dataframe_preview(df, max_rows=3)

    assert truncated is True
    assert preview["x"].tolist() == [0, 1, 2]
    assert len(df) == 5


def test_estimate_strategy_optimization_workload_counts_replay_cost() -> None:
    from rquant.dashboard.strategy_lab_data import estimate_strategy_optimization_workload

    estimate = estimate_strategy_optimization_workload(
        candidate_count=100,
        entry_mode_count=2,
        profile_variant_count=3,
        hold_count=4,
        topn_count=2,
        score_profile_count=5,
        walk_forward_folds=0,
        seconds_per_candidate_pass=0.01,
        seconds_per_replay_run=0.5,
        seconds_per_topn_combination=0.001,
    )

    assert estimate.replay_runs == 48
    assert estimate.replay_candidate_passes == 2400
    assert estimate.topn_combinations == 480
    assert estimate.walk_forward_topn_combinations == 0
    assert estimate.estimated_seconds == 48.48
    assert estimate.estimated_label == "约 48 秒"


def test_estimate_strategy_optimization_workload_counts_walk_forward_cost() -> None:
    from rquant.dashboard.strategy_lab_data import estimate_strategy_optimization_workload

    estimate = estimate_strategy_optimization_workload(
        candidate_count=50,
        entry_mode_count=1,
        profile_variant_count=2,
        hold_count=3,
        topn_count=2,
        score_profile_count=4,
        walk_forward_folds=5,
        seconds_per_candidate_pass=0.01,
        seconds_per_replay_run=0.5,
        seconds_per_topn_combination=0.001,
    )

    assert estimate.replay_runs == 18
    assert estimate.replay_candidate_passes == 600
    assert estimate.topn_combinations == 96
    assert estimate.walk_forward_topn_combinations == 480
    assert estimate.estimated_seconds == 15.576
