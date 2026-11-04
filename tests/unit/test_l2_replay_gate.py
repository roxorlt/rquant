"""Fast, offline self-checks for the one-command replay gate.

The gate itself runs the full 7-scenario benchmark; these checks only exercise
its contracts on tiny tiers (2 stocks, sub-second push windows) so the whole
file stays well below the 5 s budget. Everything is local: the replay pack and
the gate's own synthetic private inputs, no network and no credentials.
"""
from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.l2 import replay_gate
from rquant.l2._codec import _validate_bootstrap
from rquant.l2._request import parse_request_body, parse_trade_request_body
from rquant.l2.models import Binding, SubscriptionReceipt, SubscriptionStream
from rquant.l2.replay_source import FaultInjection

PACK = Path("/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/20261016-live-6stream")


def _mini_scenario() -> replay_gate.ScenarioSpec:
    return replay_gate.ScenarioSpec(name="healthy-2", stocks=2)


def test_tier_symbols_follow_the_real_40_then_synthetic_placeholders() -> None:
    tier100 = replay_gate.tier_symbols(100)
    assert len(tier100.symbols) == 100
    assert len(set(tier100.symbols)) == 100
    assert tier100.symbols[:2] == ("603258", "300006")
    assert tier100.real == replay_gate.REAL_TIER
    assert len(tier100.synthetic) == 60
    assert tier100.synthetic == tuple(f"{690000 + index:06d}" for index in range(60))
    assert replay_gate.tier_symbols(40).synthetic == ()
    assert replay_gate.tier_symbols(2).real == ("603258", "300006")
    for stocks in (0, 101, True, "40"):
        with pytest.raises(replay_gate.GateError):
            replay_gate.tier_symbols(stocks)  # type: ignore[arg-type]


def test_scale40_fault_baseline_is_wired_into_the_default_scenarios() -> None:
    by_name = {spec.name: spec for spec in replay_gate.default_scenarios()}
    assert list(by_name) == ["healthy-40", "healthy-80", "healthy-100",
                             "fault-seed-24of40", "fault-single-silent",
                             "fault-single-stall", "fault-seed-latency-40",
                             "fault-seed-latency-40-batched", "determinism-40"]
    seed_baseline = replay_gate._fault_baseline(by_name["fault-seed-24of40"])
    assert seed_baseline == {"seeded_streams": 72, "seeded_stocks": 24,
                             "failed_streams": 48, "seed_queries": 120}
    silent_baseline = replay_gate._fault_baseline(by_name["fault-single-silent"])
    assert silent_baseline is not None
    assert (silent_baseline["seeded_streams"], silent_baseline["failed_streams"]) == (117, 3)
    stall_baseline = replay_gate._fault_baseline(by_name["fault-single-stall"])
    assert stall_baseline is not None and stall_baseline["seeded_streams"] == 120
    latency_baseline = by_name["fault-seed-latency-40"]
    assert latency_baseline.connections == 1 and not latency_baseline.flow_control
    assert replay_gate._expected_latency_seed_shape(latency_baseline) == (72, 24, 48)
    latency_fixed = by_name["fault-seed-latency-40-batched"]
    assert latency_fixed.connections == 4 and latency_fixed.flow_control
    assert replay_gate._expected_latency_seed_shape(latency_fixed) == (120, 40, 0)
    assert replay_gate._expected_synthetic_streams(40) == 114
    assert replay_gate._expected_synthetic_streams(100) == 294
    assert replay_gate._expected_synthetic_streams(2) == 0
    spec = by_name["fault-seed-24of40"]
    assert replay_gate._expected_seed_shape(
        spec, replay_gate.tier_symbols(40)) == (72, 24, 48)


def test_synthetic_inputs_are_owner_only_and_parse_as_collector_inputs(
        tmp_path: Path) -> None:
    paths = replay_gate.write_synthetic_inputs(tmp_path / "inputs")
    assert set(paths) == {"bootstrap", "order", "trade"}
    for path in paths.values():
        info = path.stat()
        assert stat.S_ISREG(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert info.st_uid == os.getuid()
    _validate_bootstrap(paths["bootstrap"].read_bytes())
    order = parse_request_body(paths["order"].read_bytes())
    assert order["kind"] == "order_query" and order["header_length"] == 77
    assert order["header_extension"] == bytes(range(53))
    trade = parse_trade_request_body(paths["trade"].read_bytes())
    assert trade["kind"] == "vendor4013" and trade["header_length"] == 77
    assert replay_gate.synthetic_input_bytes() == replay_gate.synthetic_input_bytes()
    external = replay_gate._prepare_inputs(tmp_path / "inputs", tmp_path / "work")
    assert external.synthetic is False and external.source == "external"
    assert external.published()["source"] == "external"
    assert external.bootstrap == paths["bootstrap"]
    with pytest.raises(replay_gate.GateError):
        replay_gate._prepare_inputs(tmp_path / "missing-inputs", tmp_path / "work")


def test_gate_metric_contracts_on_a_two_stock_tier(tmp_path: Path) -> None:
    out = tmp_path / "report.json"
    report = replay_gate.run_gate(PACK, out=out, scenarios=(_mini_scenario(),),
                                  push_window_seconds=0.5,
                                  reference_dir=tmp_path / "no-reference")
    assert report["schema"] == "rquant-l2-replay-gate-v1"
    assert report["pack"]["streams"] == 6
    assert report["inputs"]["synthetic"] is True
    assert report["gate"]["status"] == "pass"
    assert report["gate"]["failed"] == [] and report["gate"]["scenario_count"] == 1
    assert report["gate"]["budget_ms"] == 180_000
    metrics = report["metrics"]
    assert set(metrics) == {"seed_first_packet_rate", "consumption_latency_p99_us",
                            "disk_write_mb_per_second", "memory"}
    assert metrics["seed_first_packet_rate"] == 1.0
    assert isinstance(metrics["consumption_latency_p99_us"], int)
    assert metrics["disk_write_mb_per_second"] > 0
    assert set(metrics["memory"]) == {"curve", "max_mb", "p50_mb"}
    assert metrics["memory"]["curve"] and metrics["memory"]["max_mb"] > 0

    scenario = report["scenarios"][0]
    assert scenario["name"] == "healthy-2" and scenario["status"] == "pass"
    assert all(expectation["pass"] for expectation in scenario["expectations"])
    for expectation in scenario["expectations"]:
        assert set(expectation) == {"name", "expected", "actual", "pass"}
    scenario_metrics = scenario["metrics"]
    assert scenario_metrics["batches"] == (
        scenario_metrics["seeded_streams"] + scenario["config"]["planned_push_frames"])
    assert scenario_metrics["consumption_latency_us"]["samples"] == scenario_metrics["batches"]
    assert scenario_metrics["status"]["state"] == "stopped"
    assert scenario_metrics["status"]["error_code"] == "duration_limit"
    assert scenario["config"]["synthetic_streams"] == 0
    assert scenario["config"]["connections"] == 1
    assert scenario["config"]["flow_control"] is False
    assert scenario["config"]["faults"] == {
        "withhold_seed_from_stock": None, "withhold_seed_codes": [],
        "stall_codes": [], "withhold_bootstrap_ack": False,
        "seed_latency_onset_pages": None, "seed_latency_coefficient": None}
    assert any("合成" in note for note in report["notes"])
    assert json.loads(out.read_text(encoding="utf-8"))["gate"]["status"] == "pass"


def test_gate_expects_blocked_seed_timeout_when_every_seed_is_withheld(
        tmp_path: Path) -> None:
    spec = replay_gate.ScenarioSpec(name="all-withheld-2", stocks=2,
                                    faults=FaultInjection(withhold_seed_from_stock=0))
    report = replay_gate.run_gate(PACK, out=tmp_path / "withheld.json", scenarios=(spec,),
                                  push_window_seconds=0.5,
                                  reference_dir=tmp_path / "no-reference")
    assert report["gate"]["status"] == "pass"
    scenario = report["scenarios"][0]
    status_metrics = scenario["metrics"]["status"]
    assert status_metrics["state"] == "blocked"
    assert status_metrics["error_code"] == "seed_timeout"
    assert scenario["metrics"]["seeded_streams"] == 0
    assert scenario["metrics"]["seed_first_packet_rate"] == 0.0
    assert scenario["metrics"]["batches"] == scenario["config"]["planned_push_frames"]
    passed = {expectation["name"]: expectation["pass"]
              for expectation in scenario["expectations"]}
    assert passed["state"] and passed["error_code"] and passed["batches"]


def test_gate_reports_failure_instead_of_aborting(tmp_path: Path,
                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(replay_gate, "_expected_seed_shape",
                        lambda spec, tier: (0, 0, 3 * spec.stocks))
    out = tmp_path / "failed.json"
    report = replay_gate.run_gate(PACK, out=out, scenarios=(_mini_scenario(),),
                                  push_window_seconds=0.5,
                                  reference_dir=tmp_path / "no-reference")
    assert report["gate"]["status"] == "fail"
    assert report["gate"]["failed"] == ["healthy-2"]
    scenario = report["scenarios"][0]
    assert scenario["status"] == "fail"
    assert any(expectation["name"] == "seeded_streams" and not expectation["pass"]
               for expectation in scenario["expectations"])
    assert json.loads(out.read_text(encoding="utf-8"))["gate"]["status"] == "fail"


def test_seed_metrics_use_seed_sequence_not_stream_state() -> None:
    receipt = SubscriptionReceipt(
        epoch_id="synthetic-epoch", desired_revision=0, collector_state="stopped",
        streams=(
            SubscriptionStream(
                binding=Binding(page_id=10117, kind="vendor4013", code="603258", market=17),
                state="first_push_observed", request_id=3, seed_sequence=5,
                seed_received_at=datetime.now(UTC), first_push_sequence=9),
            # This one timed out, then received a push: state says observed but
            # there is no seed receipt, so it must not count as seeded.
            SubscriptionStream(
                binding=Binding(page_id=10118, kind="vendor4027", code="603258", market=17,
                                side="buy"),
                state="first_push_observed", request_id=4, first_push_sequence=10),
            SubscriptionStream(
                binding=Binding(page_id=10127, kind="vendor4013", code="300006", market=33),
                state="failed", request_id=5),
        ))
    metrics = replay_gate._seed_metrics(receipt, streams=6, stocks=2)
    assert metrics["streams_total"] == 3
    assert metrics["seeded_streams"] == 1
    assert metrics["stocks_total"] == 2 and metrics["stocks_seeded"] == 1
    assert metrics["seed_timeout_streams"] == 2
    assert metrics["seed_first_packet_rate"] == round(1 / 3, 6)
    assert replay_gate._seed_metrics(None, streams=6, stocks=2) == {
        "streams_total": 6, "stocks_total": 2, "seeded_streams": 0, "stocks_seeded": 0,
        "seed_timeout_streams": 6, "seed_first_packet_rate": 0.0}


def test_latency_percentiles_use_nearest_rank_microseconds() -> None:
    assert replay_gate._percentiles_us(()) == {
        "p50": 0, "p95": 0, "p99": 0, "max": 0, "samples": 0}
    samples = [1_000 * (index + 1) for index in range(100)]
    percentiles = replay_gate._percentiles_us(samples)
    assert percentiles == {"p50": 50, "p95": 95, "p99": 99, "max": 100, "samples": 100}
    assert replay_gate._percentiles_us([2_500])["p50"] == 3
    assert replay_gate._percentiles_us([1_499])["max"] == 1


def test_memory_sampler_falls_back_when_ps_is_denied(
        monkeypatch: pytest.MonkeyPatch) -> None:
    def denied(pid: int) -> int | None:
        raise replay_gate._RssProbeError("ps_exec_denied")

    monkeypatch.setattr(replay_gate, "_ps_rss_kb", denied)
    reader, method, reason = replay_gate._resolve_rss_reader(os.getpid())
    assert method != "ps" and reason == "ps_exec_denied"
    assert reader() is not None and reader() > 0  # type: ignore[operator]
    summary = replay_gate._summarize_memory(
        [(0, 10.0), (250, 12.5), (500, 11.0)], method=method, fallback_reason=reason)
    assert summary["curve"] == [[0, 10.0], [250, 12.5], [500, 11.0]]
    assert summary["max_mb"] == 12.5 and summary["p50_mb"] == 11.0
    assert summary["samples"] == 3 and summary["method"] == method


def test_cli_run_subcommand_writes_a_passing_single_scenario_report(
        tmp_path: Path) -> None:
    out = tmp_path / "run.json"
    exit_code = replay_gate.main(["run", "--pack", str(PACK), "--stocks", "40",
                                  "--fault", "single-stall", "--out", str(out)])
    assert exit_code == 0
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["schema"] == "rquant-l2-replay-gate-v1"
    assert report["gate"]["status"] == "pass" and report["gate"]["scenario_count"] == 1
    scenario = report["scenarios"][0]
    assert scenario["name"] == "single-stall-40" and scenario["status"] == "pass"
    assert scenario["config"]["faults"]["stall_codes"] == ["300006"]
    assert scenario["metrics"]["batches"] == 120 + scenario["config"]["planned_push_frames"]
    assert any(expectation["name"] == "stalled_stock_push_frames_removed"
               and expectation["pass"] for expectation in scenario["expectations"])
    assert not any(expectation["name"] == "determinism_runs"
                   for expectation in scenario["expectations"])
    assert isinstance(scenario["metrics"]["consumption_latency_us"]["p99"], int)


def test_cli_rejects_an_invalid_push_seconds_value() -> None:
    with pytest.raises(SystemExit):
        replay_gate.main(["gate", "--pack", str(PACK), "--out", "unused.json",
                          "--push-seconds", "soon"])
    assert replay_gate._push_seconds("none") is None
    assert replay_gate._push_seconds("60") == 60.0


def test_fault_injection_mapping_covers_every_cli_choice() -> None:
    symbols = replay_gate.tier_symbols(40).symbols
    assert replay_gate.fault_injection("healthy", symbols) == FaultInjection()
    assert replay_gate.fault_injection("seed-24of40", symbols).withhold_seed_from_stock == 24
    assert replay_gate.fault_injection("single-silent", symbols).withhold_seed_codes == (
        "300006",)
    assert replay_gate.fault_injection("single-stall", symbols).stall_codes == ("300006",)
    with pytest.raises(replay_gate.GateError):
        replay_gate.fault_injection("meltdown", symbols)
