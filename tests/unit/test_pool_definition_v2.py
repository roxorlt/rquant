"""V2 pool definitions: PageControl authority and daily dependency execution."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from rquant import page_control
from rquant.llm.schemas import RuleCall
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SaveNlPreset,
    SaveUserPool,
    SaveUserPoolV2,
    parse_page_control_command,
)
from rquant.pipeline import _resolve_execution_order, run_daily_screen_stage
from rquant.presets import ScreenPreset, load_user_presets
from rquant.runtime_contracts import canonical_sha256
from rquant.screen.rules import not_st
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)


def _service(tmp_path: Path, *, now: datetime = NOW) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=lambda: now,
            lease_seconds=1,
        ),
    )


def _command(
    command_id: str,
    *,
    base_name: str = "breakout",
    expected_version: str | None = None,
    depends_on: str | None = None,
    delay_days: int = 0,
    display_name: str = "突破",
    rule_calls: tuple[RuleCall, ...] = (RuleCall(name="not_st", args={}),),
) -> SaveUserPoolV2:
    return SaveUserPoolV2(
        command_id=command_id,
        requested_at=NOW,
        base_name=base_name,
        display_name=display_name,
        description="日终筛选",
        rule_calls=rule_calls,
        include_columns=("CLOSE[0]",),
        depends_on=depends_on,
        delay_days=delay_days,
        expected_version=expected_version,
    )


def _pool_path(tmp_path: Path, base_name: str = "breakout") -> Path:
    return tmp_path / "data" / "user_presets" / f"{base_name}.json"


def test_v2_command_is_parsed_and_create_update_require_exact_file_version(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    create = _command("pool-create")
    parsed = parse_page_control_command(create.model_dump(mode="json"))
    assert isinstance(parsed, SaveUserPoolV2)

    first = service.submit(create)
    assert first.status is PageControlStatus.SUCCEEDED
    assert isinstance(first.result, dict)
    first_version = first.result["version"]
    first_bytes = _pool_path(tmp_path).read_bytes()
    assert first_version == canonical_sha256(json.loads(first_bytes))
    assert service.submit(create).result == first.result
    with pytest.raises(ValueError, match="different payload"):
        service.submit(create.model_copy(update={"display_name": "偷换内容"}))

    stale_create = service.submit(_command("pool-stale-create"))
    assert stale_create.status is PageControlStatus.FAILED
    assert "version" in (stale_create.error or "").lower()
    assert _pool_path(tmp_path).read_bytes() == first_bytes

    update = _command(
        "pool-update",
        expected_version=first_version,
        display_name="新突破",
        depends_on="n-shape-pool1",
        delay_days=2,
    )
    second = service.submit(update)
    assert second.status is PageControlStatus.SUCCEEDED
    assert isinstance(second.result, dict)
    assert second.result["version"] != first_version
    updated_bytes = _pool_path(tmp_path).read_bytes()
    updated = json.loads(updated_bytes)
    assert updated["display_name"] == "新突破"
    assert updated["depends_on"] == "n-shape-pool1"
    assert updated["delay_days"] == 2
    assert second.result["version"] == canonical_sha256(updated)

    stale_update = service.submit(
        _command("pool-stale-update", expected_version=first_version)
    )
    assert stale_update.status is PageControlStatus.FAILED
    assert _pool_path(tmp_path).read_bytes() == updated_bytes


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"depends_on": "missing", "delay_days": 1}, "parent"),
        ({"depends_on": "user/breakout", "delay_days": 1}, "self"),
        ({"depends_on": "n-shape-pool1", "delay_days": 0}, "delay"),
        ({"depends_on": None, "delay_days": 1}, "delay"),
        ({"depends_on": "n-shape-pool1", "delay_days": 253}, "delay"),
        ({"depends_on": "n-shape-pool1", "delay_days": -1}, "delay"),
        ({"rule_calls": (RuleCall(name="unknown_rule", args={}),)}, "rule"),
    ],
)
def test_v2_rejects_invalid_candidate_without_changing_existing_file(
    tmp_path: Path,
    changes: dict[str, object],
    reason: str,
) -> None:
    service = _service(tmp_path)
    first = service.submit(_command("pool-original"))
    assert first.status is PageControlStatus.SUCCEEDED
    assert isinstance(first.result, dict)
    old_bytes = _pool_path(tmp_path).read_bytes()
    candidate = _command("pool-invalid", expected_version=first.result["version"])
    rejected = service.submit(candidate.model_copy(update=changes))
    assert rejected.status is PageControlStatus.FAILED
    assert reason in (rejected.error or "").lower()
    assert _pool_path(tmp_path).read_bytes() == old_bytes


@pytest.mark.parametrize("delay_days", [1, 252])
def test_v2_accepts_exact_delay_boundary_of_one_to_252_trading_days(
    tmp_path: Path, delay_days: int
) -> None:
    result = _service(tmp_path).submit(
        _command(
            f"delay-{delay_days}",
            depends_on="n-shape-pool1",
            delay_days=delay_days,
        )
    )
    assert result.status is PageControlStatus.SUCCEEDED
    assert json.loads(_pool_path(tmp_path).read_text(encoding="utf-8"))["delay_days"] == delay_days


def test_queued_updates_check_version_at_consumer_execution(tmp_path: Path) -> None:
    service = _service(tmp_path)
    created = service.submit(_command("pool-initial"))
    assert created.status is PageControlStatus.SUCCEEDED
    assert isinstance(created.result, dict)
    first = _command("pool-queued-first", expected_version=created.result["version"])
    second = _command(
        "pool-queued-second",
        expected_version=created.result["version"],
        display_name="后到更新",
    )
    service.outbox.enqueue(first)
    service.outbox.enqueue(second)
    service.consumer.drain(limit=2)
    assert service.outbox.receipt(first.command_id).status is PageControlStatus.SUCCEEDED
    assert service.outbox.receipt(second.command_id).status is PageControlStatus.FAILED
    saved = json.loads(_pool_path(tmp_path).read_text(encoding="utf-8"))
    assert saved["command_id"] == first.command_id


@pytest.mark.parametrize("parent_name", ["b", "c"])
def test_v2_rejects_two_or_three_level_cycle_without_mutation(
    tmp_path: Path, parent_name: str
) -> None:
    service = _service(tmp_path)
    a = service.submit(_command("create-a", base_name="a"))
    assert a.status is PageControlStatus.SUCCEEDED
    assert isinstance(a.result, dict)
    assert service.submit(
        _command("create-b", base_name="b", depends_on="user/a", delay_days=1)
    ).status is PageControlStatus.SUCCEEDED
    if parent_name == "c":
        assert service.submit(
            _command("create-c", base_name="c", depends_on="user/b", delay_days=1)
        ).status is PageControlStatus.SUCCEEDED
    old_bytes = _pool_path(tmp_path, "a").read_bytes()
    result = service.submit(
        _command(
            "cycle-a",
            base_name="a",
            expected_version=a.result["version"],
            depends_on=f"user/{parent_name}",
            delay_days=1,
        )
    )
    assert result.status is PageControlStatus.FAILED
    assert "cycle" in (result.error or "").lower()
    assert _pool_path(tmp_path, "a").read_bytes() == old_bytes


def test_v2_recovers_after_atomic_write_without_second_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = page_control.PageControlConsumer._atomic_json
    writes = 0

    def crash_after_write(path: Path, payload: object, *, command_id: str) -> None:
        nonlocal writes
        writes += 1
        original(path, payload, command_id=command_id)
        if writes == 1:
            raise KeyboardInterrupt("after atomic replace")

    monkeypatch.setattr(
        page_control.PageControlConsumer, "_atomic_json", staticmethod(crash_after_write)
    )
    command = _command("recover-v2")
    with pytest.raises(KeyboardInterrupt):
        _service(tmp_path).submit(command)
    written = _pool_path(tmp_path).read_bytes()
    recovered = _service(tmp_path, now=NOW + timedelta(seconds=2)).submit(command)
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert isinstance(recovered.result, dict)
    assert recovered.result["version"] == canonical_sha256(json.loads(written))
    assert _pool_path(tmp_path).read_bytes() == written
    assert writes == 1


def test_v2_can_update_legacy_pool_and_legacy_commands_still_load(tmp_path: Path) -> None:
    service = _service(tmp_path)
    old = SaveUserPool(
        command_id="legacy-save",
        requested_at=NOW,
        base_name="breakout",
        rule_calls=(RuleCall(name="not_st", args={}),),
    )
    assert service.submit(old).status is PageControlStatus.SUCCEEDED
    old_definition = json.loads(_pool_path(tmp_path).read_text(encoding="utf-8"))
    version = canonical_sha256(old_definition)
    assert load_user_presets(_pool_path(tmp_path).parent)["user/breakout"].depends_on is None
    update = service.submit(_command("upgrade-legacy", expected_version=version))
    assert update.status is PageControlStatus.SUCCEEDED
    upgraded = load_user_presets(_pool_path(tmp_path).parent)["user/breakout"]
    assert upgraded.offset_days == 0
    assert upgraded.delay_days is None

    nl = SaveNlPreset(
        command_id="legacy-nl",
        requested_at=NOW,
        name="自然语言池",
        rule_calls=(RuleCall(name="not_st", args={}),),
    )
    assert service.submit(nl).status is PageControlStatus.SUCCEEDED
    assert "user/自然语言池" in load_user_presets(_pool_path(tmp_path).parent)


def test_legacy_save_cannot_erase_v2_dependency_and_exact_delay(tmp_path: Path) -> None:
    service = _service(tmp_path)
    created = service.submit(
        _command("v2-created", depends_on="n-shape-pool1", delay_days=2)
    )
    assert created.status is PageControlStatus.SUCCEEDED
    original = _pool_path(tmp_path).read_bytes()
    legacy = SaveUserPool(
        command_id="legacy-overwrite-v2",
        requested_at=NOW,
        base_name="breakout",
        rule_calls=(RuleCall(name="not_st", args={}),),
    )
    rejected = service.submit(legacy)
    assert rejected.status is PageControlStatus.FAILED
    assert _pool_path(tmp_path).read_bytes() == original


def test_v2_loader_keeps_valid_pools_when_unrelated_definition_is_invalid(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "user_presets"
    directory.mkdir()
    (directory / "valid.json").write_text(
        json.dumps(
            {
                "name": "valid",
                "display_name": "有效池",
                "description": "有效",
                "rules": [{"name": "not_st", "args": {}}],
                "depends_on": "n-shape-pool1",
                "delay_days": 2,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (directory / "bad.json").write_text(
        json.dumps(
            {
                "name": "bad",
                "rules": [{"name": "not_st", "args": {}}],
                "depends_on": "missing",
                "delay_days": 1,
            }
        ),
        encoding="utf-8",
    )
    result = load_user_presets(directory)
    assert set(result) == {"user/valid"}
    assert result["user/valid"].display_name == "有效池"
    assert result["user/valid"].depends_on == "n-shape-pool1"
    assert result["user/valid"].delay_days == 2


def test_v2_loader_skips_on_disk_dependency_cycle_but_keeps_independent_pool(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "user_presets"
    directory.mkdir()
    for name, parent in (("a", "user/b"), ("b", "user/a"), ("independent", None)):
        (directory / f"{name}.json").write_text(
            json.dumps(
                {
                    "name": name,
                    "rules": [{"name": "not_st", "args": {}}],
                    "depends_on": parent,
                    "delay_days": 0 if parent is None else 1,
                }
            ),
            encoding="utf-8",
        )
    assert set(load_user_presets(directory)) == {"user/independent"}


def test_execution_order_is_stable_for_three_levels_and_rejects_cycle() -> None:
    presets = {
        "c": ScreenPreset("c", "", [not_st()], depends_on="b", offset_days=1),
        "b": ScreenPreset("b", "", [not_st()], depends_on="a", offset_days=1),
        "a": ScreenPreset("a", "", [not_st()]),
    }
    assert _resolve_execution_order(presets) == ["a", "b", "c"]
    assert _resolve_execution_order(presets) == ["a", "b", "c"]
    presets["a"].depends_on = "c"
    with pytest.raises(ValueError, match="cycle"):
        _resolve_execution_order(presets)


def test_legacy_offset_window_keeps_union_of_previous_trading_days(tmp_path: Path) -> None:
    directory = tmp_path / "user_presets"
    directory.mkdir()
    (directory / "legacy.json").write_text(
        json.dumps(
            {
                "name": "legacy",
                "rules": [{"name": "not_st", "args": {}}],
                "depends_on": "n-shape-pool1",
                "offset_days": 2,
            }
        ),
        encoding="utf-8",
    )
    legacy = load_user_presets(directory)["user/legacy"]
    assert legacy.offset_days == 2
    assert legacy.delay_days is None
    with DuckDBStore(tmp_path / "legacy.duckdb") as store:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-07-31', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-08-03', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-08-04', 1,1,1,1,1,0,0,0,0)"
        )
        store.upsert_screen_result(
            pd.DataFrame(
                {
                    "trade_date": ["2026-07-31", "2026-08-03"],
                    "preset_name": ["n-shape-pool1"] * 2,
                    "ts_code": ["TWO", "ONE"],
                    "name": ["two", "one"],
                    "close": [1.0] * 2,
                    "pct_chg": [0.0] * 2,
                    "extra": [None] * 2,
                }
            )
        )
        observed: list[list[str] | None] = []

        def screen_stub(**kwargs: object) -> pd.DataFrame:
            observed.append(kwargs["ts_code_whitelist"])
            return pd.DataFrame(columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"])

        with patch("rquant.pipeline.screen", side_effect=screen_stub):
            result = run_daily_screen_stage(
                "2026-08-04",
                preset_names=["user/legacy"],
                store=store,
                preset_directory=directory,
            )
    assert result.preset_hits == {"user/legacy": 0}
    assert observed == [["ONE", "TWO"]]


def test_daily_screen_uses_exact_prior_trading_day_and_reloads_saved_definition(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    parent = service.submit(_command("save-parent", base_name="parent"))
    assert parent.status is PageControlStatus.SUCCEEDED
    child = service.submit(
        _command("save-child", base_name="child", depends_on="user/parent", delay_days=2)
    )
    assert child.status is PageControlStatus.SUCCEEDED
    data_dir = tmp_path / "data" / "user_presets"
    with DuckDBStore(tmp_path / "pipeline.duckdb") as store:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-07-30', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-07-31', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-08-03', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-08-04', 1,1,1,1,1,0,0,0,0)"
        )
        store.upsert_screen_result(
            pd.DataFrame(
                {
                    "trade_date": ["2026-07-30", "2026-07-31", "2026-08-03"],
                    "preset_name": ["user/parent"] * 3,
                    "ts_code": ["TOO_OLD", "TWO", "ONE"],
                    "name": ["old", "two", "one"],
                    "close": [1.0] * 3,
                    "pct_chg": [0.0] * 3,
                    "extra": [None] * 3,
                }
            )
        )
        observed: list[tuple[str, list[str] | None]] = []

        def screen_stub(**kwargs: object) -> pd.DataFrame:
            whitelist = kwargs["ts_code_whitelist"]
            observed.append((str(kwargs["trade_date"]), whitelist))
            return pd.DataFrame(columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"])

        with patch("rquant.pipeline.screen", side_effect=screen_stub):
            result = run_daily_screen_stage(
                "2026-08-04",
                preset_names=["user/child"],
                store=store,
                preset_directory=data_dir,
            )
    assert result.preset_hits == {"user/child": 0}
    assert observed == [("2026-08-04", ["TWO"])]


def test_saved_three_level_chain_runs_in_topological_order_with_each_prior_result(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    assert service.submit(_command("save-a", base_name="a")).status is PageControlStatus.SUCCEEDED
    assert service.submit(
        _command("save-b", base_name="b", depends_on="user/a", delay_days=2)
    ).status is PageControlStatus.SUCCEEDED
    assert service.submit(
        _command("save-c", base_name="c", depends_on="user/b", delay_days=1)
    ).status is PageControlStatus.SUCCEEDED
    with DuckDBStore(tmp_path / "chain.duckdb") as store:
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('X', '2026-07-31', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-08-03', 1,1,1,1,1,0,0,0,0),"
            "('X', '2026-08-04', 1,1,1,1,1,0,0,0,0)"
        )
        store.upsert_screen_result(
            pd.DataFrame(
                {
                    "trade_date": ["2026-07-31", "2026-08-03"],
                    "preset_name": ["user/a", "user/b"],
                    "ts_code": ["FROM_A", "FROM_B"],
                    "name": ["a", "b"],
                    "close": [1.0, 1.0],
                    "pct_chg": [0.0, 0.0],
                    "extra": [None, None],
                }
            )
        )
        observed: list[list[str] | None] = []

        def screen_stub(**kwargs: object) -> pd.DataFrame:
            observed.append(kwargs["ts_code_whitelist"])
            return pd.DataFrame(columns=["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"])

        with patch("rquant.pipeline.screen", side_effect=screen_stub):
            result = run_daily_screen_stage(
                "2026-08-04",
                preset_names=["user/c", "user/b", "user/a"],
                store=store,
                preset_directory=tmp_path / "data" / "user_presets",
            )
    assert result.preset_hits == {"user/a": 0, "user/b": 0, "user/c": 0}
    assert observed == [None, ["FROM_A"], ["FROM_B"]]
