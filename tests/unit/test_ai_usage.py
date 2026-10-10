"""The original PageControl SQLite owns durable shared calls and private content."""

from __future__ import annotations

import importlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from rquant.ai_assistance_contracts import AIMeasuredUsage, AIRequestBinding
from rquant.page_control import PageControlOutbox

NOW = datetime(2026, 10, 6, 15, 59, tzinfo=UTC)


def usage():
    return importlib.import_module("rquant.ai_usage")


def binding(**changes: object) -> AIRequestBinding:
    values = dict(owner_uid="alice", request_id=uuid4(), request_body_sha256="a" * 64,
                  purpose="screen", account_id="shared", model_id="model-a",
                  template_version="screen-v1", context_sha256="b" * 64,
                  reserved_at=NOW, budget_date=date(2026, 10, 6))
    values.update(changes)
    return AIRequestBinding(**values)


def journal(tmp_path: Path) -> PageControlOutbox:
    usage()
    return PageControlOutbox(tmp_path / "original.sqlite")


def dispatch(store: PageControlOutbox, value: AIRequestBinding):
    return store.ai_usage_dispatch(value.owner_uid, value.request_id,
                                   value.request_body_sha256, now=NOW)


def test_original_sqlite_owns_reservation_and_dispatch_before_the_call(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding()
    record = store.ai_usage_reserve(value, daily_limit=1)
    assert record.state == "reserved" and record.binding == value
    claimed = dispatch(store, value)
    assert claimed.claimed and claimed.token
    with sqlite3.connect(store.path) as connection:
        row = connection.execute("SELECT state, dispatch_token FROM ai_request WHERE request_id=?",
                                 (str(value.request_id),)).fetchone()
    assert row == ("dispatched", claimed.token)
    duplicate = dispatch(store, value)
    assert duplicate.claimed is False and duplicate.token is None


def test_last_call_is_reserved_once_across_distinct_connections_and_users(tmp_path: Path) -> None:
    store = journal(tmp_path)
    gate = Barrier(2)
    values = [binding(owner_uid="alice"), binding(owner_uid="bob", purpose="news_digest")]

    def reserve(value: AIRequestBinding) -> str:
        independent = PageControlOutbox(store.path)
        gate.wait(timeout=5)
        try:
            return independent.ai_usage_reserve(value, daily_limit=1).state
        except usage().AIBudgetExceeded:
            return "blocked"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(reserve, values))
    assert sorted(outcomes) == ["blocked", "reserved"]


def test_original_receipt_is_read_before_current_limit_and_cross_day(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding()
    original = store.ai_usage_reserve(value, daily_limit=1)
    same_request_later = value.model_copy(update={"reserved_at": NOW + timedelta(days=1),
                                                 "budget_date": date(2026, 10, 7)})
    assert store.ai_usage_reserve(same_request_later, daily_limit=0) == original
    assert original.binding.budget_date == date(2026, 10, 6)


@pytest.mark.parametrize("changes", [
    {"request_body_sha256": "c" * 64}, {"purpose": "pool_edit"},
    {"context_sha256": "c" * 64}, {"model_id": "model-b"},
    {"template_version": "screen-v2"}, {"account_id": "other"},
])
def test_same_uuid_cannot_change_any_original_content_binding(
    tmp_path: Path, changes: dict[str, object],
) -> None:
    store, value = journal(tmp_path), binding()
    store.ai_usage_reserve(value, daily_limit=1)
    with pytest.raises(usage().AIRequestConflict):
        store.ai_usage_reserve(value.model_copy(update=changes), daily_limit=10)


def test_other_user_cannot_read_original_request_or_result(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding()
    store.ai_usage_reserve(value, daily_limit=1)
    with pytest.raises(usage().AIRequestNotFound):
        store.ai_usage_lookup("bob", value.request_id, value.request_body_sha256)
    with pytest.raises(usage().AIRequestNotFound):
        store.ai_usage_reserve(value.model_copy(update={"owner_uid": "bob"}), daily_limit=10)


def test_restart_preserves_reserved_and_quarantines_dispatched_without_resend(tmp_path: Path) -> None:
    store = journal(tmp_path)
    waiting, sent = binding(), binding()
    store.ai_usage_reserve(waiting, daily_limit=2)
    store.ai_usage_reserve(sent, daily_limit=2)
    first = dispatch(store, sent)
    reopened = PageControlOutbox(store.path)
    assert reopened.ai_usage_recover_dispatches(now=NOW + timedelta(minutes=1)) == 1
    assert reopened.ai_usage_lookup("alice", sent.request_id, sent.request_body_sha256).state == "unknown"
    assert dispatch(reopened, sent).claimed is False
    assert dispatch(reopened, waiting).claimed is True
    assert first.token
    with pytest.raises(usage().AIBudgetExceeded):
        reopened.ai_usage_reserve(binding(), daily_limit=2)


def test_validation_failure_still_charges_actual_tokens_and_keeps_private_result_empty(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding(purpose="interpretation")
    store.ai_usage_reserve(value, daily_limit=1)
    claimed = dispatch(store, value)
    result = store.ai_usage_finish("alice", value.request_id, value.request_body_sha256,
                                  dispatch_token=claimed.token, now=NOW + timedelta(minutes=1),
                                  usage=AIMeasuredUsage(input_tokens=17, output_tokens=3),
                                  result=None, error_code="invalid_output")
    assert result.state == "completed" and result.result is None
    assert result.usage.total_tokens == 20 and result.error_code == "invalid_output"
    assert store.ai_usage_reserve(value, daily_limit=0) == result


def test_missing_usage_remains_unknown_in_monthly_statistics_and_uses_original_day(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding()
    store.ai_usage_reserve(value, daily_limit=1)
    claimed = dispatch(store, value)
    store.ai_usage_finish("alice", value.request_id, value.request_body_sha256,
                          dispatch_token=claimed.token, now=NOW + timedelta(days=1),
                          usage=AIMeasuredUsage(), result={"ready": True})
    summary = store.ai_usage_summary("alice", "shared", start_date=date(2026, 10, 1),
                                     end_date=date(2026, 10, 31))
    assert summary.calls == 1 and summary.unknown_usage_calls == 1
    assert summary.input_tokens is None and summary.output_tokens is None
    assert summary.days[0].day == date(2026, 10, 6)
    other = store.ai_usage_summary("bob", "shared", start_date=date(2026, 10, 1),
                                   end_date=date(2026, 10, 31))
    assert other.calls == 0 and other.days == ()


def test_only_proven_unsent_reservation_releases_a_slot(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding()
    store.ai_usage_reserve(value, daily_limit=1)
    released = store.ai_usage_release("alice", value.request_id, value.request_body_sha256,
                                     now=NOW, reason="not_dispatched")
    assert released.state == "not_dispatched"
    next_request = binding()
    store.ai_usage_reserve(next_request, daily_limit=1)
    dispatch(store, next_request)
    with pytest.raises(usage().AIRequestConflict):
        store.ai_usage_release("alice", next_request.request_id, next_request.request_body_sha256,
                               now=NOW, reason="not_dispatched")


def test_completed_content_is_immutable_and_requires_original_dispatch_token(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding()
    store.ai_usage_reserve(value, daily_limit=1)
    claimed = dispatch(store, value)
    arguments = dict(dispatch_token=claimed.token, now=NOW, usage=AIMeasuredUsage(input_tokens=4, output_tokens=2),
                     result={"answer": "original"})
    first = store.ai_usage_finish("alice", value.request_id, value.request_body_sha256, **arguments)
    assert store.ai_usage_finish("alice", value.request_id, value.request_body_sha256, **arguments) == first
    with pytest.raises(usage().AIRequestConflict):
        store.ai_usage_finish("alice", value.request_id, value.request_body_sha256,
                              **{**arguments, "result": {"answer": "changed"}})
    with pytest.raises(usage().AIRequestConflict):
        store.ai_usage_finish("alice", value.request_id, value.request_body_sha256,
                              **{**arguments, "dispatch_token": uuid4().hex})


def test_unknown_can_accept_only_its_actual_late_response(tmp_path: Path) -> None:
    store, value = journal(tmp_path), binding()
    store.ai_usage_reserve(value, daily_limit=1)
    claimed = dispatch(store, value)
    store.ai_usage_recover_dispatches(now=NOW)
    late = store.ai_usage_finish("alice", value.request_id, value.request_body_sha256,
                                dispatch_token=claimed.token, now=NOW,
                                usage=AIMeasuredUsage(input_tokens=1, output_tokens=1), result={"late": True})
    assert late.state == "completed" and dispatch(store, value).claimed is False


@pytest.mark.parametrize("limit", [True, -1, "1", 1.5, 2**63])
def test_daily_limit_is_a_strict_bounded_nonnegative_integer(tmp_path: Path, limit: object) -> None:
    store = journal(tmp_path)
    with pytest.raises((ValueError, ValidationError)):
        store.ai_usage_reserve(binding(), daily_limit=limit)


def test_repository_cannot_write_outside_original_owner_transaction(tmp_path: Path) -> None:
    store = journal(tmp_path)
    with sqlite3.connect(store.path) as connection:
        with pytest.raises(usage().AITransactionRequired):
            usage().AIUsageRepository(connection).reserve(binding(), daily_limit=1)


def test_original_transaction_cache_reuse_is_unsent_unmeasured_and_quota_free(tmp_path: Path) -> None:
    store = journal(tmp_path)
    value = binding(purpose="interpretation")
    with sqlite3.connect(store.path) as connection:
        repository = usage().AIUsageRepository(connection)
        with pytest.raises(usage().AITransactionRequired):
            repository.reuse_interpretation_cache(value, now=NOW)
        connection.execute("BEGIN IMMEDIATE")
        cached = repository.reuse_interpretation_cache(value, now=NOW)
        connection.commit()
    assert cached.binding == value and cached.state == "not_dispatched"
    assert cached.error_code == "cache_reused" and cached.result is None
    assert cached.dispatch_token is None and cached.dispatched_at is None
    assert cached.usage.input_tokens is None and cached.usage.output_tokens is None
    assert store.ai_usage_lookup("alice", value.request_id, value.request_body_sha256) == cached
    assert store.ai_usage_account_calls("shared", value.budget_date) == 0
    summary = store.ai_usage_summary("alice", "shared", start_date=value.budget_date, end_date=value.budget_date)
    assert summary.calls == 0 and summary.unknown_usage_calls == 0
    assert not dispatch(store, value).claimed
    with sqlite3.connect(store.path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        repository = usage().AIUsageRepository(connection)
        assert repository.reuse_interpretation_cache(value, now=NOW + timedelta(days=1)) == cached
        with pytest.raises(usage().AIRequestNotFound):
            repository.reuse_interpretation_cache(value.model_copy(update={"owner_uid": "bob"}), now=NOW)
        with pytest.raises(usage().AIRequestConflict):
            repository.reuse_interpretation_cache(value.model_copy(update={"context_sha256": "f" * 64}), now=NOW)


@pytest.mark.parametrize("state", ["reserved", "dispatched", "unknown", "completed"])
def test_cache_reuse_changes_only_proven_original_unsent_state(tmp_path: Path, state: str) -> None:
    store, value = journal(tmp_path), binding(purpose="interpretation")
    waiting = store.ai_usage_reserve(value, daily_limit=1)
    if state != "reserved":
        sent = dispatch(store, value)
        if state == "completed":
            store.ai_usage_finish("alice", value.request_id, value.request_body_sha256, dispatch_token=sent.token, now=NOW, usage=AIMeasuredUsage(input_tokens=2, output_tokens=3), result={"original": True})
        elif state == "unknown":
            store.ai_usage_unknown("alice", value.request_id, value.request_body_sha256, dispatch_token=sent.token, now=NOW)
    original = store.ai_usage_lookup("alice", value.request_id)
    with sqlite3.connect(store.path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        result = usage().AIUsageRepository(connection).reuse_interpretation_cache(value.model_copy(update={"reserved_at": NOW + timedelta(days=1), "budget_date": date(2026, 10, 7)}), now=NOW + timedelta(days=1))
        connection.commit()
    if state == "reserved":
        assert result.state == "not_dispatched" and result.binding == waiting.binding
        assert result.dispatch_token is None and result.error_code == "cache_reused"
        assert store.ai_usage_account_calls("shared", value.budget_date) == 0
    else:
        assert result == original
        assert store.ai_usage_account_calls("shared", value.budget_date) == 1
