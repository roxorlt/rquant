from __future__ import annotations

import importlib
import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from fastapi.testclient import TestClient as RawClient

from rquant.lab_jobs import (
    CommandAvailability,
    LabJobCommandContext,
    LabJobPage,
    LabJobProgress,
    LabJobSummary,
)
from rquant.paper_contracts import PaperOrderStatus, PaperRejectReason, PaperSide
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.web.models.backtests import PortfolioSourceOption
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import (
    ResearcherTestClient,
    create_private_test_app,
    with_test_proxy_identity,
)
from tests.unit.test_backtest_platform import config, sealed_portfolio

NOW = datetime(2026, 10, 5, tzinfo=UTC)
BASE = "/api/v1/backtests/portfolio"
WRITE = {"X-Rquant-Csrf": "1", "Origin": "http://testserver"}


def web_module() -> ModuleType:
    name = "rquant.web.portfolio_backtest_service"
    assert importlib.util.find_spec(name), "portfolio verified Web application service is missing"
    return importlib.import_module(name)


@pytest.fixture
def portfolio_web(tmp_path: Path):
    ledger, artifacts, bundle, authority = sealed_portfolio(tmp_path)
    job = authority.job
    availability = CommandAvailability(pause=False, resume=False, cancel=False, retry=False)
    progress = LabJobProgress(
        total_shards=1,
        terminal_shards=1,
        succeeded_shards=1,
        failed_shards=0,
        cancelled_shards=0,
        fraction=1,
    )
    ledger.get_job = lambda selected: job if selected == job.job_id else None
    ledger.get_command_context = lambda selected: (
        LabJobCommandContext(job=job, availability=availability) if selected == job.job_id else None
    )
    ledger.list_jobs = lambda **kwargs: LabJobPage(
        items=(
            LabJobSummary(
                job_id=job.job_id,
                strategy_name="portfolio_backtest",
                spec_hash=job.spec_hash,
                job_type=job.job_type,
                resource_class=job.resource_class,
                status=job.status,
                control_intent=job.control_intent,
                result_state=job.result_state,
                version=job.version,
                deadline=job.deadline,
                created_at=job.created_at,
                updated_at=job.updated_at,
                progress=progress,
                command_availability=availability,
            ),
        ),
        total_count=1,
        has_more=False,
        next_cursor=None,
    )
    source = PortfolioSourceOption(
        key=bundle.frozen.config.source_key,
        version=1,
        label="每日候选",
        start_date=bundle.frozen.config.start_date,
        end_date=bundle.frozen.config.end_date,
        updated_at=NOW,
        ranking_available=False,
        industry_available=False,
        opening_verified=True,
    )
    service = web_module().PortfolioWebService(
        reader=ledger,
        results=PortfolioResultReader(reader=ledger, artifact_root=artifacts.root),
        sources=(source,),
        default_config=bundle.frozen.config,
        preparation_available=True,
    )
    submitted = []

    def transport(payload):
        submitted.append(payload)
        command_id = payload["command_id"]
        actor = payload["actor_id"]
        expected_job = uuid5(NAMESPACE_URL, f"rquant.portfolio-job:{actor}:{command_id}")
        interaction = f"web.portfolio:{actor}:{command_id}"
        return {
            "command_id": command_id,
            "status": "succeeded",
            "enqueued_at": NOW,
            "completed_at": NOW,
            "result": {
                "result": "submitted",
                "request_id": str(
                    uuid5(NAMESPACE_URL, f"rquant.lab-job-center.interaction:{interaction}")
                ),
                "command_type": "submit",
                "job_id": str(expected_job),
                "expected_version": None,
                "spool": {
                    "path": "/private/writer/spool",
                    "state": "pending",
                    "device": 1,
                    "inode": 2,
                    "content_hash": "a" * 64,
                },
            },
        }

    settings = with_test_proxy_identity(WebSettings(serving_root=tmp_path / "no-serving"))
    settings = WebSettings.model_validate(
        settings.model_dump() | {"lab_control_users": ("researcher", "admin")}
    )
    app = create_private_test_app(
        settings,
        portfolio_backtests=service,
        lab_control_command_transport=transport,
        clock=lambda: NOW,
        background=False,
    )
    try:
        with ResearcherTestClient(app) as client:
            yield SimpleNamespace(
                client=client,
                app=app,
                job=job,
                bundle=bundle,
                result_hash=authority.evidence.complete_result_hash,
                submitted=submitted,
            )
    finally:
        artifacts.close()


def test_pb01_web_admission_uses_trusted_identity_csrf_and_original_receipt(portfolio_web) -> None:
    fixture = portfolio_web
    body = {
        "command_id": str(uuid4()),
        "requested_at": NOW.isoformat(),
        "config": config().model_dump(mode="json"),
    }
    assert fixture.client.post(BASE + "/runs", json=body).status_code == 403
    with RawClient(fixture.app) as anonymous:
        assert (
            anonymous.post(
                BASE + "/runs", json=body, headers=WRITE | {"x-rquant-user": "admin"}
            ).status_code
            == 401
        )
    assert (
        fixture.client.post(
            BASE + "/runs", json=body | {"actor_id": "admin"}, headers=WRITE
        ).status_code
        == 422
    )
    response = fixture.client.post(BASE + "/runs", json=body, headers=WRITE)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "submitted"
    assert fixture.submitted[0]["actor_id"] == "researcher"
    assert fixture.submitted[0]["kind"] == "submit_portfolio_backtest"
    assert "path" not in response.text and "/private/" not in response.text
    assert response.json()["job_id"] == str(
        uuid5(NAMESPACE_URL, f"rquant.portfolio-job:researcher:{body['command_id']}")
    )


def test_pb09_web_reads_one_sealed_generation_all_views_and_exact_html(portfolio_web) -> None:
    fixture = portfolio_web
    listing = fixture.client.get(BASE + "/runs")
    assert listing.status_code == 200, listing.text
    assert listing.json()["data"]["jobs"][0]["status"] == "completed"
    route = BASE + "/runs/" + str(fixture.job.job_id)
    summary = fixture.client.get(route)
    assert summary.status_code == 200, summary.text
    assert (
        summary.json()["data"]["performance"]["summary"]["total_return"]
        == fixture.bundle.performance.summary.total_return
    )
    assert summary.json()["serving"]["generation_id"] == fixture.result_hash
    params = {"result_hash": fixture.result_hash}
    nav = fixture.client.get(route + "/nav", params=params)
    assert nav.status_code == 200, nav.text
    assert len(nav.json()["data"]["rows"]) == 2
    for view in ("trades", "holdings", "daily", "monthly", "log"):
        response = fixture.client.get(route + "/rows", params=params | {"view": view, "limit": 1})
        assert response.status_code == 200, (view, response.text)
        assert len(response.json()["data"][view]) == 1
        assert len(response.content) <= 16 * 1024 * 1024
    page = fixture.client.get(
        route + "/rows", params=params | {"view": "daily", "offset": 1, "limit": 1}
    )
    assert page.json()["data"]["daily"][0]["trade_date"] == "2026-08-11"
    assert page.json()["data"]["next_offset"] is None
    assert (
        fixture.client.get(
            route + "/rows", params=params | {"view": "daily", "limit": 51}
        ).status_code
        == 422
    )
    assert fixture.client.get(route + "/nav", params={"result_hash": "0" * 64}).status_code == 409
    html = fixture.client.get(route + "/report.html", params=params)
    assert html.status_code == 200, html.text
    assert html.content == fixture.bundle.html.encode()
    assert "attachment" in html.headers["content-disposition"]
    assert "default-src 'none'" in html.headers["content-security-policy"]


def test_pb_read_01_original_filled_orders_read_as_filled_trades_and_log(portfolio_web) -> None:
    fixture = portfolio_web
    orders = tuple(order for day in fixture.bundle.result.days for order in day.orders)
    assert len(orders) == 3
    assert all(order.receipt.order.status is PaperOrderStatus.FILLED for order in orders)
    assert {order.intent.side for order in orders} == {PaperSide.BUY, PaperSide.SELL}
    route = BASE + "/runs/" + str(fixture.job.job_id) + "/rows"
    params = {"result_hash": fixture.result_hash}
    trades = fixture.client.get(route, params=params | {"view": "trades"})
    assert trades.status_code == 200, trades.text
    actual = trades.json()["data"]["trades"]
    assert len(actual) == len(orders)
    assert [row["side"] for row in actual] == [order.intent.side.value for order in orders]
    assert [row["status"] for row in actual] == ["已成交"] * len(orders)
    log = fixture.client.get(route, params=params | {"view": "log"})
    assert log.status_code == 200, log.text
    assert [row["message"] for row in log.json()["data"]["log"]] == ["订单已成交。"] * len(orders)


@pytest.mark.parametrize(
    ("status", "message"),
    (
        (PaperOrderStatus.PENDING, "订单待处理。"),
        (PaperOrderStatus.ACCEPTED, "订单已接收。"),
        (PaperOrderStatus.PARTIALLY_FILLED, "订单部分成交。"),
        (PaperOrderStatus.FILLED, "订单已成交。"),
        (PaperOrderStatus.REJECTED, "条件不满足，本次未成交。"),
        (PaperOrderStatus.CANCELLED, "订单已取消。"),
        (PaperOrderStatus.EXPIRED, "订单已过期。"),
    ),
)
def test_pb_read_01_log_matches_actual_paper_order_status(
    status: PaperOrderStatus, message: str
) -> None:
    payload = json.dumps(
        {"trade_date": "2026-08-10", "ts_code": "600001.SH", "status": status.value, "reason": None}
    )
    row = web_module().PortfolioWebService._log(payload)
    assert row.message == message
    assert row.level == "normal"


@pytest.mark.parametrize(
    ("reason", "message"),
    (
        (PaperRejectReason.T_PLUS_ONE, "当日买入尚不可卖出。"),
        (PaperRejectReason.SUSPENDED, "股票停牌，未成交。"),
        (PaperRejectReason.LIMIT_LOCKED, "涨跌停限制，未成交。"),
        (PaperRejectReason.INSUFFICIENT_CASH, "资金不足，未成交。"),
        (PaperRejectReason.INSUFFICIENT_POSITION, "可卖数量不足，未成交。"),
        (PaperRejectReason.INVALID_LOT, "数量不符合整手要求，未成交。"),
        (PaperRejectReason.EXPIRED, "订单已过期，未成交。"),
        (PaperRejectReason.RISK_REJECTED, "风险限制生效，未成交。"),
    ),
)
def test_pb_read_01_log_matches_actual_paper_rejection_reason(
    reason: PaperRejectReason, message: str
) -> None:
    payload = json.dumps(
        {
            "trade_date": "2026-08-10",
            "ts_code": "600001.SH",
            "status": PaperOrderStatus.REJECTED.value,
            "reason": reason.value,
        }
    )
    row = web_module().PortfolioWebService._log(payload)
    assert row.message == message
    assert row.level == "note"


@pytest.mark.parametrize("status", ("filled", "rejected", "FUTURE_STATE", ""))
@pytest.mark.parametrize("reason", (None, "missing_held_close"))
def test_pb_read_01_unknown_log_status_cannot_be_a_known_event(
    status: str, reason: str | None
) -> None:
    payload = json.dumps(
        {"trade_date": "2026-08-10", "ts_code": "600001.SH", "status": status, "reason": reason}
    )
    with pytest.raises(ValueError, match="status"):
        web_module().PortfolioWebService._log(payload)


@pytest.mark.parametrize(
    ("status", "reason", "message", "level"),
    (
        ("skipped", "below_lot", "目标不足一手，未提交订单。", "note"),
        ("incomplete", "missing_held_close", "持仓缺少收盘价，本次结果不完整。", "error"),
        ("risk", "drawdown_released", "回撤已恢复，解除仓位限制。", "note"),
    ),
)
def test_pb_read_01_original_business_log_states_remain_available(
    status: str, reason: str, message: str, level: str
) -> None:
    payload = json.dumps(
        {"trade_date": "2026-08-10", "ts_code": None, "status": status, "reason": reason}
    )
    row = web_module().PortfolioWebService._log(payload)
    assert (row.message, row.level) == (message, level)


def test_pb06_web_uninstalled_source_is_explicit_unavailable(tmp_path: Path) -> None:
    with ResearcherTestClient(
        create_private_test_app(
            WebSettings(serving_root=tmp_path / "empty"), clock=lambda: NOW, background=False
        )
    ) as client:
        response = client.get(BASE + "/capabilities")
        assert response.status_code == 200, response.text
        assert response.json()["data"]["can_run"] is False
        assert response.json()["data"]["message"]
        assert response.json()["serving"]["state"] == "unavailable"


def test_pb07_complete_http_envelope_budget_includes_metadata() -> None:
    from rquant.web.envelope import Envelope, ServingMeta, ServingState

    value = Envelope(
        data={"text": "x" * (16 * 1024 * 1024)},
        serving=ServingMeta(
            generation_id="a" * 64,
            built_at=NOW,
            age_seconds=0,
            state=ServingState.READY,
            message=None,
            detail="",
        ),
    )
    with pytest.raises(Exception, match="范围|budget"):
        web_module().bounded_portfolio_response(value)


def test_pb03_editable_v3_cost_schema_round_trips_exact_domain_bytes() -> None:
    import rquant.web.models.backtests as contracts

    assert hasattr(contracts, "PortfolioEditableConfig"), (
        "typed editable v3 portfolio config is missing"
    )
    original = config()
    editable = contracts.PortfolioEditableConfig.from_domain(original)
    assert editable.model_dump_json() == original.model_dump_json()
    assert editable.to_domain() == original
    assert editable.to_domain().config_hash == original.config_hash
    schema = contracts.PortfolioEditableConfig.model_json_schema(mode="serialization")
    cost = schema["$defs"]["PortfolioCostConfig"]
    assert cost["type"] == "object"
    assert "minimum_commission" not in cost["properties"]
    assert "commission_rules" in cost["required"]
    request = contracts.PortfolioCreateRequest.model_validate_json(
        __import__("json").dumps(
            {
                "command_id": str(uuid4()),
                "requested_at": NOW.isoformat(),
                "config": editable.model_dump(mode="json"),
            }
        )
    )
    assert request.config.to_domain() == original
