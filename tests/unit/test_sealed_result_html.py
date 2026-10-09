"""Full original display rendering only; no service, math or artifact writer."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from html.parser import HTMLParser
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

if TYPE_CHECKING:
    from rquant.factor.display_artifact import FactorDisplayArtifactV1
    from rquant.factor.stream_job_artifact import FactorStreamDisplayArtifact
    from rquant.sealed_result_ownership import SealedArtifactFact, SealedOwnerBinding
    from rquant.strategy_template_run import StrategyTemplateResult

RENDERED_SAMPLES: dict[str, bytes] = {}


def core() -> ModuleType:
    try:
        return import_module("rquant.sealed_result_html")
    except ModuleNotFoundError:
        pytest.fail(
            "required readonly sealed HTML core is not implemented", pytrace=False
        )


def html_document(fragment: bytes) -> bytes:
    return (
        b'<!doctype html><html><head><meta charset="utf-8"><title>test</title></head><body>'
        + fragment
        + b"</body></html>"
    )


def factor_display(
    version: int = 1,
) -> FactorDisplayArtifactV1 | FactorStreamDisplayArtifact:
    from rquant.factor.display_artifact import FactorDisplayArtifactV1
    from rquant.factor.stream_job_artifact import FactorStreamDisplayArtifact
    from rquant.strict_json import canonical_json_bytes

    raw = (Path(__file__).parent / "fixtures" / "factor-display-v1.json").read_bytes()
    if version == 1:
        return FactorDisplayArtifactV1.model_validate_json(raw)
    # Synthetic v2 display from literal existing values; no projector or evaluator.
    data = json.loads(raw)
    data.update(
        schema_version=2,
        selection="all",
        pool_label="全市场（沪深非 ST）",
        snapshot_id="7" * 64,
    )
    data["portfolio_days"] = [
        dict(
            decision_date=d["decision_date"],
            groupings=[
                g | {"cumulative_status": "available" if g["groups"] else "gap"}
                for g in d["groupings"]
            ],
        )
        for d in data["portfolio_days"]
    ]
    for day in data["coverage_days"]:
        day["status"] = "complete"
    data.pop("content_sha256")
    data["content_sha256"] = hashlib.sha256(canonical_json_bytes(data)).hexdigest()
    return FactorStreamDisplayArtifact.model_validate_json(json.dumps(data))


def strategy_result() -> StrategyTemplateResult:
    from rquant.backtest.contracts import BacktestDayResult
    from rquant.paper_contracts import PaperAccountSnapshot
    from rquant.strategy_template_run import (
        StrategyTemplateResult,
        TemplateExitDecision,
    )

    account = PaperAccountSnapshot(
        account_id="alice-strategy",
        as_of_time=datetime(2026, 10, 6, 7, tzinfo=UTC),
        cash=Decimal("101005.00"),
        available_cash=Decimal("101005.00"),
        frozen_cash=Decimal("0"),
        realized_pnl=Decimal("1005.00"),
        unrealized_pnl=Decimal("0"),
        nav=Decimal("101005.00"),
    )
    day = BacktestDayResult(
        trade_date=date(2026, 10, 6),
        rebalanced=False,
        decisions=(),
        orders=(),
        skipped=(),
        fees=Decimal("0.00"),
        account=account,
        market_value=Decimal("0"),
        daily_return=Decimal("0.01005"),
        normalized_nav=Decimal("1.01005"),
    )
    return StrategyTemplateResult(
        owner_id="alice",
        strategy_id="sample-strategy",
        version=2,
        definition_fingerprint="a" * 64,
        definition_record_hash="b" * 64,
        input_hash="c" * 64,
        calendar_source_identity="d" * 64,
        cost_spec_id="e" * 64,
        status="complete",
        days=(day,),
        exit_decisions=(
            TemplateExitDecision(
                trade_date=day.trade_date,
                ts_code="000001.SZ",
                entry_signal_id="6" * 64,
                decision_id="7" * 64,
                reason="max_holding_days",
                decided_at=account.as_of_time,
            ),
        ),
    )


def ownership(
    domain: str, payload: object
) -> tuple[SealedOwnerBinding, SealedArtifactFact]:
    c = import_module("rquant.sealed_result_ownership")
    job = str(UUID(int=11))
    kind = (
        "submit_factor_run"
        if domain == "factor"
        else "run_strategy_template"
        if domain == "strategy"
        else "submit_portfolio_backtest"
    )
    artifact = c.SealedArtifactFact(
        domain=domain,
        job_id=job,
        spec_hash="b" * 64,
        manifest_hash="c" * 64,
        complete_result_hash="d" * 64,
        full_artifact_hash=payload.full_artifact_sha256
        if domain == "factor"
        else "e" * 64,
        result_payload_hash=payload.result_sha256
        if domain == "factor"
        else payload.content_hash
        if domain == "strategy"
        else "f" * 64,
        input_hash=payload.input_sha256
        if domain == "factor"
        else payload.input_hash
        if domain == "strategy"
        else "1" * 64,
        display_hash=payload.content_sha256 if domain == "factor" else None,
        html_sha256=hashlib.sha256(payload).hexdigest()
        if domain == "portfolio"
        else None,
        complete=True,
        private_owner="alice" if domain == "strategy" else None,
    )
    submission = c.OriginalSubmissionFact(
        domain=domain,
        command_id=str(UUID(int=10)),
        command_kind=kind,
        command_sha256="a" * 64,
        actor_id="alice",
        job_id=job,
        spec_hash=artifact.spec_hash,
        origin_verified=True,
    )
    effect = c.OriginalEffectFact(
        command_id=submission.command_id,
        command_kind=kind,
        command_sha256=submission.command_sha256,
        status="succeeded",
        submitted_job_id=job,
        submitted_spec_hash=artifact.spec_hash,
        worker_owner_id="worker",
    )
    sealed_job = c.SealedJobFact(
        domain=domain,
        job_id=job,
        spec_hash=artifact.spec_hash,
        status="succeeded",
        manifest_hash=artifact.manifest_hash,
        complete_result_hash=artifact.complete_result_hash,
    )
    return c.bind_sealed_owner(submission, effect, sealed_job, artifact), artifact


def forbidden_math(*args: object, **kwargs: object) -> object:
    pytest.fail("renderer attempted research/statistics recomputation")


def numeric_literals(value: object) -> list[str]:
    if isinstance(value, dict):
        return [s for item in value.values() for s in numeric_literals(item)]
    if isinstance(value, (tuple, list)):
        return [s for item in value for s in numeric_literals(item)]
    return [str(value)] if isinstance(value, float) else []


def test_html_has_fixed_original_budgets_and_no_external_resource_validation() -> None:
    c = core()
    assert c.MAX_HTML_BYTES == 4 * 1024 * 1024
    assert c.MAX_ZIP_BYTES == 32 * 1024 * 1024
    for body in (
        b"<script>alert(1)</script>",
        b'<img src="https://example.test/x">',
        b'<svg><image href="file:///secret"/></svg>',
        b'<style>@import "https://example.test";</style>',
        b'<p onclick="x()">x</p>',
    ):
        with pytest.raises(ValueError):
            c.validate_offline_html(html_document(body))


_ORIGINAL_PORTFOLIO_CSP_META = (
    b'<meta http-equiv="Content-Security-Policy" content="'
    b"default-src 'none'; style-src 'unsafe-inline'; script-src 'none'; "
    b"font-src 'none'; base-uri 'none'; form-action 'none'"
    b'">'
)


@pytest.mark.parametrize("domain,version", [("factor", 1), ("factor", 2), ("strategy", 1)])
def test_new_sealed_export_bytes_include_one_strict_head_csp(domain: str, version: int) -> None:
    c = core()
    payload = factor_display(version) if domain == "factor" else strategy_result()
    binding, artifact = ownership(domain, payload)
    renderer = c.render_factor_html if domain == "factor" else c.render_strategy_html
    body = renderer(payload, binding=binding, current_artifact=artifact, requester="alice")

    class Headers(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.in_head = False
            self.policies: list[str] = []

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag == "head":
                self.in_head = True
            fields = dict(attrs)
            if tag == "meta" and fields.get("http-equiv") == "Content-Security-Policy":
                assert self.in_head
                self.policies.append(fields.get("content") or "")

        def handle_endtag(self, tag: str) -> None:
            if tag == "head":
                self.in_head = False

    headers = Headers()
    headers.feed(body.decode("utf-8"))
    assert headers.policies == ["default-src 'none'; style-src 'unsafe-inline'; script-src 'none'; "
                                "font-src 'none'; base-uri 'none'; form-action 'none'"]
    c.validate_offline_html(body)
    RENDERED_SAMPLES[f"C15-F01-{domain}-v{version}.html"] = body


def original_portfolio_document(
    body: bytes, *, head: bytes = _ORIGINAL_PORTFOLIO_CSP_META
) -> bytes:
    return (
        b'<!doctype html><html><head><meta charset="utf-8">'
        + head
        + b"</head><body>"
        + body
        + b"</body></html>"
    )


def test_original_portfolio_static_markup_and_csp_remain_offline() -> None:
    c = core()
    body = original_portfolio_document(
        b"<dl><dt>cash</dt><dd>100.00</dd></dl>"
        b'<figure><figcaption>NAV <i class="swatch"></i></figcaption>'
        b'<svg><g data-series="strategy"><path d="M0 0 L1 1"/></g></svg>'
        b'</figure><table><tr><td data-label="NAV">1.125</td></tr></table>'
    )
    c.validate_offline_html(body)
    binding, artifact = ownership("portfolio", body)
    assert (
        c.reuse_portfolio_html(
            body,
            expected_sha256=hashlib.sha256(body).hexdigest(),
            binding=binding,
            current_artifact=artifact,
            requester="alice",
        )
        == body
    )


@pytest.mark.parametrize(
    "head",
    [
        b'<meta http-equiv="refresh" content="0;url=https://x">',
        b'<meta http-equiv="set-cookie" content="x=y">',
        b'<meta http-equiv="Content-Security-Policy">',
        _ORIGINAL_PORTFOLIO_CSP_META.replace(b"script-src 'none'", b"script-src 'unsafe-inline'"),
        _ORIGINAL_PORTFOLIO_CSP_META.replace(b"default-src 'none'", b"default-src https://x"),
        _ORIGINAL_PORTFOLIO_CSP_META.replace(b"font-src 'none'; ", b""),
        _ORIGINAL_PORTFOLIO_CSP_META.replace(b"<meta ", b'<meta name="viewport" '),
        _ORIGINAL_PORTFOLIO_CSP_META * 2,
    ],
)
def test_portfolio_csp_compatibility_does_not_allow_other_headers_or_policies(
    head: bytes,
) -> None:
    with pytest.raises(ValueError):
        core().validate_offline_html(original_portfolio_document(b"<p>x</p>", head=head))


@pytest.mark.parametrize(
    "body",
    [
        _ORIGINAL_PORTFOLIO_CSP_META,
        b'<p http-equiv="Content-Security-Policy">x</p>',
        b'<div data-series="strategy">x</div>',
        b'<i data-label="NAV">x</i>',
        b'<figure onclick="alert(1)"><figcaption>x</figcaption></figure>',
        b'<svg><g data-series="strategy" onload="alert(1)"></g></svg>',
        b'<i style="background:url(https://x)">x</i>',
        b'<table><tr><td data-label="NAV" style="background:url(file:///x)">x</td></tr></table>',
    ],
)
def test_portfolio_static_attributes_do_not_enable_active_content(body: bytes) -> None:
    with pytest.raises(ValueError):
        core().validate_offline_html(original_portfolio_document(body))


@pytest.mark.parametrize("version", [1, 2])
def test_factor_renders_original_values_only_in_both_sealed_display_contracts(
    version: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    c = core()
    display = factor_display(version)
    binding, artifact = ownership("factor", display)
    monkeypatch.setattr(
        import_module("rquant.factor.display_artifact"),
        "project_factor_display_artifact",
        forbidden_math,
    )
    monkeypatch.setattr(
        import_module("rquant.factor.result"),
        "assemble_factor_research_result",
        forbidden_math,
    )
    monkeypatch.setattr(
        import_module("rquant.factor.daily_stream"),
        "evaluate_factor_daily_stream",
        forbidden_math,
    )
    before = display.model_dump_json()
    body = c.render_factor_html(
        display,
        binding=binding,
        current_artifact=artifact,
        requester="alice",
        title="因子结果",
    )
    text = body.decode("utf-8")
    assert (
        "<svg" in text
        and "<table" in text
        and "IC" in text
        and "分组" in text
        and "覆盖" in text
    )
    assert "累计 IC 是有效日 IC 的和" in text
    assert all(
        value in text for value in numeric_literals(display.model_dump(mode="json"))
    )
    assert display.model_dump_json() == before
    c.validate_offline_html(body)
    RENDERED_SAMPLES[f"factor-v{version}.html"] = body


def test_strategy_preserves_original_money_nav_return_exit_and_missing_values() -> None:
    c = core()
    result = strategy_result()
    binding, artifact = ownership("strategy", result)
    before = result.model_dump_json()
    body = c.render_strategy_html(
        result, binding=binding, current_artifact=artifact, requester="alice"
    )
    text = body.decode()
    assert (
        "<svg" in text
        and "101005.00" in text
        and "0.01005" in text
        and "1.01005" in text
    )
    assert "退出" in text and "7" * 64 in text and "—" in text
    assert result.model_dump_json() == before
    c.validate_offline_html(body)
    RENDERED_SAMPLES["strategy-v1.html"] = body


def test_titles_are_escaped_and_unknown_or_current_cross_owner_denies() -> None:
    c = core()
    display = factor_display()
    binding, artifact = ownership("factor", display)
    body = c.render_factor_html(
        display,
        binding=binding,
        current_artifact=artifact,
        requester="alice",
        title='<img src=x onerror=alert(1)>&"',
    )
    assert (
        b"<img" not in body
        and b"&lt;img" in body
        and b"&amp;" in body
        and b"&quot;" in body
    )
    for owner in ("bob", "admin"):
        with pytest.raises(PermissionError):
            c.render_factor_html(
                display, binding=binding, current_artifact=artifact, requester=owner
            )
    with pytest.raises(PermissionError):
        c.render_factor_html(
            display, binding=None, current_artifact=artifact, requester="alice"
        )


@pytest.mark.parametrize(
    "field", ["content_sha256", "full_artifact_sha256", "result_sha256", "input_sha256"]
)
def test_factor_payload_or_full_bindings_cannot_be_substituted(field: str) -> None:
    c = core()
    display = factor_display()
    binding, artifact = ownership("factor", display)
    with pytest.raises((ValueError, PermissionError)):
        c.render_factor_html(
            display.model_copy(update={field: "9" * 64}),
            binding=binding,
            current_artifact=artifact,
            requester="alice",
        )


def test_partial_strategy_wrong_input_or_fake_owner_rejects() -> None:
    c = core()
    result = strategy_result()
    binding, artifact = ownership("strategy", result)
    for fields in (
        {"status": "incomplete"},
        {"owner_id": "bob"},
        {"input_hash": "9" * 64},
        {"content_hash": "9" * 64},
    ):
        with pytest.raises((ValueError, PermissionError)):
            c.render_strategy_html(
                result.model_copy(update=fields),
                binding=binding,
                current_artifact=artifact,
                requester="alice",
            )
    with pytest.raises(ValueError):
        c.render_factor_html(
            {"content_sha256": "9" * 64},
            binding=binding,
            current_artifact=artifact,
            requester="alice",
        )


def test_portfolio_reuses_identical_existing_html_only_after_digest_owner_and_offline_checks() -> (
    None
):
    c = core()
    body = '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>组合</title></head><body><table><tr><td>1.125</td></tr></table></body></html>'.encode()
    binding, artifact = ownership("portfolio", body)
    expected = hashlib.sha256(body).hexdigest()
    assert (
        c.reuse_portfolio_html(
            body,
            expected_sha256=expected,
            binding=binding,
            current_artifact=artifact,
            requester="alice",
        )
        == body
    )
    RENDERED_SAMPLES["portfolio-reused.html"] = body
    with pytest.raises(ValueError):
        c.reuse_portfolio_html(
            body,
            expected_sha256="9" * 64,
            binding=binding,
            current_artifact=artifact,
            requester="alice",
        )
    with pytest.raises(PermissionError):
        c.reuse_portfolio_html(
            body,
            expected_sha256=expected,
            binding=binding,
            current_artifact=artifact,
            requester="admin",
        )
    substituted = body.replace(b"1.125", b"9.999")
    with pytest.raises((ValueError, PermissionError)):
        c.reuse_portfolio_html(
            substituted,
            expected_sha256=hashlib.sha256(substituted).hexdigest(),
            binding=binding,
            current_artifact=artifact,
            requester="alice",
        )
    malicious = b'<html><body><iframe src="file:///x"></iframe></body></html>'
    with pytest.raises(ValueError):
        c.reuse_portfolio_html(
            malicious,
            expected_sha256=hashlib.sha256(malicious).hexdigest(),
            binding=binding,
            current_artifact=artifact,
            requester="alice",
        )


def test_overflow_rejects_whole_output_and_retains_original_capacity_intersections() -> (
    None
):
    c = core()
    display = factor_display()
    binding, artifact = ownership("factor", display)
    with pytest.raises(ValueError):
        c.render_factor_html(
            display,
            binding=binding,
            current_artifact=artifact,
            requester="alice",
            max_bytes=256,
        )
    with pytest.raises(ValueError):
        c.render_factor_html(
            display,
            binding=binding,
            current_artifact=artifact,
            requester="alice",
            max_bytes=c.MAX_HTML_BYTES + 1,
        )
    with pytest.raises(ValueError):
        c.render_factor_html(
            display,
            binding=binding,
            current_artifact=artifact,
            requester="alice",
            title="x" * (64 * 1024 + 1),
        )
    for change in (
        {"html_bytes": c.MAX_HTML_BYTES + 1},
        {"zip_bytes": c.MAX_ZIP_BYTES + 1},
        {"cell_bytes": 64 * 1024 + 1},
        {"owner_bytes": 7 * 1024 * 1024 + 1},
    ):
        with pytest.raises(ValueError):
            c.require_export_capacity(**change)
    c.require_export_capacity(
        html_bytes=c.MAX_HTML_BYTES,
        zip_bytes=c.MAX_ZIP_BYTES,
        cell_bytes=64 * 1024,
        owner_bytes=7 * 1024 * 1024,
    )


@pytest.mark.parametrize(
    "body",
    [
        b'<meta http-equiv="refresh" content="0;url=https://x">',
        b"<style>p { background: u\\72l(https://x) }</style>",
        b"<svg><foreignObject>x</foreignObject></svg>",
        b'<a href="javascript:alert(1)">x</a>',
        b"<style>@font-face { src:url(file:///x) }</style>",
        b'<object data="data:text/html,x"></object>',
        b'<base href="https://x">',
        b'<svg><rect fill="u&#114;l(https://x)"/></svg>',
        b'<svg><path stroke="url(file:///x)"/></svg>',
        b'<meta charset="utf-7">',
        b'<?xml-stylesheet href="https://x"?>',
    ],
)
def test_encoded_css_active_html_and_external_resources_deny(body: bytes) -> None:
    c = core()
    with pytest.raises(ValueError):
        c.validate_offline_html(html_document(body))
