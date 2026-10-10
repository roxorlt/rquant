"""Offline HTML report behavior from hand-checkable account and index facts."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from html.parser import HTMLParser
from pathlib import Path

import pytest

from rquant.backtest import BacktestDayResult, BacktestResult, run_portfolio_backtest
from rquant.backtest.benchmark import BenchmarkDay, BenchmarkSeries, _source_identity
from rquant.backtest.report import render_backtest_html
from rquant.paper_contracts import PaperAccountSnapshot, PaperHolding
from tests.unit.test_portfolio_backtest import _request

_FIRST = date(2026, 8, 10)
_SECOND = date(2026, 8, 11)
_BASELINE = date(2026, 8, 7)
_CALENDAR_ID = "f" * 64
_HOSTILE_CODE = '<img src=x onerror="alert(1)">'


class _Tags(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str | None]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, dict(attrs)))


def _result(*, hostile_holding: bool = False) -> BacktestResult:
    days = []
    for trade_date, nav, previous, normalized in (
        (_FIRST, Decimal("1050"), Decimal("1000"), Decimal("1.05")),
        (_SECOND, Decimal("1029"), Decimal("1050"), Decimal("1.029")),
    ):
        holdings = (
            (
                PaperHolding(
                    code=_HOSTILE_CODE,
                    quantity=100,
                    available_quantity=100,
                    frozen_quantity=0,
                    average_cost=Decimal("1"),
                    market_price=Decimal("1"),
                ),
            )
            if hostile_holding
            else ()
        )
        market_value = Decimal("100") if hostile_holding else Decimal("0")
        cash = nav - market_value
        account = PaperAccountSnapshot(
            account_id="offline-test",
            as_of_time=datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=UTC),
            cash=cash,
            available_cash=cash,
            frozen_cash=Decimal("0"),
            holdings=holdings,
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            nav=nav,
        )
        days.append(
            BacktestDayResult(
                trade_date=trade_date,
                rebalanced=False,
                decisions=(),
                orders=(),
                skipped=(),
                fees=Decimal("0"),
                account=account,
                market_value=market_value,
                daily_return=nav / previous - 1,
                normalized_nav=normalized,
            )
        )
    return BacktestResult(
        request_id="a" * 64,
        producer_commit="b" * 40,
        input_generation_id="c" * 64,
        calendar_source_identity=_CALENDAR_ID,
        cost_spec_id="d" * 64,
        status="complete",
        days=tuple(days),
    )


def _benchmark(result: BacktestResult, *, code: str = "000300.SH") -> BenchmarkSeries:
    closes = ((_BASELINE, 100.0), (_FIRST, 110.0), (_SECOND, 99.0))
    return BenchmarkSeries(
        source_identity=_source_identity(code, _CALENDAR_ID, closes),
        ts_code=code,
        calendar_source_identity=_CALENDAR_ID,
        backtest_content_hash=result.content_hash,
        baseline_trade_date=_BASELINE,
        baseline_close=100.0,
        days=(
            BenchmarkDay(
                trade_date=_FIRST,
                close=110.0,
                daily_return=110.0 / 100.0 - 1,
                normalized_nav=110.0 / 100.0,
            ),
            BenchmarkDay(
                trade_date=_SECOND,
                close=99.0,
                daily_return=99.0 / 110.0 - 1,
                normalized_nav=99.0 / 100.0,
            ),
        ),
    )


def test_hand_calculated_ledger_and_benchmark_render_as_offline_report() -> None:
    result = _result()
    report = render_backtest_html(result, _benchmark(result))
    html = report.html_bytes.decode("utf-8")

    assert html.startswith("<!doctype html>")
    assert report.sha256 == hashlib.sha256(report.html_bytes).hexdigest()
    assert report == render_backtest_html(result, _benchmark(result))
    assert "2026年8月10日" in html
    assert "2026年8月11日" in html
    assert "1.0500" in html and "1.0290" in html
    assert "+5.00%" in html and "−2.00%" in html
    assert "+2.90%" in html and "−1.00%" in html
    assert "+3.94%" in html  # (1.029 / 0.99) - 1
    assert "1,029.00 元" in html
    assert "起点 1.0500 · 终点 1.0290" in html
    assert "起点 0.00% · 终点 −2.00%" in html
    assert "retrospective_daily_bar" in html
    assert "不代表真实预挂单或保证成交" in html
    assert "基准尚未提供" not in html
    assert html.count("<svg") == 2
    assert "<script" not in html and "http://" not in html and "https://" not in html


def test_source_wording_stays_reader_facing_and_technical_provenance_is_folded() -> None:
    result = _result()
    html = render_backtest_html(result, _benchmark(result)).html_bytes.decode("utf-8")
    body_before_details = html.split('<details class="provenance">', maxsplit=1)[0]

    assert "事后指数收盘价" in body_before_details
    assert "retrospective_daily_bar" not in body_before_details
    assert "index_daily_bar" not in body_before_details
    assert "retrospective_daily_bar" in html.split('<details class="provenance">', maxsplit=1)[1]
    assert "静态图 · 无脚本" not in body_before_details
    assert "Portfolio research" not in body_before_details
    assert "rQuant / 组合研究" in body_before_details


def test_390px_chart_has_readable_equivalent_date_and_value_ticks() -> None:
    result = _result()
    html = render_backtest_html(result, _benchmark(result)).html_bytes.decode("utf-8")
    mobile_css = html.split("@media(max-width:640px)", maxsplit=1)[1].split(
        "@media print", maxsplit=1
    )[0]

    # At 390px: 18px page gutters + 8px card padding and 1px borders per side.
    chart_width = 390 - 2 * (18 + 8 + 1)
    assert 11 * chart_width / 760 < 5  # Existing SVG axis text is too small.
    assert 12 * len("2026年8月11日") < (chart_width - 8) / 2
    assert ".chart-scale{display:grid" in mobile_css
    assert (
        "font-size:12px"
        in mobile_css.split(".chart-scale{", maxsplit=1)[1].split("}", maxsplit=1)[0]
    )
    scales = re.findall(r'<div class="chart-scale"[^>]*>(.*?)</div>', html, flags=re.S)
    assert len(scales) == 2
    assert "上限 1.1099" in scales[0] and "下限 0.9801" in scales[0]
    assert all("2026年8月10日" in scale and "2026年8月11日" in scale for scale in scales)


def test_missing_benchmark_is_explicit_and_optional_metrics_are_not_zero_filled() -> None:
    html = render_backtest_html(_result()).html_bytes.decode("utf-8")
    assert "基准尚未提供" in html
    assert "超额收益" not in html
    assert "无成交记录" in html
    assert "期末空仓" in html
    assert "—" in html  # Undefined risk ratios stay missing.


def test_dynamic_holding_and_benchmark_text_are_escaped_and_no_active_resources_exist() -> None:
    result = _result(hostile_holding=True)
    report = render_backtest_html(result, _benchmark(result, code=_HOSTILE_CODE))
    html = report.html_bytes.decode("utf-8")
    tags = _Tags()
    tags.feed(html)

    assert "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;" in html
    assert _HOSTILE_CODE not in html
    assert all(
        tag not in {"script", "img", "link", "iframe", "object", "form", "use"}
        for tag, _ in tags.tags
    )
    assert all(not key.startswith("on") for _, attrs in tags.tags for key in attrs)
    assert all("src" not in attrs and "href" not in attrs for _, attrs in tags.tags)


def test_incomplete_or_inconsistent_account_series_is_rejected() -> None:
    raw = _result().model_dump(mode="python", exclude={"content_hash"})
    raw["status"] = "incomplete"
    raw["days"][-1].update(
        account=None,
        market_value=None,
        daily_return=None,
        normalized_nav=None,
        incomplete_reason="missing_held_close",
    )
    with pytest.raises(ValueError, match="complete"):
        render_backtest_html(BacktestResult.model_validate(raw))

    raw = _result().model_dump(mode="python", exclude={"content_hash"})
    raw["days"][-1]["daily_return"] = Decimal("0")
    with pytest.raises(ValueError, match="daily return"):
        render_backtest_html(BacktestResult.model_validate(raw))


def test_mismatched_benchmark_binding_is_rejected() -> None:
    result = _result()
    benchmark = _benchmark(result)
    changed = _result(hostile_holding=True)
    with pytest.raises(ValueError, match="different backtest result"):
        render_backtest_html(changed, benchmark)


def test_fixed_byte_limit_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("rquant.backtest.report.MAX_BACKTEST_HTML_BYTES", 256)
    with pytest.raises(ValueError, match="byte limit"):
        render_backtest_html(_result())


def test_real_paper_ledger_fill_and_single_day_chart_are_visible(tmp_path: Path) -> None:
    result = run_portfolio_backtest(_request(("600000.SH",)), research_root=tmp_path)
    html = render_backtest_html(result).html_bytes.decode("utf-8")

    assert "1 笔" in html
    assert "600000.SH" in html
    assert "<circle" in html  # A single date still has a visible chart mark.
