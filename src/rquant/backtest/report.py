"""Deterministic, self-contained HTML views of a verified daily account ledger."""

from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal
from html import escape
from typing import Self

import pandas as pd
from pydantic import model_validator

from rquant.backtest.benchmark import (
    BenchmarkComparison,
    BenchmarkSeries,
    compare_backtest_to_benchmark,
)
from rquant.backtest.contracts import BacktestResult, Sha256
from rquant.paper_contracts import PaperOrderStatus
from rquant.perf import EquityCurve, PerformanceSummary, equity_curve, performance_summary
from rquant.runtime_contracts import RuntimeContractModel

MAX_BACKTEST_HTML_BYTES = 4 * 1024 * 1024


class BacktestHtmlReport(RuntimeContractModel):
    html_bytes: bytes
    sha256: Sha256

    @model_validator(mode="after")
    def validate_content(self) -> Self:
        if len(self.html_bytes) > MAX_BACKTEST_HTML_BYTES:
            raise ValueError("backtest HTML exceeds the fixed byte limit")
        if self.sha256 != hashlib.sha256(self.html_bytes).hexdigest():
            raise ValueError("backtest HTML sha256 does not match its bytes")
        return self


def _safe(value: object) -> str:
    return escape(str(value), quote=True)


def _date(value: date) -> str:
    return f"{value.year}年{value.month}月{value.day}日"


def _money(value: Decimal | None) -> str:
    return "—" if value is None else f"{value:,.2f} 元"


def _nav(value: Decimal | float | None) -> str:
    return "—" if value is None else f"{value:,.4f}"


def _rate(value: Decimal | float | None) -> str:
    if value is None:
        return "—"
    rounded = Decimal(f"{Decimal(str(value)) * 100:.2f}")
    prefix = "+" if rounded > 0 else "−" if rounded < 0 else ""
    return f"{prefix}{abs(rounded):,.2f}%"


def _number(value: int | float | None) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        return f"{value:,}"
    rounded = Decimal(f"{value:.2f}")
    return f"{'−' if rounded < 0 else ''}{abs(rounded):,.2f}"


def _verify_complete_result(result: BacktestResult) -> BacktestResult:
    if not isinstance(result, BacktestResult):
        raise TypeError("a BacktestResult is required")
    verified = BacktestResult.model_validate(result.model_dump(mode="python"))
    if verified.status != "complete":
        raise ValueError("backtest HTML requires a complete account ledger")
    dates = tuple(day.trade_date for day in verified.days)
    if tuple(sorted(set(dates))) != dates:
        raise ValueError("backtest account dates must be ordered and unique")
    first = verified.days[0]
    assert first.account is not None
    if first.normalized_nav is None or first.normalized_nav <= 0:
        raise ValueError("backtest normalized NAV cannot establish initial cash")
    initial_cash = (first.account.nav / first.normalized_nav).quantize(Decimal("0.01"))
    if initial_cash <= 0:
        raise ValueError("backtest initial cash must be positive")
    previous_nav = initial_cash
    for day in verified.days:
        assert day.account is not None
        if (
            day.daily_return is None
            or day.normalized_nav is None
            or day.daily_return < -1
            or day.normalized_nav < 0
            or previous_nav <= 0
        ):
            raise ValueError("backtest account lacks a complete daily return and NAV series")
        if day.daily_return != day.account.nav / previous_nav - 1:
            raise ValueError("backtest daily return disagrees with account NAV")
        if day.normalized_nav != day.account.nav / initial_cash:
            raise ValueError("backtest normalized NAV disagrees with account NAV")
        previous_nav = day.account.nav
    return verified


def _returns(result: BacktestResult) -> pd.Series:
    return pd.Series(
        (float(day.daily_return) for day in result.days),
        index=pd.DatetimeIndex(day.trade_date for day in result.days),
        dtype="float64",
    )


def _metric(label: str, value: str) -> str:
    return f'<div class="metric"><dt>{_safe(label)}</dt><dd>{_safe(value)}</dd></div>'


def _metrics(summary: PerformanceSummary) -> str:
    items = (
        ("区间收益", _rate(summary.total_return)),
        ("年化收益", _rate(summary.annualized_return)),
        ("最大回撤", _rate(summary.max_drawdown)),
        ("年化波动", _rate(summary.annualized_volatility)),
        ("夏普比率", _number(summary.sharpe)),
        ("卡玛比率", _number(summary.calmar)),
        ("胜率", _rate(summary.win_rate)),
        (
            "回撤持续",
            "—" if summary.max_drawdown_duration is None else f"{summary.max_drawdown_duration} 日",
        ),
    )
    return '<dl class="metrics">' + "".join(_metric(*item) for item in items) + "</dl>"


def _chart_path(values: tuple[float, ...], low: float, high: float) -> str:
    return " ".join(
        f"{'M' if index == 0 else 'L'}{56 + 640 * index / max(1, len(values) - 1):.2f},"
        f"{22 + 160 * (high - value) / (high - low):.2f}"
        for index, value in enumerate(values)
    )


def _chart_single_point(values: tuple[float, ...], low: float, high: float) -> str:
    if len(values) != 1:
        return ""
    y = 22 + 160 * (high - values[0]) / (high - low)
    return f'<circle cx="56" cy="{y:.2f}" r="4"/>'


def _chart(
    *,
    chart_id: str,
    title: str,
    dates: tuple[date, ...],
    strategy: tuple[float, ...],
    benchmark: tuple[float, ...] | None,
    benchmark_label: str | None,
    baseline: float,
    rate_axis: bool,
) -> str:
    values = (*strategy, *(benchmark or ()), baseline)
    low, high = min(values), max(values)
    if low == high:
        low -= 0.01
        high += 0.01
    pad = (high - low) * 0.09
    low -= pad
    high += pad
    baseline_y = 22 + 160 * (high - baseline) / (high - low)
    top_label = _rate(high) if rate_axis else _nav(high)
    bottom_label = _rate(low) if rate_axis else _nav(low)
    formatter = _rate if rate_axis else _nav
    chart_data = f"起点 {formatter(strategy[0])} · 终点 {formatter(strategy[-1])}"
    benchmark_markup = ""
    benchmark_legend = ""
    if benchmark is not None:
        benchmark_markup = (
            '<g class="series benchmark" data-series="benchmark">'
            f'<path d="{_chart_path(benchmark, low, high)}"/>'
            f"{_chart_single_point(benchmark, low, high)}</g>"
        )
        benchmark_legend = (
            f'<span><i class="swatch benchmark-swatch"></i>{_safe(benchmark_label)}</span>'
        )
    return f"""
      <figure class="chart-card">
        <figcaption><h3>{_safe(title)}</h3><span>逐个交易日 · 收盘估值</span></figcaption>
        <svg viewBox="0 0 760 242" role="img" aria-labelledby="{chart_id}-title">
          <title id="{chart_id}-title">{_safe(title)}</title>
          <line class="grid" x1="56" x2="696" y1="22" y2="22"/>
          <line class="grid" x1="56" x2="696" y1="182" y2="182"/>
          <line class="baseline" x1="56" x2="696" y1="{baseline_y:.2f}" y2="{baseline_y:.2f}"/>
          <text class="axis" x="48" y="26" text-anchor="end">{_safe(top_label)}</text>
          <text class="axis" x="48" y="185" text-anchor="end">{_safe(bottom_label)}</text>
          <text class="axis" x="56" y="220">{_safe(_date(dates[0]))}</text>
          <text class="axis" x="696" y="220" text-anchor="end">{_safe(_date(dates[-1]))}</text>
          <g class="series strategy" data-series="strategy">
            <path d="{_chart_path(strategy, low, high)}"/>
            {_chart_single_point(strategy, low, high)}
          </g>
          {benchmark_markup}
        </svg>
        <div class="chart-scale" aria-label="图表刻度">
          <span>上限 {_safe(top_label)}</span><span>下限 {_safe(bottom_label)}</span>
          <span>{_safe(_date(dates[0]))}</span><span>{_safe(_date(dates[-1]))}</span>
        </div>
        <div class="legend">
          <span><i class="swatch strategy-swatch"></i>组合账本</span>{benchmark_legend}
        </div>
        <p class="chart-data">{_safe(chart_data)}</p>
      </figure>"""


def _ledger(result: BacktestResult) -> str:
    labels = ("日期", "账户净值", "归一净值", "当日收益", "现金", "持仓市值", "费用", "持仓")
    rows = []
    for day in result.days:
        assert day.account is not None
        values = (
            _date(day.trade_date),
            _money(day.account.nav),
            _nav(day.normalized_nav),
            _rate(day.daily_return),
            _money(day.account.cash),
            _money(day.market_value),
            _money(day.fees),
            f"{len(day.account.holdings)} 只",
        )
        rows.append(
            "<tr>"
            + "".join(
                f'<td data-label="{_safe(label)}">{_safe(value)}</td>'
                for label, value in zip(labels, values, strict=True)
            )
            + "</tr>"
        )
    return (
        '<div class="table-wrap"><table><thead><tr>'
        + "".join(f'<th scope="col">{_safe(label)}</th>' for label in labels)
        + "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )


def _execution(result: BacktestResult) -> str:
    orders = tuple(order for day in result.days for order in day.orders)
    fills = tuple(order.receipt.fill for order in orders if order.receipt.fill is not None)
    rejected = sum(order.receipt.order.status is PaperOrderStatus.REJECTED for order in orders)
    notional = sum((fill.notional for fill in fills), Decimal("0"))
    fees = sum((day.fees for day in result.days), Decimal("0"))
    summary = (
        '<dl class="metrics compact">'
        + _metric("成交", f"{len(fills)} 笔")
        + _metric("拒单", f"{rejected} 笔")
        + _metric("成交额", _money(notional))
        + _metric("累计费用", _money(fees))
        + "</dl>"
    )
    if not fills:
        summary += '<p class="empty-note">无成交记录；订单和持仓只按已记录的账本展示。</p>'
    final_account = result.days[-1].account
    assert final_account is not None
    if not final_account.holdings:
        holdings = '<p class="empty-note">期末空仓。</p>'
    else:
        holdings = (
            '<div class="holding-list">'
            + "".join(
                '<div class="holding">'
                f"<strong>{_safe(holding.code)}</strong>"
                f"<span>{_safe(f'{holding.quantity:,} 股')}</span>"
                f"<span>收盘价 {_safe(_money(holding.market_price))}</span>"
                f"<span>市值 {_safe(_money(holding.quantity * holding.market_price))}</span>"
                "</div>"
                for holding in final_account.holdings
            )
            + "</div>"
        )
    return summary + '<h3 class="subhead">期末持仓</h3>' + holdings


def _benchmark_section(
    benchmark: BenchmarkSeries | None, comparison: BenchmarkComparison | None
) -> str:
    if benchmark is None or comparison is None:
        return (
            '<section class="section benchmark-note" aria-labelledby="benchmark-title">'
            '<div class="section-head"><h2 id="benchmark-title">指数基准</h2></div>'
            '<p class="empty-note">基准尚未提供；相对表现指标因此不可计算。</p></section>'
        )
    relative = comparison.relative
    items = (
        ("基准区间收益", _rate(comparison.benchmark_performance.total_return)),
        ("超额收益", _rate(relative.excess_total_return)),
        ("年化 Alpha", _rate(relative.alpha)),
        ("Beta", _number(relative.beta)),
        ("跟踪误差", _rate(relative.tracking_error)),
        ("信息比率", _number(relative.information_ratio)),
    )
    metrics = "".join(_metric(*item) for item in items)
    return f"""
      <section class="section benchmark-note" aria-labelledby="benchmark-title">
        <div class="section-head"><h2 id="benchmark-title">指数基准</h2>
          <span class="section-tag">{_safe(benchmark.ts_code)}</span></div>
        <dl class="metrics compact">{metrics}</dl>
        <p class="source-note">指数基准使用事后指数收盘价，仅供离线回顾比较；
        不代表历史盘前已取得，也不得作为 09:25 决策输入。</p>
      </section>"""


def _provenance(result: BacktestResult, benchmark: BenchmarkSeries | None) -> str:
    values = (
        ("回测结果 SHA-256", result.content_hash),
        ("请求标识", result.request_id),
        ("输入代际", result.input_generation_id),
        ("日历来源", result.calendar_source_identity),
        ("费用口径", result.cost_spec_id),
        ("代码提交", result.producer_commit),
        ("指数来源 SHA-256", None if benchmark is None else benchmark.source_identity),
    )
    if benchmark is not None:
        values += (
            ("基准来源模式", benchmark.source_mode),
            ("基准来源表", benchmark.source_table),
        )
    return (
        '<details class="provenance"><summary>技术标识与来源</summary><dl>'
        + "".join(
            f"<div><dt>{_safe(label)}</dt>"
            f"<dd>{_safe(value if value is not None else '—')}</dd></div>"
            for label, value in values
        )
        + "</dl></details>"
    )


_STYLE = """
  :root{color-scheme:light;--paper:#fcfbf8;--ink:#223535;--muted:#607572;--line:#d8e1dc;
    --teal:#1d686b;--sand:#b37b3e;--wash:#edf4f1;--serif:Georgia,"Songti SC",serif;
    --sans:"PingFang SC","Hiragino Sans GB","Noto Sans CJK SC",-apple-system,BlinkMacSystemFont,
    sans-serif}
  *{box-sizing:border-box}html{background:#e8eeea}body{margin:0;color:var(--ink);
    font-family:var(--sans);
    font-size:14px;line-height:1.6}main{max-width:1120px;margin:28px auto;padding:54px 56px 64px;
    background:var(--paper);box-shadow:0 12px 50px #213a3212}h1,h2,h3,p,figure,dl{margin:0}
  .kicker{color:var(--teal);font-size:11px;letter-spacing:.17em;font-weight:700;
    text-transform:uppercase}
  .masthead{border-top:4px solid var(--teal);padding-top:25px}
    .masthead h1{font-family:var(--serif);
    font-size:clamp(31px,4vw,47px);line-height:1.18;letter-spacing:.015em;margin:13px 0 12px}
  .deck{max-width:700px;color:var(--muted)}.period{margin-top:20px;color:var(--ink);font-size:13px}
  .hero{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(230px,.7fr);gap:28px;
    margin:42px 0 38px;padding:30px 34px;background:var(--wash);border-left:3px solid var(--teal)}
  .eyebrow{font-size:12px;letter-spacing:.08em;color:var(--muted)}
    .hero-number{font-family:var(--serif);
    font-size:clamp(34px,5vw,56px);line-height:1.12;letter-spacing:-.025em;margin:8px 0 4px;
    font-variant-numeric:tabular-nums;overflow-wrap:anywhere}
    .hero-return{color:var(--teal);font-weight:700;
    font-size:19px}
  .hero-copy{align-self:end;color:var(--muted);font-size:13px}.hero-copy strong{display:block;
    color:var(--ink);font-size:15px;margin-bottom:9px}
  .section{border-top:1px solid var(--line);padding:30px 0 36px}.section-head{display:flex;
    align-items:baseline;justify-content:space-between;gap:12px;margin-bottom:20px}
  .section-head h2{font-family:var(--serif);font-size:25px;font-weight:600;line-height:1.25}
  .section-tag{font-size:12px;color:var(--teal);font-weight:600;overflow-wrap:anywhere}
  .metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:0}
  .metric{margin:0;padding:15px 17px;border:1px solid var(--line);min-width:0;background:#fff}
  .metric dt{font-size:12px;color:var(--muted)}.metric dd{margin:5px 0 0;font-size:20px;
    font-weight:650;font-variant-numeric:tabular-nums;overflow-wrap:anywhere}
    .compact .metric dd{font-size:17px}
  .charts{display:grid;grid-template-columns:1fr 1fr;gap:14px}.chart-card{padding:17px 16px 13px;
    background:#fff;border:1px solid var(--line);min-width:0}.chart-card figcaption{display:flex;
    justify-content:space-between;gap:12px;align-items:baseline}.chart-card h3{font-size:16px}
  .chart-card figcaption span{color:var(--muted);font-size:11px}.chart-card svg{display:block;
    width:100%;
    height:auto;max-width:100%;margin-top:10px;overflow:visible}.grid{stroke:#e5ebe7;
    stroke-width:1}
  .baseline{stroke:#aabbb2;stroke-width:1;stroke-dasharray:4 4}.axis{fill:var(--muted);
    font:11px var(--sans)}.series path{fill:none;stroke-width:2.7;stroke-linecap:round;
    stroke-linejoin:round}.strategy path{stroke:var(--teal)}.benchmark path{stroke:var(--sand)}
  .strategy circle{fill:var(--teal)}.benchmark circle{fill:var(--sand)}
  .legend{display:flex;flex-wrap:wrap;gap:15px;color:var(--muted);font-size:11px;
    overflow-wrap:anywhere}
  .chart-data{font-size:12px;color:var(--ink);margin-top:8px;font-variant-numeric:tabular-nums}
  .chart-scale{display:none}
  .swatch{display:inline-block;width:14px;height:2px;vertical-align:middle;margin-right:5px;
    background:var(--teal)}.benchmark-swatch{background:var(--sand)}
  .source-note,.empty-note{color:var(--muted);font-size:12px;line-height:1.7;margin-top:15px}
  .table-wrap{width:100%}table{width:100%;border-collapse:collapse;font-size:12px;
    font-variant-numeric:tabular-nums}th,td{padding:11px 8px;border-bottom:1px solid var(--line);
    text-align:right;white-space:nowrap}th:first-child,td:first-child{text-align:left}
  th{color:var(--muted);font-weight:500;background:var(--wash)}
    tbody tr:last-child td{font-weight:650}
  .subhead{margin:26px 0 8px;font-size:15px}.holding-list{display:grid;gap:8px;margin-top:12px}
  .holding{display:grid;grid-template-columns:1.25fr .55fr 1fr 1fr;gap:12px;align-items:center;
    border-bottom:1px solid var(--line);padding:10px 0;font-size:12px;overflow-wrap:anywhere}
  .holding span{text-align:right}.provenance{border-top:1px solid var(--line);padding-top:20px;
    color:var(--muted);font-size:12px}.provenance summary{cursor:pointer;color:var(--ink);
    font-weight:600}.provenance summary:focus-visible{outline:2px solid var(--teal);
    outline-offset:4px}
  .provenance dl{margin-top:14px}.provenance dl>div{display:grid;
    grid-template-columns:135px minmax(0,1fr);
    gap:12px;padding:5px 0}.provenance dd{margin:0;overflow-wrap:anywhere;
    font-family:ui-monospace,monospace}
  .foot{font-size:11px;color:var(--muted);margin-top:35px}
  @media(max-width:850px){.charts{grid-template-columns:1fr}
    .metrics{grid-template-columns:repeat(2,minmax(0,1fr))}}
  @media(max-width:640px){html{background:var(--paper)}main{margin:0;padding:27px 18px 42px;
    box-shadow:none}.hero{grid-template-columns:1fr;margin:31px 0;padding:24px 22px;gap:18px}
    .section{padding:25px 0 29px}.section-head h2{font-size:22px}.metric{padding:12px}
    .metric dd{font-size:17px}.compact .metric dd{font-size:15px}.chart-card{padding:12px 8px}
    .chart-card figcaption{padding:0 8px}.chart-card figcaption span{font-size:10px}
    .chart-card .axis{display:none}
    .chart-scale{display:grid;grid-template-columns:1fr 1fr;gap:2px 8px;
      font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums}
    .chart-scale span{min-width:0;overflow-wrap:anywhere}
    .chart-scale span:nth-child(even){text-align:right}
    table,tbody,tr,td{display:block;width:100%}thead{display:none}tr{padding:9px 12px;
      border:1px solid var(--line);margin:0 0 10px;background:#fff}td,
    td:first-child{text-align:right;
      border:0;padding:4px 0;white-space:normal;display:flex;justify-content:space-between;
      gap:10px;overflow-wrap:anywhere}td::before{content:attr(data-label);color:var(--muted);
      font-weight:400;flex:none}td:first-child{font-weight:650;text-align:right}
    .holding{grid-template-columns:1fr 1fr;gap:3px}.holding span{text-align:right}
    .provenance dl>div{grid-template-columns:1fr;gap:0}}
  @media print{html,body{background:#fff}main{max-width:none;margin:0;padding:0;box-shadow:none}
    .section,.chart-card,tr,.holding{break-inside:avoid}.provenance{display:none}}"""


def render_backtest_html(
    result: BacktestResult, benchmark: BenchmarkSeries | None = None
) -> BacktestHtmlReport:
    """Render bounded offline bytes; no network, files, scripts, or ambient time."""
    verified = _verify_complete_result(result)
    comparison = None
    if benchmark is not None:
        if not isinstance(benchmark, BenchmarkSeries):
            raise TypeError("a BenchmarkSeries is required")
        benchmark = BenchmarkSeries.model_validate(benchmark.model_dump(mode="python"))
        comparison = compare_backtest_to_benchmark(verified, benchmark)
    returns = _returns(verified)
    curve: EquityCurve = equity_curve(returns)
    summary = comparison.strategy_performance if comparison else performance_summary(returns)
    dates = tuple(day.trade_date for day in verified.days)
    benchmark_curve = None
    if benchmark is not None:
        benchmark_curve = equity_curve(
            pd.Series(
                (day.daily_return for day in benchmark.days),
                index=pd.DatetimeIndex(dates),
                dtype="float64",
            )
        )
    benchmark_label = None if benchmark is None else f"指数 {benchmark.ts_code}"
    nav_chart = _chart(
        chart_id="nav",
        title="归一净值走势",
        dates=dates,
        strategy=tuple(float(day.normalized_nav) for day in verified.days),
        benchmark=None
        if benchmark is None
        else tuple(day.normalized_nav for day in benchmark.days),
        benchmark_label=benchmark_label,
        baseline=1.0,
        rate_axis=False,
    )
    drawdown_chart = _chart(
        chart_id="drawdown",
        title="回撤走势",
        dates=dates,
        strategy=tuple(float(value) for value in curve.drawdown),
        benchmark=None
        if benchmark_curve is None
        else tuple(float(value) for value in benchmark_curve.drawdown),
        benchmark_label=benchmark_label,
        baseline=0.0,
        rate_axis=True,
    )
    final_day = verified.days[-1]
    assert final_day.account is not None
    content_security_policy = (
        "default-src 'none'; style-src 'unsafe-inline'; script-src 'none'; "
        "font-src 'none'; base-uri 'none'; form-action 'none'"
    )
    html = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="{content_security_policy}">
<title>组合回测研究报告 · rQuant</title><style>{_STYLE}</style></head>
<body><main>
  <header class="masthead">
    <p class="kicker">rQuant / 组合研究</p>
    <h1>组合回测研究报告</h1>
    <p class="deck">逐日账户账本的离线视图。收益与曲线来自收盘估值；指标仅作研究参考。</p>
    <p class="period">{_safe(_date(dates[0]))} — {_safe(_date(dates[-1]))}
      · {_safe(f"{len(dates):,} 个交易日")}</p>
  </header>
  <section class="hero" aria-label="期末概览">
    <div><p class="eyebrow">期末账户净值</p>
      <p class="hero-number">{_safe(_money(final_day.account.nav))}</p>
      <p class="hero-return">{_safe(_rate(summary.total_return))}
        <span class="eyebrow">区间收益</span></p></div>
    <div class="hero-copy"><strong>归一净值 {_safe(_nav(final_day.normalized_nav))}</strong>
      <p>以账本初始资金为 1.0000。每日现金、持仓市值与账户净值可在下方逐日核对。</p></div>
  </section>
  <section class="section" aria-labelledby="metrics-title">
    <div class="section-head"><h2 id="metrics-title">绩效概览</h2>
      <span class="section-tag">按 252 个交易日年化</span></div>
    {_metrics(summary)}</section>
  <section class="section" aria-labelledby="curves-title">
    <div class="section-head"><h2 id="curves-title">资金曲线</h2>
      <span class="section-tag">按交易日观察</span></div>
    <div class="charts">{nav_chart}{drawdown_chart}</div></section>
  {_benchmark_section(benchmark, comparison)}
  <section class="section" aria-labelledby="ledger-title">
    <div class="section-head"><h2 id="ledger-title">逐日账本</h2>
      <span class="section-tag">收盘估值</span></div>{_ledger(verified)}</section>
  <section class="section" aria-labelledby="execution-title">
    <div class="section-head"><h2 id="execution-title">成交与持仓</h2>
      <span class="section-tag">模拟执行账本</span></div>{_execution(verified)}</section>
  <section class="section" aria-labelledby="assumption-title">
    <div class="section-head"><h2 id="assumption-title">口径与限制</h2></div>
    <p class="source-note">{_safe(verified.execution_assumption)}。
    当日开盘价用于模拟撮合，收盘价用于日终估值；
    图表并非真实预挂单或实盘成交记录。</p></section>
  {_provenance(verified, benchmark)}
  <p class="foot">rQuant · 离线研究报告 · 内容由已验证的回测结果确定</p>
</main></body></html>
"""
    payload = html.encode("utf-8")
    if len(payload) > MAX_BACKTEST_HTML_BYTES:
        raise ValueError("backtest HTML exceeds the fixed byte limit")
    return BacktestHtmlReport(html_bytes=payload, sha256=hashlib.sha256(payload).hexdigest())
