"""Tushare document parser and strategy-audit tests."""

from __future__ import annotations

from rquant.tushare_docs import (
    classify_tushare_interface,
    derive_interface_catalog_rows,
    parse_tushare_doc_page,
    parse_tushare_menu,
    parse_tushare_purchase_goods,
    render_audit_markdown,
)

SAMPLE_HTML = """
<section id="document">
<div id="jstree">
<ul>
<li><a href="/document/2?doc_id=14">股票数据</a>
  <ul>
    <li><a href="/document/2?doc_id=15">行情数据</a>
      <ul>
        <li><a href="/document/2?doc_id=370">历史分钟</a></li>
        <li><a href="/document/2?doc_id=374">实时分钟</a></li>
      </ul>
    </li>
  </ul>
</li>
<li><a href="/document/2?doc_id=147">宏观经济</a>
  <ul><li><a href="/document/2?doc_id=325">采购经理指数（PMI）</a></li></ul>
</li>
</ul>
</div>
<div class="content col-md-9 col-sm-8 col-xs-12">
<h2 id="a">A股实时分钟</h2>
<p>接口：rt_min<br>
描述：获取全A股票实时分钟数据，包括1~60min<br>
限量：单次最大1000行数据，可以通过股票代码提取数据<br>
权限：正式权限请参阅 权限说明</p>
<p><strong>输入参数</strong></p>
<table>
<thead><tr><th>名称</th><th>类型</th><th>必选</th><th>描述</th></tr></thead>
<tbody>
<tr><td>freq</td><td>str</td><td>Y</td><td>1MIN,5MIN,15MIN,30MIN,60MIN</td></tr>
<tr><td>ts_code</td><td>str</td><td>Y</td><td>支持单个和多个代码</td></tr>
</tbody>
</table>
<p><strong>输出参数</strong></p>
<table>
<thead><tr><th>名称</th><th>类型</th><th>默认显示</th><th>描述</th></tr></thead>
<tbody>
<tr><td>code</td><td>str</td><td>Y</td><td>股票代码</td></tr>
<tr><td>vol</td><td>float</td><td>Y</td><td>成交量(股）</td></tr>
</tbody>
</table>
</div>
</section>
"""


def test_parse_tushare_menu_keeps_nested_category_path() -> None:
    links = parse_tushare_menu(SAMPLE_HTML)

    by_id = {link.doc_id: link for link in links}

    assert by_id[14].category_path == ["股票数据"]
    assert by_id[14].is_section is True
    assert by_id[15].category_path == ["股票数据", "行情数据"]
    assert by_id[15].is_section is True
    assert by_id[374].category_path == ["股票数据", "行情数据", "实时分钟"]
    assert by_id[374].is_section is False


def test_parse_tushare_doc_page_extracts_api_notes_and_fields() -> None:
    menu_link = parse_tushare_menu(SAMPLE_HTML)[3]
    page = parse_tushare_doc_page(SAMPLE_HTML, menu_link)

    assert page.title == "A股实时分钟"
    assert page.api_name == "rt_min"
    assert page.description == "获取全A股票实时分钟数据，包括1~60min"
    assert page.limit_note == "单次最大1000行数据，可以通过股票代码提取数据"
    assert page.permission_note == "正式权限请参阅 权限说明"
    assert [field.name for field in page.input_fields] == ["freq", "ts_code"]
    assert page.input_fields[0].required == "Y"
    assert [field.name for field in page.output_fields] == ["code", "vol"]
    assert page.output_fields[1].description == "成交量(股）"


def test_parse_tushare_doc_page_extracts_history_start_month() -> None:
    html = SAMPLE_HTML.replace(
        "描述：获取全A股票实时分钟数据，包括1~60min<br>",
        "描述：获取当日个股和ETF的集合竞价成交情况。本接口历史数据开始于2025年1月。<br>",
    ).replace("接口：rt_min<br>", "接口：stk_auction<br>")
    menu_link = parse_tushare_menu(SAMPLE_HTML)[3].model_copy(
        update={"doc_id": 369, "title": "当日集合竞价"}
    )
    page = parse_tushare_doc_page(html, menu_link)

    assert page.history_coverage_type == "historical"
    assert page.history_start == "2025-01"
    assert page.history_coverage_note == "本接口历史数据开始于2025年1月。"


def test_parse_tushare_doc_page_marks_intraday_today_history() -> None:
    html = SAMPLE_HTML.replace(
        "描述：获取全A股票实时分钟数据，包括1~60min<br>",
        "描述：获取A股当日盘中历史分钟数据，可以提取单只股票当日开盘以来的所有分钟数据<br>",
    ).replace("接口：rt_min<br>", "接口：rt_min_daily<br>")
    menu_link = parse_tushare_menu(SAMPLE_HTML)[3].model_copy(
        update={"doc_id": 457, "title": "A股实时分钟-日累计"}
    )
    page = parse_tushare_doc_page(html, menu_link)

    assert page.history_coverage_type == "intraday_today"
    assert page.history_start is None
    assert page.history_coverage_note == "仅当日开盘以来"


def test_parse_tushare_doc_page_cleans_api_name_with_extra_sentence() -> None:
    html = SAMPLE_HTML.replace(
        "接口：rt_min<br>",
        "接口：daily，可以通过数据工具调试和查看数据<br>",
    )
    menu_link = parse_tushare_menu(SAMPLE_HTML)[3]
    page = parse_tushare_doc_page(html, menu_link)

    assert page.api_name == "daily"


def test_classify_tushare_interface_flags_realtime_monitoring_value() -> None:
    menu_link = parse_tushare_menu(SAMPLE_HTML)[3]
    page = parse_tushare_doc_page(SAMPLE_HTML, menu_link)

    row = classify_tushare_interface(page)

    assert row.is_a_share_related is True
    assert row.priority == 1
    assert "intraday_realtime" in row.capability_tags
    assert "盘中准实时监控" in row.strategy_uses


def test_classify_macro_doc_as_not_a_share_core() -> None:
    macro_html = SAMPLE_HTML.replace("A股实时分钟", "采购经理指数（PMI）").replace(
        "接口：rt_min<br>\n描述：获取全A股票实时分钟数据，包括1~60min<br>",
        "接口：cn_pmi<br>\n描述：获取中国采购经理指数月度数据<br>",
    )
    macro_link = parse_tushare_menu(SAMPLE_HTML)[4]
    page = parse_tushare_doc_page(macro_html, macro_link)

    row = classify_tushare_interface(page)

    assert row.is_a_share_related is False
    assert row.priority >= 4


def test_classify_does_not_treat_rate_limit_minutes_as_historical_minute() -> None:
    html = SAMPLE_HTML.replace("A股实时分钟", "A股日线行情").replace(
        "接口：rt_min<br>\n描述：获取全A股票实时分钟数据，包括1~60min<br>",
        "接口：daily<br>\n描述：获取股票行情数据。基础积分每分钟内可调取500次<br>",
    )
    menu_link = parse_tushare_menu(SAMPLE_HTML)[3].model_copy(
        update={"title": "历史日线", "category_path": ["股票数据", "行情数据", "历史日线"]}
    )
    page = parse_tushare_doc_page(html, menu_link)

    row = classify_tushare_interface(page)

    assert "historical_minute" not in row.capability_tags
    assert "daily_quote" in row.capability_tags


def test_render_audit_markdown_includes_priority_table() -> None:
    menu_link = parse_tushare_menu(SAMPLE_HTML)[3]
    page = parse_tushare_doc_page(SAMPLE_HTML, menu_link)
    row = classify_tushare_interface(page)

    markdown = render_audit_markdown([row])

    assert "## 优先接入接口" in markdown
    assert "| 374 | A股实时分钟 | rt_min | 1 |" in markdown
    assert "盘中准实时监控" in markdown


def test_derive_interface_catalog_rows_converts_all_a_share_rows() -> None:
    realtime_link = parse_tushare_menu(SAMPLE_HTML)[3]
    realtime_page = parse_tushare_doc_page(SAMPLE_HTML, realtime_link)
    realtime_row = classify_tushare_interface(realtime_page)

    macro_html = SAMPLE_HTML.replace("A股实时分钟", "采购经理指数（PMI）").replace(
        "接口：rt_min<br>\n描述：获取全A股票实时分钟数据，包括1~60min<br>",
        "接口：cn_pmi<br>\n描述：获取中国采购经理指数月度数据<br>",
    )
    macro_page = parse_tushare_doc_page(macro_html, parse_tushare_menu(SAMPLE_HTML)[4])
    macro_row = classify_tushare_interface(macro_page)

    rows = derive_interface_catalog_rows([realtime_row, macro_row])

    assert len(rows) == 1
    row = rows[0]
    assert row.doc_id == 374
    assert row.api_name == "rt_min"
    assert row.integration_stage == "stage_1_realtime"
    assert row.update_cadence == "intraday_realtime"
    assert row.target_table_hint == "minute_bar"
    assert row.permission_level == "official_permission"
    assert "盘中监控" in row.strategy_value
    assert row.history_coverage_type == "unknown"


def test_suspend_d_catalog_reflects_authoritative_integration() -> None:
    from rquant.tushare_docs import TushareDocPage

    page = TushareDocPage(
        doc_id=214,
        title="每日停复牌信息",
        category_path=["股票数据", "行情数据", "每日停复牌信息"],
        is_section=False,
        doc_url="https://tushare.pro/document/2?doc_id=214",
        api_name="suspend_d",
        description="按日期获取股票停复牌信息",
    )

    rows = derive_interface_catalog_rows([classify_tushare_interface(page)])

    assert rows[0].integration_status == "already_integrated"
    assert rows[0].target_table_hint == (
        "stock_suspend_event + stock_suspend_coverage"
    )
    assert rows[0].history_coverage_type == "unknown"


def test_parse_tushare_purchase_goods_keeps_api_permission_coverage() -> None:
    payload = {
        "code": 0,
        "data": [
            {
                "id": 25,
                "type": 2,
                "name": "集合竞价成交",
                "desc": "集合竞价数据",
                "price": 500.0,
                "duration_unit": "1Y",
                "doc_url": "https://tushare.pro/document/2?doc_id=369",
                "api_count": 3,
                "api_list": [
                    {
                        "api_id": 294,
                        "api_name": "stk_auction_o",
                        "api_title": "股票开盘集合竞价数据",
                        "api_doc_url": "https://tushare.pro/document/2?doc_id=353",
                    },
                    {
                        "api_id": 295,
                        "api_name": "stk_auction_c",
                        "api_title": "股票收盘集合竞价数据",
                        "api_doc_url": "https://tushare.pro/document/2?doc_id=354",
                    },
                    {
                        "api_id": 313,
                        "api_name": "stk_auction",
                        "api_title": "当日集合竞价",
                        "api_doc_url": "https://tushare.pro/document/2?doc_id=369",
                    },
                ],
            }
        ],
    }

    goods = parse_tushare_purchase_goods(payload)

    assert len(goods) == 1
    good = goods[0]
    assert good.good_id == 25
    assert good.good_type == 2
    assert good.doc_id == 369
    assert good.price == 500.0
    assert good.api_names == ["stk_auction_o", "stk_auction_c", "stk_auction"]
    assert good.api_doc_ids == [353, 354, 369]
