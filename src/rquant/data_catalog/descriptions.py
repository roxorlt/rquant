"""Human-reviewed Chinese copy for every published contract and physical field.

The builder fails when a new contract or column has no entry here. Units are left blank
when the source contract does not establish a stable unit across datasets.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetCopy:
    name: str
    purpose: str
    category: str


@dataclass(frozen=True)
class FieldCopy:
    name: str
    description: str
    unit: str | None = None


DATASETS: dict[str, DatasetCopy] = {
    "daily_bar": DatasetCopy("股票日线", "查看股票每天的开收盘价、成交量与涨跌", "行情"),
    "stock_status_daily": DatasetCopy("股票状态", "核对股票名称和 ST 状态在盘中的变化", "股票资料"),
    "minute_bar": DatasetCopy("股票分钟线", "查看盘中每分钟的价格与成交", "行情"),
    "auction_bar": DatasetCopy("集合竞价", "查看开盘竞价价格、成交与量比", "行情"),
    "stock_suspend_event": DatasetCopy(
        "停复牌事件", "追踪股票停牌与复牌公告对应的交易日", "股票资料"
    ),
    "stock_suspend_coverage": DatasetCopy(
        "停复牌采集记录", "检查每日停复牌信息是否采集完成", "数据质量"
    ),
    "adj_factor": DatasetCopy("复权因子", "为历史价格复权提供调整系数", "行情"),
    "limit_list_daily": DatasetCopy("涨跌停明细", "查看涨跌停股票及封单、开板情况", "行情"),
    "ths_daily": DatasetCopy("同花顺板块日线", "查看同花顺板块每天的行情", "板块"),
    "dc_daily": DatasetCopy("东方财富板块日线", "查看东方财富板块每天的行情", "板块"),
    "ths_index": DatasetCopy("同花顺板块", "查询同花顺板块名称与基础资料", "板块"),
    "ths_member": DatasetCopy("同花顺板块成分", "查找股票所属的同花顺板块", "板块"),
    "dc_index": DatasetCopy("东方财富板块", "查询东方财富板块的当日概况", "板块"),
    "dc_member": DatasetCopy("东方财富板块成分", "查找股票所属的东方财富板块", "板块"),
    "kpl_concept": DatasetCopy("概念成分", "查找概念板块的股票与热度", "板块"),
    "kpl_concept_daily": DatasetCopy("概念成分历史", "按交易日查看概念板块成分与热度", "板块"),
    "moneyflow": DatasetCopy("个股资金流向", "查看个股按单量划分的买卖资金", "资金"),
    "moneyflow_dc": DatasetCopy("东方财富个股资金", "查看东方财富口径的个股资金流向", "资金"),
    "moneyflow_ths": DatasetCopy("同花顺个股资金", "查看同花顺口径的个股资金流向", "资金"),
    "moneyflow_ind_ths": DatasetCopy("同花顺行业资金", "比较同花顺行业的资金流向", "资金"),
    "moneyflow_ind_dc": DatasetCopy("东方财富行业资金", "比较东方财富行业的资金流向", "资金"),
    "moneyflow_cnt_ths": DatasetCopy("同花顺概念资金", "比较同花顺概念板块的资金流向", "资金"),
    "moneyflow_mkt_dc": DatasetCopy("东方财富市场资金", "查看沪深市场整体资金流向", "资金"),
}


FIELDS: dict[str, FieldCopy] = {
    "adj_factor": FieldCopy("复权因子", "用于历史价格调整的因子"),
    "amount": FieldCopy("成交额", "该时段的成交金额；单位沿用数据源口径"),
    "auction_type": FieldCopy("竞价类型", "标识开盘或收盘竞价"),
    "available_at": FieldCopy("可用时间", "这条记录在研究中最早可使用的时间"),
    "avg_price": FieldCopy("均价", "该交易日的平均成交价"),
    "board_code": FieldCopy("板块代码", "该板块在数据源中的代码"),
    "board_name": FieldCopy("板块名称", "该板块在数据源中的名称"),
    "board_type": FieldCopy("板块类型", "数据源给出的板块分类"),
    "buy_elg_amount": FieldCopy("超大单买入额", "超大单买入金额；单位沿用数据源口径"),
    "buy_elg_amount_rate": FieldCopy("超大单买入占比", "超大单买入额的来源占比", "%"),
    "buy_elg_vol": FieldCopy("超大单买入量", "超大单买入成交量；单位沿用数据源口径"),
    "buy_lg_amount": FieldCopy("大单买入额", "大单买入金额；单位沿用数据源口径"),
    "buy_lg_amount_rate": FieldCopy("大单买入占比", "大单买入额的来源占比", "%"),
    "buy_lg_vol": FieldCopy("大单买入量", "大单买入成交量；单位沿用数据源口径"),
    "buy_md_amount": FieldCopy("中单买入额", "中单买入金额；单位沿用数据源口径"),
    "buy_md_amount_rate": FieldCopy("中单买入占比", "中单买入额的来源占比", "%"),
    "buy_md_vol": FieldCopy("中单买入量", "中单买入成交量；单位沿用数据源口径"),
    "buy_sm_amount": FieldCopy("小单买入额", "小单买入金额；单位沿用数据源口径"),
    "buy_sm_amount_rate": FieldCopy("小单买入占比", "小单买入额的来源占比", "%"),
    "buy_sm_amount_stock": FieldCopy("主力净流入最大股", "板块中主力资金净流入最多的股票"),
    "buy_sm_vol": FieldCopy("小单买入量", "小单买入成交量；单位沿用数据源口径"),
    "category": FieldCopy("板块分类", "数据源给出的板块类别"),
    "change": FieldCopy("涨跌额", "相对前收盘价的价格变化"),
    "close": FieldCopy("收盘价", "该交易日或时段结束时的价格"),
    "close_price": FieldCopy("领涨股最新价", "板块领涨股票的最新价格"),
    "close_sh": FieldCopy("沪市收盘点位", "上证指数的收盘点位", "点"),
    "close_sz": FieldCopy("深市收盘点位", "深证成指的收盘点位", "点"),
    "company_num": FieldCopy("公司数量", "板块包含的上市公司数量", "家"),
    "con_code": FieldCopy("成分股代码", "板块成分股票的代码"),
    "con_name": FieldCopy("成分股名称", "板块成分股票的名称"),
    "conflict_reason": FieldCopy("冲突原因", "多来源股票状态发生分歧时的原因"),
    "content_type": FieldCopy("内容类型", "数据源给出的行业内容分类"),
    "coverage_state": FieldCopy("采集状态", "该交易日停复牌采集的完成情况"),
    "created_at": FieldCopy("记录创建时间", "本地写入该记录的时间"),
    "description": FieldCopy("说明", "数据源给出的板块说明"),
    "down_num": FieldCopy("下跌家数", "板块内下跌的股票数量", "只"),
    "exchange": FieldCopy("交易所", "数据源给出的交易所"),
    "fd_amount": FieldCopy("封单额", "涨停或跌停封单金额；单位沿用数据源口径"),
    "first_time": FieldCopy("首次封板时间", "该交易日首次封板的时间"),
    "float_mv": FieldCopy("流通市值", "流通股市值；单位沿用数据源口径"),
    "freq": FieldCopy("分钟周期", "这一条分钟行情的时间周期"),
    "high": FieldCopy("最高价", "该交易日或时段内的最高价格"),
    "hot_num": FieldCopy("热度", "数据源给出的概念热度值"),
    "idx_type": FieldCopy("指数类型", "数据源给出的板块指数类型"),
    "industry": FieldCopy("行业", "数据源给出的行业名称"),
    "industry_index": FieldCopy("行业指数", "数据源给出的行业指数值", "点"),
    "ingested_at": FieldCopy("入库时间", "该记录写入本地数据集的时间"),
    "is_st": FieldCopy("ST 状态", "股票是否被标记为 ST 或 *ST"),
    "large_net_amount": FieldCopy("大单净额", "大单与超大单合计的买卖净额；单位沿用数据源口径"),
    "large_net_vol": FieldCopy("大单净量", "大单与超大单合计的买卖净量；单位沿用数据源口径"),
    "last_time": FieldCopy("最后封板时间", "该交易日最后一次封板的时间"),
    "latest": FieldCopy("最新价", "数据源给出的最新成交价格"),
    "lead_stock": FieldCopy("领涨股票", "板块内领涨股票名称或代码"),
    "leading_code": FieldCopy("领涨股代码", "板块领涨股票的代码"),
    "leading_name": FieldCopy("领涨股名称", "板块领涨股票的名称"),
    "leading_pct": FieldCopy("领涨股涨跌幅", "板块领涨股票的涨跌幅", "%"),
    "level": FieldCopy("板块层级", "数据源给出的板块层级"),
    "limit_amount": FieldCopy("板上成交额", "封板期间的成交金额；单位沿用数据源口径"),
    "limit_status": FieldCopy("涨跌停状态", "标识涨停或跌停状态"),
    "limit_times": FieldCopy("连续涨停次数", "数据源给出的连续涨停次数", "次"),
    "list_date": FieldCopy("上市日期", "股票或板块的上市日期"),
    "low": FieldCopy("最低价", "该交易日或时段内的最低价格"),
    "member_count": FieldCopy("成分数量", "板块内的成分股票数量", "只"),
    "name": FieldCopy("名称", "数据源给出的股票或板块名称"),
    "name_source": FieldCopy("名称来源", "股票名称所采用的数据来源"),
    "net_amount": FieldCopy("资金净额", "买入与卖出金额之差；单位沿用数据源口径"),
    "net_amount_rank": FieldCopy("资金净额排名", "按资金净额排列的来源排名", "名"),
    "net_amount_rate": FieldCopy("资金净额占比", "资金净额的来源占比", "%"),
    "net_buy_amount": FieldCopy("净买入额", "数据源给出的净买入金额；单位沿用数据源口径"),
    "net_d5_amount": FieldCopy("近五日资金净额", "近五个交易日资金净额；单位沿用数据源口径"),
    "net_sell_amount": FieldCopy("净卖出额", "数据源给出的净卖出金额；单位沿用数据源口径"),
    "open": FieldCopy("开盘价", "该交易日或时段开始时的价格"),
    "open_times": FieldCopy("开板次数", "该交易日封板后再次打开的次数", "次"),
    "pct_change": FieldCopy("涨跌幅", "相对前收盘价的涨跌百分比", "%"),
    "pct_change_sh": FieldCopy("沪市涨跌幅", "上证指数当日涨跌幅", "%"),
    "pct_change_stock": FieldCopy("领涨股涨跌幅", "板块领涨股票的涨跌幅", "%"),
    "pct_change_sz": FieldCopy("深市涨跌幅", "深证成指当日涨跌幅", "%"),
    "pct_chg": FieldCopy("涨跌幅", "相对前收盘价的涨跌百分比", "%"),
    "pre_close": FieldCopy("前收盘价", "上一交易日的收盘价格"),
    "price": FieldCopy("竞价价格", "集合竞价形成的价格"),
    "queried_at": FieldCopy("查询时间", "完成该次源端查询的时间"),
    "row_count": FieldCopy("记录条数", "该次采集得到的停复牌记录数", "条"),
    "sell_elg_amount": FieldCopy("超大单卖出额", "超大单卖出金额；单位沿用数据源口径"),
    "sell_elg_vol": FieldCopy("超大单卖出量", "超大单卖出成交量；单位沿用数据源口径"),
    "sell_lg_amount": FieldCopy("大单卖出额", "大单卖出金额；单位沿用数据源口径"),
    "sell_lg_vol": FieldCopy("大单卖出量", "大单卖出成交量；单位沿用数据源口径"),
    "sell_md_amount": FieldCopy("中单卖出额", "中单卖出金额；单位沿用数据源口径"),
    "sell_md_vol": FieldCopy("中单卖出量", "中单卖出成交量；单位沿用数据源口径"),
    "sell_sm_amount": FieldCopy("小单卖出额", "小单卖出金额；单位沿用数据源口径"),
    "sell_sm_vol": FieldCopy("小单卖出量", "小单卖出成交量；单位沿用数据源口径"),
    "session_scope": FieldCopy("交易时段范围", "停复牌事件影响的交易时段"),
    "snapshot_hash": FieldCopy("采集摘要", "该次采集结果的校验摘要"),
    "source": FieldCopy("数据来源", "这条记录实际采用的数据源"),
    "st_source": FieldCopy("ST 来源", "ST 状态所采用的数据来源"),
    "suspend_timing": FieldCopy("停复牌时点", "停复牌事件发生在交易日内的时点"),
    "suspend_type": FieldCopy("停复牌类型", "标识停牌或复牌"),
    "swing": FieldCopy("振幅", "该交易日最高与最低价形成的价格振幅", "%"),
    "total_mv": FieldCopy("总市值", "全部股份的市值；单位沿用数据源口径"),
    "trade_date": FieldCopy("交易日期", "该记录所属的交易日"),
    "trade_time": FieldCopy("交易时间", "该条分钟行情对应的时间"),
    "ts_code": FieldCopy("证券或板块代码", "该记录对应的股票、指数或板块代码"),
    "turnover_rate": FieldCopy("换手率", "成交股份占流通股份的比例", "%"),
    "turnover_ratio": FieldCopy("换手率", "该交易日股票的换手率", "%"),
    "up_num": FieldCopy("上涨家数", "板块内上涨的股票数量", "只"),
    "up_stat": FieldCopy("连板统计", "数据源给出的连续涨停统计"),
    "updated_at": FieldCopy("最近更新时间", "这条资料最近一次更新的时间"),
    "vol": FieldCopy("成交量", "该交易日或时段的成交量；单位沿用数据源口径"),
    "volume_ratio": FieldCopy("量比", "当前成交量与历史同期成交量的比值"),
}
