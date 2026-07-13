"""DuckDB 表结构定义。所有 DDL 集中在这里，便于查表和迁移。"""

DAILY_BAR_DDL = """
CREATE TABLE IF NOT EXISTS daily_bar (
    ts_code     VARCHAR NOT NULL,
    trade_date  DATE    NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    pre_close   DOUBLE,
    change      DOUBLE,
    pct_chg     DOUBLE,
    vol         DOUBLE,
    amount      DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

INDEX_DAILY_BAR_DDL = """
CREATE TABLE IF NOT EXISTS index_daily_bar (
    ts_code     VARCHAR NOT NULL,
    trade_date  DATE    NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    pre_close   DOUBLE,
    change      DOUBLE,
    pct_chg     DOUBLE,
    vol         DOUBLE,
    amount      DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

STOCK_BASIC_DDL = """
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code     VARCHAR PRIMARY KEY,
    symbol      VARCHAR,
    name        VARCHAR,
    area        VARCHAR,
    industry    VARCHAR,
    list_date   DATE,
    market      VARCHAR,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

ADJ_FACTOR_DDL = """
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code     VARCHAR NOT NULL,
    trade_date  DATE    NOT NULL,
    adj_factor  DOUBLE  NOT NULL,
    PRIMARY KEY (ts_code, trade_date)
);
"""

DAILY_INDICATOR_DDL = """
CREATE TABLE IF NOT EXISTS daily_indicator (
    ts_code     VARCHAR NOT NULL,
    trade_date  DATE    NOT NULL,
    ma5         DOUBLE,
    ma10        DOUBLE,
    ma20        DOUBLE,
    ma60        DOUBLE,
    rsi6        DOUBLE,
    rsi14       DOUBLE,
    macd        DOUBLE,
    macd_signal DOUBLE,
    macd_hist   DOUBLE,
    kdj_k       DOUBLE,
    kdj_d       DOUBLE,
    kdj_j       DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

DAILY_STATE_DDL = """
CREATE TABLE IF NOT EXISTS daily_state (
    ts_code               VARCHAR NOT NULL,
    trade_date            DATE    NOT NULL,
    is_st                 BOOLEAN,
    is_bj                 BOOLEAN,
    board_type            VARCHAR,   -- main | gem | star | bj（主板/创业板/科创板/北交所）
    limit_pct             DOUBLE,    -- 0.05 | 0.10 | 0.20 | 0.30
    limit_up_price        DOUBLE,
    limit_down_price      DOUBLE,
    is_limit_up           BOOLEAN,
    is_limit_down         BOOLEAN,
    is_first_limit_up     BOOLEAN,   -- 今涨停且昨未涨停
    is_yiziban            BOOLEAN,   -- 一字板（open=high=low=close 且涨停）
    consecutive_limit_ups INTEGER,   -- 连板数（含今日，0 表示今日未涨停）
    body_upper            DOUBLE,    -- max(open, close) 实体上沿
    body_lower            DOUBLE,    -- min(open, close) 实体下沿
    PRIMARY KEY (ts_code, trade_date)
);
"""

DAILY_BASIC_DDL = """
CREATE TABLE IF NOT EXISTS daily_basic (
    ts_code        VARCHAR NOT NULL,
    trade_date     DATE    NOT NULL,
    turnover_rate  DOUBLE,
    volume_ratio   DOUBLE,
    total_mv       DOUBLE,
    circ_mv        DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

MONEYFLOW_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS moneyflow_daily (
    ts_code          VARCHAR NOT NULL,
    trade_date       DATE    NOT NULL,
    buy_lg_vol       DOUBLE,
    sell_lg_vol      DOUBLE,
    buy_elg_vol      DOUBLE,
    sell_elg_vol     DOUBLE,
    large_net_vol    DOUBLE,
    large_net_amount DOUBLE,
    source           VARCHAR DEFAULT 'tushare',
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, source)
);
"""

MARKET_SENTIMENT_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS market_sentiment_daily (
    trade_date                   DATE PRIMARY KEY,
    stock_count                  INTEGER,
    up_count                     INTEGER,
    down_count                   INTEGER,
    flat_count                   INTEGER,
    limit_up_count               INTEGER,
    first_limit_up_count         INTEGER,
    limit_down_count             INTEGER,
    yiziban_count                INTEGER,
    max_consecutive_limit_ups    INTEGER,
    high_board_count             INTEGER,
    up_ratio_pct                 DOUBLE,
    limit_up_ratio_pct           DOUBLE,
    avg_pct_chg                  DOUBLE,
    median_pct_chg               DOUBLE,
    total_amount                 DOUBLE,
    high_60d_ratio_pct           DOUBLE,   -- 收盘创60日新高标的占比（市场温度）
    above_ma20_ratio_pct         DOUBLE,   -- 收盘在20日均线上方标的占比
    created_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# 已存在的表补温度列（CREATE IF NOT EXISTS 不会改旧表结构）
MARKET_SENTIMENT_HIGH60_MIGRATION_DDL = """
ALTER TABLE market_sentiment_daily ADD COLUMN IF NOT EXISTS high_60d_ratio_pct DOUBLE;
"""

MARKET_SENTIMENT_MA20_MIGRATION_DDL = """
ALTER TABLE market_sentiment_daily ADD COLUMN IF NOT EXISTS above_ma20_ratio_pct DOUBLE;
"""

SCREEN_RESULT_DDL = """
CREATE TABLE IF NOT EXISTS screen_result (
    trade_date    DATE    NOT NULL,
    preset_name   VARCHAR NOT NULL,
    ts_code       VARCHAR NOT NULL,
    name          VARCHAR,
    close         DOUBLE,
    pct_chg       DOUBLE,
    extra         JSON,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, preset_name, ts_code)
);
"""

POOL2_WATCH_DDL = """
CREATE TABLE IF NOT EXISTS pool2_watch (
    ts_code       VARCHAR   PRIMARY KEY,
    entry_date    DATE      NOT NULL,
    limit_up_date DATE      NOT NULL,
    body_upper    DOUBLE    NOT NULL,
    body_lower    DOUBLE    NOT NULL,
    level_40      DOUBLE    NOT NULL,
    level_30      DOUBLE    NOT NULL,
    level_20      DOUBLE    NOT NULL,
    stop_strong   DOUBLE    NOT NULL,
    stop_weak     DOUBLE    NOT NULL,
    status        VARCHAR   DEFAULT 'active',
    exit_date     DATE,
    exit_reason   VARCHAR,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

MONITOR_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS monitor_event (
    trade_date    DATE      NOT NULL,
    ts_code       VARCHAR   NOT NULL,
    level         VARCHAR   NOT NULL,
    trigger_price DOUBLE,
    level_price   DOUBLE,
    trigger_time  TIMESTAMP NOT NULL,
    trigger_type  VARCHAR,
    pool          VARCHAR,
    body_upper    DOUBLE,
    body_lower    DOUBLE,
    PRIMARY KEY (trade_date, ts_code, level)
);
"""

AUCTION_BAR_DDL = """
CREATE TABLE IF NOT EXISTS auction_bar (
    ts_code       VARCHAR NOT NULL,
    trade_date    DATE    NOT NULL,
    auction_type  VARCHAR NOT NULL,
    price         DOUBLE,
    vol           DOUBLE,
    amount        DOUBLE,
    turnover_rate DOUBLE,
    volume_ratio  DOUBLE,
    source        VARCHAR DEFAULT 'tushare',
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, auction_type, source)
);
"""

MINUTE_BAR_DDL = """
CREATE TABLE IF NOT EXISTS minute_bar (
    ts_code     VARCHAR   NOT NULL,
    trade_time  TIMESTAMP NOT NULL,
    freq        VARCHAR   NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    vol         DOUBLE,
    amount      DOUBLE,
    source      VARCHAR   DEFAULT 'tushare',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_time, freq, source)
);
"""

INTRADAY_FEATURE_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS intraday_feature_snapshot (
    snapshot_id   VARCHAR   PRIMARY KEY,
    ts_code       VARCHAR   NOT NULL,
    trade_date    DATE      NOT NULL,
    as_of_time    TIMESTAMP NOT NULL,
    feature_set   VARCHAR   NOT NULL,
    lookback_days INTEGER,
    payload       JSON,
    source        VARCHAR,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

PAPER_POSITION_DDL = """
CREATE TABLE IF NOT EXISTS paper_position (
    position_id          VARCHAR PRIMARY KEY,
    trade_date           DATE      NOT NULL,
    ts_code              VARCHAR   NOT NULL,
    name                 VARCHAR,
    pool                 VARCHAR,
    entry_time           TIMESTAMP NOT NULL,
    entry_price          DOUBLE    NOT NULL,
    entry_price_raw      DOUBLE,
    entry_signal         VARCHAR   NOT NULL,
    candidate_id         VARCHAR   NOT NULL,
    entry_level_price    DOUBLE,
    entry_t_date         DATE,
    earliest_exit_date   DATE      NOT NULL,
    t_close              DOUBLE,
    t_high               DOUBLE,
    limit_up_price_next  DOUBLE,
    stop_loss_price      DOUBLE    NOT NULL,
    stop_loss_basis      VARCHAR   NOT NULL,
    stop_loss_pct        DOUBLE    NOT NULL,
    take_profit_price    DOUBLE,
    take_profit_pct      DOUBLE,
    take_profit_basis    VARCHAR,
    trailing_stop_pct    DOUBLE,
    trailing_stop_price  DOUBLE,
    status               VARCHAR   NOT NULL,
    exit_time            TIMESTAMP,
    exit_price           DOUBLE,
    exit_reason          VARCHAR,
    holding_trading_days INTEGER,
    pnl_pct              DOUBLE,
    max_price_seen       DOUBLE,
    max_drawdown_pct     DOUBLE,
    feature_snapshot_id  VARCHAR,
    param_payload        JSON,
    strategy_name        VARCHAR,               -- 策略家族（GROUP BY 第一维）
    signal_factors       JSON,                  -- 入场判定因子命中矩阵（signal_provenance）
    run_mode             VARCHAR DEFAULT 'live', -- live | replay（回测仓同表共存）
    run_id               VARCHAR,               -- replay 批次 id，整批清理用
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

PAPER_POSITION_ENTRY_RAW_MIGRATION_DDL = """
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS entry_price_raw DOUBLE;
"""

PAPER_POSITION_TAKE_PROFIT_BASIS_MIGRATION_DDL = """
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS take_profit_basis VARCHAR;
"""

PAPER_POSITION_STRATEGY_NAME_MIGRATION_DDL = """
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS strategy_name VARCHAR;
"""

PAPER_POSITION_SIGNAL_FACTORS_MIGRATION_DDL = """
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS signal_factors JSON;
"""

PAPER_POSITION_RUN_MODE_MIGRATION_DDL = """
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS run_mode VARCHAR DEFAULT 'live';
"""

PAPER_POSITION_RUN_ID_MIGRATION_DDL = """
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS run_id VARCHAR;
"""

PAPER_POSITION_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS paper_position_event (
    event_id    VARCHAR   PRIMARY KEY,
    position_id VARCHAR   NOT NULL,
    event_time  TIMESTAMP NOT NULL,
    event_type  VARCHAR   NOT NULL,
    price       DOUBLE,
    size_pct    DOUBLE,
    payload     JSON,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# notification_log 表已弃用（v0.13.x），迁移到 logs/notification_log.jsonl 文件
# 5/22 真实事故：手动跑 push 在盘中（monitor 持写锁）写表撞 IOError 丢日志
# 云端旧表保留备查不强制 drop；新写入走 rquant.notify.log.append（JSONL append-only）

# 东财涨停池每日快照（akshare stock_zt_pool_em 只有当天有数据，历史返回空，
# 必须每日采集）。封单/成交额、封单/流通市值等派生比值在查询侧算，不落库。
LIMIT_UP_POOL_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS limit_up_pool_daily (
    ts_code            VARCHAR NOT NULL,
    trade_date         DATE    NOT NULL,
    name               VARCHAR,
    pct_chg            DOUBLE,
    close              DOUBLE,    -- 最新价
    amount             DOUBLE,    -- 成交额
    circ_mv            DOUBLE,    -- 流通市值
    total_mv           DOUBLE,
    turnover_rate      DOUBLE,
    seal_amount        DOUBLE,    -- 封板资金
    first_seal_time    VARCHAR,   -- 首次封板时间 '092500'
    last_seal_time     VARCHAR,
    break_count        INTEGER,   -- 炸板次数
    limit_up_stat      VARCHAR,   -- 涨停统计，如 '3/2'
    consecutive_boards INTEGER,   -- 连板数
    industry           VARCHAR,
    source             VARCHAR DEFAULT 'eastmoney',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, source)
);
"""

# Tushare 官方涨跌停/炸板榜（limit_list_d，历史从 2020 起，不含 ST），可历史
# 回补；与 limit_up_pool_daily（东财，只有当天数据、必须当日采集）互为交叉验证。
# 源字段 limit（'U'涨停/'D'跌停/'Z'炸板）是 SQL 关键字，落库改名 limit_status。
LIMIT_LIST_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS limit_list_daily (
    ts_code        VARCHAR NOT NULL,
    trade_date     DATE    NOT NULL,
    name           VARCHAR,
    industry       VARCHAR,
    close          DOUBLE,
    pct_chg        DOUBLE,
    amount         DOUBLE,    -- 成交额
    limit_amount   DOUBLE,    -- 板上成交金额（涨停无此值）
    float_mv       DOUBLE,    -- 流通市值
    total_mv       DOUBLE,
    turnover_ratio DOUBLE,
    fd_amount      DOUBLE,    -- 封单金额
    first_time     VARCHAR,   -- 首次封板时间 '103551'
    last_time      VARCHAR,
    open_times     INTEGER,   -- 开板次数
    up_stat        VARCHAR,   -- 涨停统计 'N/T'（N天/T次板）
    limit_times    INTEGER,   -- 连板数
    limit_status   VARCHAR NOT NULL,  -- U 涨停 / D 跌停 / Z 炸板（源字段名 limit）
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, limit_status)
);
"""

# ═══ 统一数据集回补层（dataset_backfill）表 ═══
# 字段全部按 2026-07-01 实测返回定义（见 adapter/tushare.py 各薄方法 docstring），
# 本地回补权威、云端没有 → 全部走 research_sync 的 MERGE 语义。

# 同花顺概念/行业指数日行情（ths_daily，实测 12 字段，无 amount）
THS_INDEX_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS ths_index_daily (
    ts_code        VARCHAR NOT NULL,
    trade_date     DATE    NOT NULL,
    open           DOUBLE,
    high           DOUBLE,
    low            DOUBLE,
    close          DOUBLE,
    pre_close      DOUBLE,
    avg_price      DOUBLE,
    change         DOUBLE,
    pct_change     DOUBLE,
    vol            DOUBLE,
    turnover_rate  DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

# 东财板块日行情（dc_daily，2020 起，实测 13 字段）
DC_INDEX_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS dc_index_daily (
    ts_code        VARCHAR NOT NULL,
    trade_date     DATE    NOT NULL,
    open           DOUBLE,
    high           DOUBLE,
    low            DOUBLE,
    close          DOUBLE,
    change         DOUBLE,
    pct_change     DOUBLE,
    vol            DOUBLE,
    amount         DOUBLE,
    swing          DOUBLE,    -- 振幅
    turnover_rate  DOUBLE,
    category       VARCHAR,   -- 概念板块 / 行业板块 / 地域板块
    PRIMARY KEY (ts_code, trade_date)
);
"""

# 同花顺板块列表快照（ths_index）。源字段 count/type 是 SQL 关键字，
# 落库改名 member_count/board_type
THS_BOARD_DDL = """
CREATE TABLE IF NOT EXISTS ths_board (
    ts_code       VARCHAR PRIMARY KEY,
    name          VARCHAR,
    member_count  INTEGER,
    exchange      VARCHAR,
    list_date     DATE,
    board_type    VARCHAR,   -- N概念 I行业 R地域 S特色 ST风格
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# 东财板块列表快照（dc_index，源按 trade_date 出数，快照留当日行情概览）。
# 源字段 leading 是 DuckDB 保留字（TRIM 语法），落库改名 leading_name
DC_BOARD_DDL = """
CREATE TABLE IF NOT EXISTS dc_board (
    ts_code        VARCHAR PRIMARY KEY,
    trade_date     DATE,
    name           VARCHAR,
    leading_name   VARCHAR,   -- 领涨股名称（源字段 leading）
    leading_code   VARCHAR,
    pct_change     DOUBLE,
    leading_pct    DOUBLE,
    total_mv       DOUBLE,
    turnover_rate  DOUBLE,
    up_num         INTEGER,
    down_num       INTEGER,
    idx_type       VARCHAR,   -- 概念板块 / 行业板块 / 地域板块
    level          VARCHAR,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# 板块-成分映射快照（源字段 ts_code 是板块代码，落库改名 board_code）
THS_BOARD_MEMBER_DDL = """
CREATE TABLE IF NOT EXISTS ths_board_member (
    board_code  VARCHAR NOT NULL,
    con_code    VARCHAR NOT NULL,
    con_name    VARCHAR,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (board_code, con_code)
);
"""

DC_BOARD_MEMBER_DDL = """
CREATE TABLE IF NOT EXISTS dc_board_member (
    board_code  VARCHAR NOT NULL,
    con_code    VARCHAR NOT NULL,
    con_name    VARCHAR,
    trade_date  DATE,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (board_code, con_code)
);
"""

# moneyflow（Tushare 个股口径）全 20 字段补列：原表只落大单/特大单 vol 与净额，
# dataset_backfill 回补小单/中单及全部 amount 字段
_MONEYFLOW_FULL_COLS = (
    "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
    "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
    "buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount",
)
MONEYFLOW_DAILY_FULL_MIGRATION_DDLS = [
    f"ALTER TABLE moneyflow_daily ADD COLUMN IF NOT EXISTS {col} DOUBLE;"
    for col in _MONEYFLOW_FULL_COLS
]

# 东财个股资金流（moneyflow_dc，2023-09 起，实测 15 字段，amount 单位万元）
MONEYFLOW_DC_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS moneyflow_dc_daily (
    ts_code              VARCHAR NOT NULL,
    trade_date           DATE    NOT NULL,
    name                 VARCHAR,
    pct_change           DOUBLE,
    close                DOUBLE,
    net_amount           DOUBLE,
    net_amount_rate      DOUBLE,
    buy_elg_amount       DOUBLE,
    buy_elg_amount_rate  DOUBLE,
    buy_lg_amount        DOUBLE,
    buy_lg_amount_rate   DOUBLE,
    buy_md_amount        DOUBLE,
    buy_md_amount_rate   DOUBLE,
    buy_sm_amount        DOUBLE,
    buy_sm_amount_rate   DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

# 同花顺个股资金流（moneyflow_ths，实测 13 字段）
MONEYFLOW_THS_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS moneyflow_ths_daily (
    ts_code             VARCHAR NOT NULL,
    trade_date          DATE    NOT NULL,
    name                VARCHAR,
    pct_change          DOUBLE,
    latest              DOUBLE,    -- 最新价
    net_amount          DOUBLE,
    net_d5_amount       DOUBLE,    -- 5日主力净额
    buy_lg_amount       DOUBLE,
    buy_lg_amount_rate  DOUBLE,
    buy_md_amount       DOUBLE,
    buy_md_amount_rate  DOUBLE,
    buy_sm_amount       DOUBLE,
    buy_sm_amount_rate  DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

# 同花顺行业资金流（moneyflow_ind_ths，实测 12 字段）
MONEYFLOW_IND_THS_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS moneyflow_ind_ths_daily (
    ts_code           VARCHAR NOT NULL,
    trade_date        DATE    NOT NULL,
    industry          VARCHAR,
    lead_stock        VARCHAR,
    close             DOUBLE,
    pct_change        DOUBLE,
    company_num       INTEGER,
    pct_change_stock  DOUBLE,    -- 领涨股涨跌幅
    close_price       DOUBLE,    -- 领涨股最新价
    net_buy_amount    DOUBLE,
    net_sell_amount   DOUBLE,
    net_amount        DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

# 东财板块/概念资金流（moneyflow_ind_dc，实测 18 字段）。
# 源字段 rank 是 SQL 关键字，落库改名 net_amount_rank
MONEYFLOW_IND_DC_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS moneyflow_ind_dc_daily (
    ts_code              VARCHAR NOT NULL,
    trade_date           DATE    NOT NULL,
    content_type         VARCHAR,   -- 行业 / 概念 / 地域
    name                 VARCHAR,
    pct_change           DOUBLE,
    close                DOUBLE,
    net_amount           DOUBLE,
    net_amount_rate      DOUBLE,
    buy_elg_amount       DOUBLE,
    buy_elg_amount_rate  DOUBLE,
    buy_lg_amount        DOUBLE,
    buy_lg_amount_rate   DOUBLE,
    buy_md_amount        DOUBLE,
    buy_md_amount_rate   DOUBLE,
    buy_sm_amount        DOUBLE,
    buy_sm_amount_rate   DOUBLE,
    buy_sm_amount_stock  VARCHAR,   -- 主力净流入最大股
    net_amount_rank      INTEGER,
    PRIMARY KEY (ts_code, trade_date)
);
"""

# 同花顺概念资金流（moneyflow_cnt_ths，实测 12 字段）
MONEYFLOW_CNT_THS_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS moneyflow_cnt_ths_daily (
    ts_code           VARCHAR NOT NULL,
    trade_date        DATE    NOT NULL,
    name              VARCHAR,
    lead_stock        VARCHAR,
    close_price       DOUBLE,
    pct_change        DOUBLE,
    industry_index    DOUBLE,
    company_num       INTEGER,
    pct_change_stock  DOUBLE,
    net_buy_amount    DOUBLE,
    net_sell_amount   DOUBLE,
    net_amount        DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

# 东财大盘资金流（moneyflow_mkt_dc，1 行/日，实测 15 字段）
MONEYFLOW_MKT_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS moneyflow_mkt_daily (
    trade_date           DATE PRIMARY KEY,
    close_sh             DOUBLE,
    pct_change_sh        DOUBLE,
    close_sz             DOUBLE,
    pct_change_sz        DOUBLE,
    net_amount           DOUBLE,
    net_amount_rate      DOUBLE,
    buy_elg_amount       DOUBLE,
    buy_elg_amount_rate  DOUBLE,
    buy_lg_amount        DOUBLE,
    buy_lg_amount_rate   DOUBLE,
    buy_md_amount        DOUBLE,
    buy_md_amount_rate   DOUBLE,
    buy_sm_amount        DOUBLE,
    buy_sm_amount_rate   DOUBLE
);
"""

# 龙虎榜每日明细（top_list，2005 起，实测 15 字段）。
# 同票同日可因多个上榜理由重复出现 → reason 进主键。
# 源数据偶发完全相同的重复行（2026-07-01 实测 6 行，字段全同），
# upsert 批内去重无损
TOP_LIST_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS top_list_daily (
    trade_date     DATE    NOT NULL,
    ts_code        VARCHAR NOT NULL,
    name           VARCHAR,
    close          DOUBLE,
    pct_change     DOUBLE,
    turnover_rate  DOUBLE,
    amount         DOUBLE,
    l_sell         DOUBLE,    -- 龙虎榜卖出额
    l_buy          DOUBLE,    -- 龙虎榜买入额
    l_amount       DOUBLE,    -- 龙虎榜成交额
    net_amount     DOUBLE,
    net_rate       DOUBLE,
    amount_rate    DOUBLE,
    float_values   DOUBLE,    -- 流通市值
    reason         VARCHAR NOT NULL,
    PRIMARY KEY (trade_date, ts_code, reason)
);
"""

# 龙虎榜机构席位明细（top_inst，实测 10 字段）。
# 一日一票多席位，同席位可同时上买入/卖出榜 → exalter/side/reason 进主键
TOP_INST_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS top_inst_daily (
    trade_date  DATE    NOT NULL,
    ts_code     VARCHAR NOT NULL,
    exalter     VARCHAR NOT NULL,   -- 营业部名称
    side        VARCHAR NOT NULL,   -- 0 买入榜 / 1 卖出榜
    reason      VARCHAR NOT NULL,
    buy         DOUBLE,
    buy_rate    DOUBLE,
    sell        DOUBLE,
    sell_rate   DOUBLE,
    net_buy     DOUBLE,
    PRIMARY KEY (trade_date, ts_code, exalter, side, reason)
);
"""

# 开盘啦榜单（kpl_list，实测 24 字段）。tag（涨停/炸板/跌停/自然涨停/竞价）进主键
KPL_LIST_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS kpl_list_daily (
    ts_code         VARCHAR NOT NULL,
    trade_date      DATE    NOT NULL,
    tag             VARCHAR NOT NULL,
    name            VARCHAR,
    lu_time         VARCHAR,   -- 涨停时间 'HH:MM:SS'
    ld_time         VARCHAR,
    open_time       VARCHAR,   -- 开板时间
    last_time       VARCHAR,
    lu_desc         VARCHAR,   -- 涨停原因
    theme           VARCHAR,
    net_change      DOUBLE,    -- 主力净额
    bid_amount      DOUBLE,    -- 竞价成交额
    status          VARCHAR,   -- 首板 / N连板
    bid_change      DOUBLE,
    bid_turnover    DOUBLE,
    lu_bid_vol      DOUBLE,
    pct_chg         DOUBLE,
    bid_pct_chg     DOUBLE,
    rt_pct_chg      DOUBLE,
    limit_order     DOUBLE,    -- 封单量
    amount          DOUBLE,
    turnover_rate   DOUBLE,
    free_float      DOUBLE,
    lu_limit_order  DOUBLE,    -- 最大封单
    PRIMARY KEY (ts_code, trade_date, tag)
);
"""

# 开盘啦题材成分快照（kpl_concept_cons）。源按「题材当日活跃」增量打点
# （每天只写 3-8 个题材的全量成分，约 250-660 行/日），trade_date 是该题材
# 最近一次打点日。源字段 ts_code 是题材代码（如 000129.KP）→ board_code，
# desc 是 SQL 关键字 → description（沿用 hm_list 惯例）
KPL_CONCEPT_MEMBER_DDL = """
CREATE TABLE IF NOT EXISTS kpl_concept_member (
    board_code   VARCHAR NOT NULL,   -- 题材代码（源字段 ts_code，如 000129.KP）
    board_name   VARCHAR,
    con_code     VARCHAR NOT NULL,
    con_name     VARCHAR,
    trade_date   DATE,               -- 该题材成分最近一次打点日
    description  VARCHAR,            -- 上榜理由（源字段 desc）
    hot_num      BIGINT,             -- 个股热度
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (board_code, con_code)
);
"""

# 开盘啦题材成分「日度」表（kpl_concept_daily）。与 kpl_concept_member 快照表
# 的区别：快照表 PK (board_code, con_code) 只留每题材最近一次打点（当前成分），
# 回测取不到「信号日 T 的历史成分」；本表 PK 含 trade_date，存每天的成分打点
# （不折叠），供 board_auction_strength 按 ≤T 最近打点还原当日题材成分。
# 源字段 ts_code 是题材代码 → board_code、name → board_name（同快照表惯例）。
KPL_CONCEPT_MEMBER_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS kpl_concept_member_daily (
    trade_date   DATE    NOT NULL,   -- 该题材成分的打点日
    board_code   VARCHAR NOT NULL,   -- 题材代码（源字段 ts_code，如 000129.KP）
    board_name   VARCHAR,
    con_code     VARCHAR NOT NULL,
    con_name     VARCHAR,
    hot_num      BIGINT,             -- 个股热度
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, board_code, con_code)
);
"""

# 市场交易统计（daily_info，1 行/市场分段/日，实测 14 字段）
MARKET_DAILY_INFO_DDL = """
CREATE TABLE IF NOT EXISTS market_daily_info (
    trade_date   DATE    NOT NULL,
    ts_code      VARCHAR NOT NULL,   -- 市场分段代码 SH_A / SZ_A / SH_STAR ...
    ts_name      VARCHAR,
    com_count    INTEGER,
    total_share  DOUBLE,
    float_share  DOUBLE,
    total_mv     DOUBLE,
    float_mv     DOUBLE,
    amount       DOUBLE,
    vol          DOUBLE,
    trans_count  DOUBLE,
    pe           DOUBLE,
    tr           DOUBLE,
    exchange     VARCHAR,
    PRIMARY KEY (trade_date, ts_code)
);
"""

# 游资名录（hm_list，静态快照，实测 3 字段）。源字段 desc 是 SQL 关键字，改名 description
HM_LIST_DDL = """
CREATE TABLE IF NOT EXISTS hm_list (
    name         VARCHAR PRIMARY KEY,
    description  VARCHAR,
    orgs         VARCHAR,   -- 关联机构 JSON 字符串
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

RISK_BLACKLIST_DDL = """
CREATE TABLE IF NOT EXISTS risk_blacklist (
    list_label      VARCHAR   NOT NULL,   -- 如 "430黑名单"
    ts_code         VARCHAR   NOT NULL,
    name            VARCHAR,
    sub_categories  VARCHAR[],            -- 多类别合并（净资产为负 / 营收利润不达标 ...）
    risk_type       VARCHAR,              -- ST预警
    source_file     VARCHAR,
    imported_at     DATE      NOT NULL,
    expires_at      DATE      NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (list_label, ts_code)
);
"""

DATASET_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS dataset_snapshot (
    snapshot_id       VARCHAR     PRIMARY KEY,
    strategy_name     VARCHAR     NOT NULL,
    manifest_id       VARCHAR,
    as_of_time        TIMESTAMPTZ NOT NULL,
    code_commit       VARCHAR     NOT NULL,
    origin            VARCHAR     NOT NULL,
    status            VARCHAR     NOT NULL CHECK (status IN ('building', 'ready')),
    table_watermarks  JSON        NOT NULL,
    quality_issue_ids JSON        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL,
    completed_at      TIMESTAMPTZ
);
"""

DATASET_COVERAGE_DDL = """
CREATE TABLE IF NOT EXISTS dataset_coverage (
    snapshot_id     VARCHAR     NOT NULL,
    dataset_id      VARCHAR     NOT NULL,
    coverage_scope  VARCHAR     NOT NULL,
    table_name      VARCHAR     NOT NULL,
    expected_count  BIGINT      NOT NULL CHECK (expected_count >= 0),
    available_count BIGINT      NOT NULL CHECK (available_count >= 0),
    missing_count   BIGINT      NOT NULL CHECK (missing_count >= 0),
    coverage_ratio  DOUBLE,
    missing_reasons JSON        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snapshot_id, dataset_id, coverage_scope),
    CHECK (available_count + missing_count = expected_count),
    CHECK (available_count <= expected_count),
    CHECK (
        (expected_count = 0 AND coverage_ratio IS NULL)
        OR (
            expected_count > 0
            AND coverage_ratio IS NOT NULL
            AND abs(coverage_ratio - available_count::DOUBLE / expected_count) <= 1e-9
        )
    )
);
"""

DATA_QUALITY_ISSUE_DDL = """
CREATE TABLE IF NOT EXISTS data_quality_issue (
    issue_id       VARCHAR     PRIMARY KEY,
    rule_id        VARCHAR     NOT NULL,
    dataset_id     VARCHAR     NOT NULL,
    severity       VARCHAR     NOT NULL CHECK (severity IN ('P0', 'P1', 'P2', 'P3')),
    status         VARCHAR     NOT NULL CHECK (status IN ('open', 'resolved')),
    scope_key      VARCHAR     NOT NULL,
    message        VARCHAR     NOT NULL,
    evidence       JSON        NOT NULL,
    first_seen_at  TIMESTAMPTZ NOT NULL,
    last_seen_at   TIMESTAMPTZ NOT NULL,
    resolved_at    TIMESTAMPTZ
);
"""

TRADE_CALENDAR_DDL = """
CREATE TABLE IF NOT EXISTS trade_calendar (
    exchange      VARCHAR     NOT NULL,
    cal_date      DATE        NOT NULL,
    is_open       BOOLEAN     NOT NULL,
    pretrade_date DATE,
    source        VARCHAR     NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, cal_date)
);
"""

DATA_METADATA_TABLE_DDLS: tuple[str, ...] = (
    DATASET_SNAPSHOT_DDL,
    DATASET_COVERAGE_DDL,
    DATA_QUALITY_ISSUE_DDL,
)

BASE_DDL = [
    DAILY_BAR_DDL, INDEX_DAILY_BAR_DDL, STOCK_BASIC_DDL, ADJ_FACTOR_DDL,
    DAILY_INDICATOR_DDL, DAILY_STATE_DDL, DAILY_BASIC_DDL,
    MONEYFLOW_DAILY_DDL, MARKET_SENTIMENT_DAILY_DDL,
    SCREEN_RESULT_DDL, POOL2_WATCH_DDL, MONITOR_EVENT_DDL,
    AUCTION_BAR_DDL, MINUTE_BAR_DDL, INTRADAY_FEATURE_SNAPSHOT_DDL,
    PAPER_POSITION_DDL, PAPER_POSITION_EVENT_DDL,
    LIMIT_UP_POOL_DAILY_DDL, LIMIT_LIST_DAILY_DDL, RISK_BLACKLIST_DDL,
    # 统一数据集回补层（dataset_backfill）
    THS_INDEX_DAILY_DDL, DC_INDEX_DAILY_DDL,
    THS_BOARD_DDL, DC_BOARD_DDL, THS_BOARD_MEMBER_DDL, DC_BOARD_MEMBER_DDL,
    MONEYFLOW_DC_DAILY_DDL, MONEYFLOW_THS_DAILY_DDL,
    MONEYFLOW_IND_THS_DAILY_DDL, MONEYFLOW_IND_DC_DAILY_DDL,
    MONEYFLOW_CNT_THS_DAILY_DDL, MONEYFLOW_MKT_DAILY_DDL,
    TOP_LIST_DAILY_DDL, TOP_INST_DAILY_DDL,
    KPL_LIST_DAILY_DDL, KPL_CONCEPT_MEMBER_DDL, KPL_CONCEPT_MEMBER_DAILY_DDL,
    MARKET_DAILY_INFO_DDL, HM_LIST_DDL,
]

LEGACY_MIGRATION_DDL = [
    MARKET_SENTIMENT_HIGH60_MIGRATION_DDL,
    MARKET_SENTIMENT_MA20_MIGRATION_DDL,
    PAPER_POSITION_ENTRY_RAW_MIGRATION_DDL,
    PAPER_POSITION_TAKE_PROFIT_BASIS_MIGRATION_DDL,
    PAPER_POSITION_STRATEGY_NAME_MIGRATION_DDL,
    PAPER_POSITION_SIGNAL_FACTORS_MIGRATION_DDL,
    PAPER_POSITION_RUN_MODE_MIGRATION_DDL,
    PAPER_POSITION_RUN_ID_MIGRATION_DDL,
    *MONEYFLOW_DAILY_FULL_MIGRATION_DDLS,
]

VERSIONED_COMPATIBILITY_DDL = [
    *LEGACY_MIGRATION_DDL,
    *DATA_METADATA_TABLE_DDLS,
    TRADE_CALENDAR_DDL,
]

# Compatibility export for callers outside rQuant; schema initialization uses
# BASE_DDL plus the versioned registry in storage.migrations.
ALL_DDL = [*BASE_DDL, *VERSIONED_COMPATIBILITY_DDL]
