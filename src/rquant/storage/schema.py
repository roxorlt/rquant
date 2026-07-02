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

ALL_DDL = [
    DAILY_BAR_DDL, INDEX_DAILY_BAR_DDL, STOCK_BASIC_DDL, ADJ_FACTOR_DDL,
    DAILY_INDICATOR_DDL, DAILY_STATE_DDL, DAILY_BASIC_DDL,
    MONEYFLOW_DAILY_DDL, MARKET_SENTIMENT_DAILY_DDL,
    MARKET_SENTIMENT_HIGH60_MIGRATION_DDL, MARKET_SENTIMENT_MA20_MIGRATION_DDL,
    SCREEN_RESULT_DDL, POOL2_WATCH_DDL, MONITOR_EVENT_DDL,
    AUCTION_BAR_DDL, MINUTE_BAR_DDL, INTRADAY_FEATURE_SNAPSHOT_DDL,
    PAPER_POSITION_DDL, PAPER_POSITION_ENTRY_RAW_MIGRATION_DDL,
    PAPER_POSITION_TAKE_PROFIT_BASIS_MIGRATION_DDL, PAPER_POSITION_EVENT_DDL,
    LIMIT_UP_POOL_DAILY_DDL, LIMIT_LIST_DAILY_DDL, RISK_BLACKLIST_DDL,
]
