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

NOTIFICATION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS notification_log (
    sent_at    TIMESTAMP NOT NULL,
    scene      VARCHAR   NOT NULL,
    channel    VARCHAR   NOT NULL,   -- pushdeer | pushplus
    target     VARCHAR,                -- key/token 前 8 位
    success    BOOLEAN   NOT NULL,
    error_msg  VARCHAR,
    title      VARCHAR
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
    DAILY_BAR_DDL, STOCK_BASIC_DDL, ADJ_FACTOR_DDL,
    DAILY_INDICATOR_DDL, DAILY_STATE_DDL, DAILY_BASIC_DDL,
    SCREEN_RESULT_DDL, POOL2_WATCH_DDL, MONITOR_EVENT_DDL,
    NOTIFICATION_LOG_DDL, RISK_BLACKLIST_DDL,
]
