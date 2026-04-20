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

ALL_DDL = [
    DAILY_BAR_DDL, STOCK_BASIC_DDL, ADJ_FACTOR_DDL,
    DAILY_INDICATOR_DDL, DAILY_STATE_DDL, DAILY_BASIC_DDL,
]
