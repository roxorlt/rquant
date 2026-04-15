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

ALL_DDL = [DAILY_BAR_DDL, STOCK_BASIC_DDL, ADJ_FACTOR_DDL, DAILY_INDICATOR_DDL]
