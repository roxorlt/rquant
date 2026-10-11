# 研究数据管线（历史数据，不依赖盘中 runtime）

- 根目录：`/home/lighthouse/rquant-research`（`RQUANT_RESEARCH_ROOT`，写在 `/etc/rquant-web.env`），在 web 发布目录和 runtime 之外。
- `market.duckdb`：trade_calendar、stock_basic、daily_bar（原始）、adj_factor、daily_basic、daily_state、index_daily、minute_bar（5 分钟，市值前 300、近 1 年）、view `daily_bar_hfq`（后复权）、screen_result（research_* 预设）。
- 结果文件：factor/、portfolio_backtest/、strategy/、data_audit/、fundamentals/。
- 任务：`python -m rquant.research daily`（增量拉数 → 预设选股 → 5 个策略回测 → 6 个标准因子检验 + 跟踪 → 覆盖审计）。
- 定时：`rquant-research.timer`（工作日 18:10），unit 文件在 `deploy/research/`，User=lighthouse，只读 web 的 env 与 `.tushare.env`（仅 token，600）。
- 回滚：`sudo systemctl disable --now rquant-research.timer && sudo rm /etc/systemd/system/rquant-research.{service,timer} && sudo systemctl daemon-reload`；从 `/etc/rquant-web.env` 删 `RQUANT_RESEARCH_ROOT` 行并重启 rquant-web。数据目录可整体删除。
