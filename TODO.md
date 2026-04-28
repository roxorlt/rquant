# rQuant TODO

## 待解决问题

### APScheduler 常驻进程在 macOS 休眠后失效

**背景**：
- `rquant serve` 使用 APScheduler `BlockingScheduler` 常驻运行，cron 每日 17:00 触发 ingest + pipeline
- APScheduler 内部用 `threading.Event.wait(timeout)` 做定时唤醒
- macOS 笔记本长时间合盖休眠（如周末 2-3 天），唤醒后进程仍在（`ps` 可见），但 scheduler 的调度线程已死，不再触发任何 job，也不输出 misfire 警告
- 已将 `misfire_grace_time` 从 1s → 3600s → 7200s，仍无法解决线程死亡问题（grace time 只解决"短暂 miss"，不解决"scheduler 彻底停摆"）

**已观察到的故障**：
- 4/21（周一）：cron miss（旧 1s grace time）
- 4/24（周四）：cron miss by 1h2m（3600s grace time 刚好不够）
- 4/27-28（周一-周二）：进程在但 scheduler 线程死亡，4/24 之后零输出

**临时解决方案**：
- 发现 miss 后手动 `rquant run-daily --date YYYY-MM-DD` 补跑
- 手动 kill 进程 + `launchctl bootout/bootstrap` 重启服务

**长期解决方案选项**：

1. **launchd StartCalendarInterval 替代常驻进程**
   - 去掉 `rquant serve`，改为 launchd 每天 17:00 直接拉起 `rquant run-daily`
   - 优点：launchd 是 macOS 原生调度，有自己的唤醒和补执行机制，不依赖 Python 线程
   - 缺点：仍依赖本地笔记本开机/唤醒

2. **serve 进程加 watchdog**
   - 另起一个 launchd job 定期（如每小时）检查 scheduler 是否活着，死了自动重启
   - 缺点：治标不治本，增加复杂度

3. **crontab 替代 APScheduler**
   - 最简单粗暴，macOS 原生支持
   - 缺点：同方案 1，仍依赖本地

4. **迁移到云端（Cloudflare Workers / Cron Triggers）**
   - 彻底不依赖本地笔记本在线
   - CF Workers Cron Triggers 可以每天定时触发，稳定性由 CF 保证
   - 需要评估：DuckDB 数据访问方式（本地文件 vs 远程存储）、Tushare API 调用、数据回写
   - 可能需要拆分：云端负责调度+触发，本地负责数据计算（或全量迁移到云端）

5. **迁移到 VPS / 家庭服务器**
   - 24/7 运行，不受笔记本休眠影响
   - 现有架构（APScheduler + DuckDB）可直接迁移
   - 缺点：多一台机器要维护

**决策方向**：迁移到腾讯云轻量服务器（方案 5），彻底解决。详见下方部署计划。

---

### 腾讯云轻量服务器部署计划

**服务器信息**：
- IP: 82.156.4.48
- 系统: OpenCloudOS 9 (RHEL 系), kernel 6.6.34
- 配置: 2 核 / 7.5GB RAM / 120GB SSD (84GB 可用)
- Python: 3.11.6 ✅
- 带宽: 5Mbps

**资源预算**（当前 + 未来 GUI 全量部署）：
| 组件 | 内存 | 硬盘 |
|------|------|------|
| DuckDB (rquant pipeline) | ~200MB | ~500MB（年增长慢） |
| FastAPI 后端（未来） | ~100MB | 代码 <50MB |
| 前端静态文件 nginx（未来） | ~50MB | build <20MB |
| 盘中 monitor（未来） | ~100MB | — |
| **合计** | **~450MB** | **<1GB** |
| **服务器余量** | 5.5GB 可用 | 84GB 可用 |

结论：2 核 / 7.5GB 跑 rQuant 全部组件（调度 + 计算 + API + 前端 + 监控 + 通知）绰绰有余，几个人用 5Mbps 带宽也完全够。

**部署步骤**（待执行）：
1. 服务器装 uv + git clone rQuant
2. 配置 `.env`（Tushare token 等）
3. 用 systemd timer 替代 APScheduler 做每日调度
   - `rquant run-daily` 每天 17:05 定时执行
   - systemd 是系统级调度，不依赖 Python 线程，不怕进程死
4. 盘中 monitor 用 systemd service（交易日 09:25-15:05）
5. PushDeer 通知直接从服务器推
6. 后续 GUI 阶段：FastAPI 后端 + nginx 反向代理前端静态文件

**迁移后本地笔记本的角色**：
- 开发用，不再承担生产调度
- 可保留一份 DuckDB 副本用于本地分析/调试

---

## MVP 路径

### P0 收尾
- [ ] v0.5.x tag + CHANGELOG Unreleased 归档
- [ ] monitor plist 缺 `StartCalendarInterval`（只 boot 启动，不会每天 09:25 自动拉起）

### 待做
- [ ] **Week 6：PushDeer 告警通知**（替换原计划的 cc2im——cc2im 受限于微信 token 限制；参考 `30-projects/xueqiuFollow/src/notifier.py`：`POST https://api2.pushdeer.com/message/push` + `pushkey/text/desp/type=markdown`，code=0 即成功。现阶段只推 admin（刘彤）的 iPhone + Mac，key 在 `.env`（已从 xueqiuFollow/config.yaml admin 组复制，不进 git）。后续如增加订阅者再扩 group 概念）
- [ ] **Week 7：Streamlit UI + 自然语言输入**
- [ ] **Week 8：通达信选股公式支持**
- [ ] **部署到腾讯云服务器**（可在 Week 6 之后，与 GUI 并行推进）

### 后续
- [ ] Pool 1/Pool 2 阈值持续观察调优
- [ ] 盘中分时明细（akshare 1min bars）
- [ ] tick 数据存储（watchlist 50 只，约 1.5-2GB/年）
- [ ] 前后端分离 GUI 产品化（rule registry + FastAPI + 前端）

### Monitor 弹窗 UX 改进（2026-04-28 复盘发现）

当前 `check_exits` 逐个标的弹原生 macOS dialog 让用户「保留/踢出」，存在三个问题：

1. **可视化不足**：弹窗只显示数字（body、档位、止损价），需要按日 K 线图样式画出来——body 区间 + 档位线 + 当前位置一眼看清，比纯数字直观
2. **进度焦虑**：弹连续多个弹窗时不知道还剩几个，没有「N/M」或进度条，每弹一个都担心是不是没头
3. **云端 GUI 友好**：未来部署到腾讯云后，逐个弹原生 dialog 不可行。需要改为「批量列出所有待决策项 → 用户在网页/移动端一次性勾选 → 提交批量决定」的模式（适配 Streamlit/FastAPI + 前端）

实施时机：在 Week 7 Streamlit UI 阶段一起做（UI 框架建好后弹窗逻辑顺势改成网页交互）。
