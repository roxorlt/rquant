# rQuant 项目指令

## 项目定位

rQuant 是一个**个人自用**的 A 股量化选股与盯盘平台：
- 只做「条件筛选 + 实时监控 + 告警通知」
- **明确暂时不做**：实盘下单、高频策略、Tick 级微观结构、Level2

## 开发环境

- **平台**：macOS（MacBook），**不要依赖只支持 Windows 的工具**（如 QMT 客户端、掘金客户端）
- **Python**：3.11+
- **包管理**：uv 优先（未定，下次决策）
- **操作系统相关注意**：
  - 避免装 TA-Lib（C 扩展 Mac 上麻烦），用 pandas-ta 替代
  - 通达信协议用 mootdx（pytdx 活跃 fork），不用老 pytdx

## 技术栈约束

不要随意扩张，以下是已决定的栈：

| 层 | 已选 | 不要选 |
|---|---|---|
| 数据 | Tushare Pro + AKShare + Ashare + mootdx | Wind/iFinD/Choice API（太贵）、miniQMT（需 Windows） |
| 存储 | DuckDB（主）+ Parquet + SQLite（状态） | PostgreSQL（杀鸡用牛刀）、InfluxDB（不需要） |
| 指标 | pandas-ta | TA-Lib（Mac 装麻烦） |
| 调度 | APScheduler | Celery / Airflow（个人项目用不上） |
| 日志 | loguru | 标准 logging（手动配置烦） |
| UI | Streamlit | React/Vue 从零写（先别开分支） |
| 通知 | PushDeer（参考 30-projects/xueqiuFollow/src/notifier.py），现阶段只推 admin（刘彤） | cc2im（受限于微信 token 限制）、企业微信 webhook、新搭通知系统 |

## 代码风格

- **类型注解必写**：函数签名全部 type hint
- **Pydantic 模型**：所有跨层数据结构用 Pydantic，不要裸 dict 传递
- **不写过度抽象**：MVP 阶段是函数式脚本 + 简单 class，不要设计"框架"
- **不写注释写啥**：只在有非显式约束/坑时写注释

## DuckDB 并发约束（强制）

DuckDB 是**单文件锁**：持写锁的进程在期间会**拒绝所有新连接（包括 `read_only=True`）**。
原 v0.12.x 笔记里写的「单写多读 → read_only 可跟 writer 共存」是误读；DuckDB 实际行为
是「写者持锁期间，任何新 open 都失败」（参考官方 docs/stable/connect/concurrency）。

**`rquant-monitor` 是常驻写入者**（盘中 9:25–15:00 持续写 `monitor_event` /
`notification_log`），期间 dashboard / canvas / nl-screen 任何直连主库的 read_only
连接都会撞 `IOError: Could not set lock on file ...`（5/20 真实事故）。

### 正确写法：只读消费者读副本，写消费者写主库

```python
# 只读消费者（dashboard / canvas / nl-screen / 临时 CLI 查询）
from rquant.storage.duckdb import open_readonly_store, open_readonly_connection

with open_readonly_store() as store:        # 优先副本，副本不可用降级主库
    df = store.query_screen_result(...)

# 裸 connect 版（dashboard/app.py 用）
conn = open_readonly_connection()
```

副本由 `rquant-replica-sync.timer` 每 5min 跑 `scripts/sync-readonly-replica.sh` 维护
（cp 主库 + WAL → verify → atomic mv 替换 `rquant_ro.duckdb`），延迟最多 5min。

### 错误写法（会跟 monitor 抢锁）

```python
store = DuckDBStore()                                       # 默认写模式
conn = duckdb.connect(str(path))                            # 默认写模式
store = DuckDBStore(settings.duckdb_path, read_only=True)   # 直连主库，盘中撞写锁
```

### 写者串行调度

明确需要写的服务（`rquant-monitor` / `rquant-daily` / `rquant-backup` / `rquant-replica-sync`
本身只读不写主库，但同一时刻只能一个写者）按 systemd timer 约定串行，watchdog 和 timer
错开。新增 Streamlit / FastAPI / 临时脚本时，code review 必查这一条。

## MVP 路径（必须按顺序）

不要并行推进多个阶段。按周迭代：

1. Week 1：数据接入 + DuckDB 存储 → **能跑再下一步**
2. Week 2：指标计算
3. Week 3a：派生字段层（daily_state）
4. Week 3b：筛选规则（Python 函数积木，命名对齐通达信/MyTT 风格）
5. Week 4：调度（APScheduler）+ 筛选结果落库
6. Week 5：盘中监控（Ashare 轮询）
7. Week 6：通知（PushDeer）
8. Week 7：Streamlit UI + 自然语言输入（LLM → 积木调用）
9. Week 8：通达信选股公式支持（解析器 → MyTT/积木）

## 验证规范

- **改动可运行代码必须实际运行验证**，不只凭代码逻辑判断
- **让用户试用时必须明确说明**：运行命令、Python 环境、版本号
- **输出本地路径用完整绝对路径**，不用 `~` 或相对路径
- **人工核验数据按时间倒序取**：从最新日期往旧找第一个满足测试条件的样本，找不到再往前推，并说明推了多少天
- **systemd unit 改动（`deploy/systemd/*.{service,timer}`）必须在 push 或合并前完成 cloud 端验证**：mac 没装 systemd，OnCalendar 等语法本地测不出。Codex 必须在取得用户单独明确的高风险授权后自行 SSH 到 cloud 端运行 `systemd-analyze calendar '<spec>' --iterations 5`，确认 parse 通过且 Iteration 间隔符合预期（如步进 2min 不是 2sec），并向用户报告结果；验证通过前不得 push 或合并该 systemd 改动。已知坑：
  - `HH:MM..HH:MM/N` 中 `/N` 是步进**秒**，不是分钟
  - 跨小时分钟范围 `09:30..11:30/2` 整段被 `Invalid argument` 拒收
  - minute 字段 `*/N` 通配步进**不接受**，但 `0/N` 显式起点接受
  - 已知能 work 的 2min 步进语法：`OnCalendar=Mon..Fri *-*-* 9..14:0/2`

## 版本控制与部署

### 基本原则

- **main 永远可运行可部署**：任何时刻 checkout main 都能跑起来
- **简化版 GitHub Flow**：只有 main + feature 分支，不搞 develop/release
- 当前受管 origin 是 private GitHub，方便服务器 `git pull`；未来 Gitea remote 必须先具备
  `docs/engineering/task-lifecycle.md` 定义的等价不可变 review/merge evidence adapter，才可受管合并
  与回收；否则任务必须阻塞或隔离，不得以 ancestry 猜测替代证据

### 受管任务生命周期（强制）

用户只参与需求、方案取舍、用户可见行为的业务验收、高风险变更的明确授权，以及无法恢复的业务工作是否放弃的决定；不需要操作 Git、worktree、测试、PR、CI、合并、tag、部署、回滚、验证或清理。

Codex 必须负责从隔离开发到交付关闭的完整生命周期。所有改变仓库内容的新任务均须遵守
[`docs/engineering/task-lifecycle.md`](docs/engineering/task-lifecycle.md)；该文件是任务状态、租约、
PR 证据、发布分类、回收门禁、交付回执与存量保护的唯一详细规范。本文件或其他流程规则与其
冲突时，以该文件为准。

每个改变仓库内容的任务都必须有 PR（可为 draft）及 lifecycle evidence。开始时先执行新鲜的
`git fetch --prune`，从新鲜的 `origin/main` 创建隔离分支和 worktree；本地 `main` 只用于受保护的
集成、发布或只读检查，dirty、diverged 或归属不明时，所有合并、tag、部署与回收必须阻塞，直到有
clean/reconciled 证据；本策略不授权 destructive reconciliation，必须由独立的可审计 remediation task
保留并归属提交和本地文件后处理；禁止盲目 `pull`、`reset`、`clean`、覆盖或删除。
改变用户可见行为的任务须在用户业务验收后才能就绪或合并；纯技术、文档与流程任务通过技术门禁后
可自动推进。任务只有安全回收完成且 PR 终态回执发布后才算关闭；dirty、未知、证据不完整或 PR 后
新增工作的任务必须隔离（`quarantined`）并保留。合并、tag、部署与回收必须由单一 owner 持集成锁串行执行。

本轮及本策略合并前已存在的分支与 worktree 均为 legacy：不得因本策略删除、重命名、移动、
重置或迁移；后续仅可经独立审计并满足生命周期规范的证据门禁后处理。

### 分支命名规范

```
cdx/YYYYMMDD-<kind>-<topic>  # Codex，例如 cdx/20260804-fix-duckdb-lock
cc/YYYYMMDD-<kind>-<topic>   # Claude Code，例如 cc/20260804-feat-vp-engine
```

`<kind>` 只能是 `feat`、`fix`、`docs`、`refactor`、`test`、`chore` 或 `deploy`；Codex 或 Claude
可控制的 worktree 必须位于 `.worktrees/`，目录名只用字母、数字和连字符。外部 harness worktree
虽不受位置规则约束，仍须通过全部生命周期门禁，并只能以 harness/tool 支持的非强制 remove 或 close
操作回收；不能证明 ownership 或没有受支持的操作时必须 `quarantined`，不得手工删目录或关闭。不得
在本地 `main` 开发功能。历史非规范名称均为 legacy，不重命名。

### Commit 规范（Conventional Commits）

每条 commit 必须带前缀：

- `feat:` 新功能
- `fix:` bug 修复
- `docs:` 文档
- `refactor:` 重构（不改行为）
- `test:` 测试
- `chore:` 构建/配置/依赖
- `deploy:` 部署相关

示例：
```
feat(data): add Tushare daily OHLCV ingestion with AKShare fallback
fix(indicator): correct MA calculation when there are NaN values
chore: init pyproject.toml with uv
```

### 版本号（SemVer）

- **v0.x.0** = MVP 阶段每周小版本（v0.1.0 Week 1 完成，v0.7.0 MVP 完成）
- **v1.0.0** = 第一次真正可用（能日常跑筛选 + 推送告警）
- **v1.x.y** = 可用后的迭代
- 主版本（2.0.0）保留给架构重大变更

### 受管集成与发布门禁

**合并前**必须完成：本地实际运行与核心路径验证（不是只看代码）、适用测试、
`CHANGELOG.md` 的 `[Unreleased]` 更新，以及相关 `README.md` 或文档更新。改变用户可见行为的任务还须
完成用户业务验收，才可合入 main。

**合并后**按 PR lifecycle record 中的发布分类执行：发布类任务（影响产品行为、runtime、打包产物或生产
部署结果）必须创建指向已在 `origin/main` 的 squash merge commit 的 annotated SemVer tag，并以精确 tag
完成受控部署与健康检查。非发布类任务是不影响产品行为、runtime、打包产物或生产部署结果的任务；包括
实际符合条件的文档、流程、test、refactor 或 chore 变更。非发布类任务必须在 PR lifecycle record 中记录
tag、部署与健康检查均为 N/A 及具体理由。

**关闭前**必须在适用的合并后门禁完成后，按生命周期规范安全回收并向 PR 发布终态交付回执；未完成前任务
不得视为结束。

### Changelog

**`CHANGELOG.md` 用 Keep a Changelog 格式**，每次合 main 时同步写。分类：

- `Added` — 新功能
- `Changed` — 已有功能变更
- `Deprecated` — 即将废弃
- `Removed` — 已移除
- `Fixed` — bug 修复
- `Security` — 安全相关

### 秘钥与配置

- **`.env` 不进 git**（Tushare token、企业微信 webhook 等）
- **`.env.example` 进 git**，作为字段模板
- 所有秘钥从 `.env` 读，代码里禁止硬编码

### 部署纪律

- **部署到服务器只用 tag 或 main 的特定 commit**，不要跟着 main HEAD 盲目 pull
- **`DEPLOY.md` 记录每次部署**：日期 + tag + 备注 + 回滚命令（人肉 deploy log）
- 部署环境和本地环境有差异时（路径、端口、定时任务），用 `.env` 区分，不改代码
- MVP 阶段**先不上 Docker**，Python venv + systemd service 足够；等部署痛了再容器化
- 每次部署前**本地/staging 跑通**再上服务器，不在 main 上直接调试

## 生产环境与协作模式

### 生产环境

- **主部署**：腾讯云轻量服务器 `82.156.0.68`（用户名 `lighthouse`）
  - OS：OpenCloudOS 9.2（RHEL 系，包管理 dnf）
  - Python 3.11.6 + uv，代码在 `/home/lighthouse/rquant/`
  - 装宝塔面板（pNjE）做管理：Web 终端 + 文件管理 + 内置 sshd 配置
  - 调度用 systemd timer + service（unit 文件在 `deploy/systemd/`）
- **本地 macOS**：开发 + 数据热备（rsync 拉云端 DuckDB），不再承担生产调度
- **避坑**：不要写 `82.156.4.48`——那是错的 IP（之前误用过半天），所有服务器命令统一用 `82.156.0.68`

### 数据冗余策略（2026-07-02 分家后）

生产表（日线/筛选/池子）以云端为权威，研究表（分钟线/竞价/模拟盘）以本地为权威：

- **云端**：systemd timer 跑 daily / monitor，生产表写云端主库
- **本地热备落独立文件**：`sync-from-cloud.sh` 把云端快照下载到 `data/cloud_backup.duckdb`，
  **绝不整文件替换 `rquant.duckdb`**（7/2 事故：整文件替换把本地盘中 monitor 的写入
  打进被 unlink 的幽灵 inode，残留 WAL 与新文件代际错配，主库打不开）
- **合并**：日终窗口由 `rquant research-sync` 把生产表从备份合并进本地 `rquant.duckdb`
  （生产表整表替换、研究表按主键 merge），随后原子刷新本地只读副本 `rquant_ro.duckdb`
- **本地盘中 monitor**（launchd `com.roxor.rquant-monitor`）写研究表进本地主库，
  与云端 monitor 并存（云端管告警权威，本地管分钟落库 + 模拟盘实验）
- **禁止**本地再跑 `rquant serve`（曾有僵尸 LaunchAgent `com.roxor.rquant` 本地 17:00
  重复跑 daily，造成重复推送，2026-07-02 已卸载）

### 受控自动发布模式（Codex 代管）

用户已授权 Codex 代管日常 PR merge、tag 和腾讯云代码部署。自动化必须走固定安全链路，
不是任意生产权限：

1. PR 仅在 mergeable 且 Python 3.11/3.12 CI 全绿后 squash merge。只有发布类任务才创建
   annotated SemVer tag，且 tag 必须指向合并后的精确 `origin/main` commit；非发布类任务必须在
   PR lifecycle record 中记录 tag、部署与健康检查均为 N/A 及具体理由，不升级版本、不部署。
2. 腾讯云日常发布只允许通过
   `bash scripts/deploy-production.sh --target <exact-tag>`；禁止盲拉 main。部署器即使技术上接受完整 SHA，
   也仅限受控诊断或既有兼容场景，不得替代普通发布的精确 tag 门禁。
3. Codex 可直接 SSH 做只读诊断和调用上述部署器，不再要求用户粘贴普通部署命令。
4. 部署器只接受干净 tracked worktree、main 内精确 target 和快进更新；使用部署锁、
   `uv sync --frozen`、双 preflight、JSONL 审计及失败自动回滚。
5. 需要重启的发布在工作日 09:15-15:10 自动延期；不存在 force/emergency 绕过。
6. sudo 仅允许 `deploy/sudoers/rquant-production-deploy` 中逐条列出的 rQuant service restart。
7. `deploy/systemd/`、`deploy/nginx/`、`deploy/frp/`、生产数据库写入/修复、密钥轮换和其他
   destructive 操作仍属高风险变更，必须取得用户单独明确授权，不得借自动发布器绕过。
8. SSH、sudo 白名单或回滚链路不可用时，发布状态是 blocked；不得改走宝塔/API 等未审计旁路。

完整操作说明见 `docs/production-release.md`。旧 `scripts/deploy.sh` 仅用于用户明确授权的
人工基础设施发布，不用于无人值守代码部署。

### 通知通道

- **PushDeer**：管理员刘彤的 iPhone + Mac，配 `.env` 的 `PUSHDEER_KEYS`（多 key 逗号分隔）
- **PushPlus**：不装 PushDeer 的协作者（如美丞），配 `.env` 的 `PUSHPLUS_TOKENS`
- 两通道独立调用、独立失败，互不影响
- 验证命令：`rquant notify-test`（绕过场景开关，直接推一条测试消息到所有配置的通道）

## 边界守则（重要）

当讨论到新功能时，先问：**这个需求是否属于「条件筛选 + 监控 + 告警」这个核心范围？**

- 属于 → 可以做
- 不属于（如：下单、高频、Tick 分析、策略自动优化）→ 提醒用户这超出了项目边界，是否要扩张

这是个人项目最容易阵亡的原因——功能无限扩张。

## 相关文件

- 顶层设计：[README.md](README.md)
- 数据源矩阵：[docs/data-sources-matrix.md](docs/data-sources-matrix.md)
- 开源参考：[docs/references.md](docs/references.md)
