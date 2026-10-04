# 查询页候选与验收记录

日期：2026-10-05。范围：v2 C2.2「研究 → 查询」。原候选已完成一次独立审查，五条验收问题需要修复。本轮补修等待原审查者定向复核。生产安装和入口迁移尚未执行。

## 已实现

- 查看公开行情三表、实际字段、行数、日期范围和来源更新时刻。
- 编辑单条 SELECT / WITH，运行、查看计划、用 Ctrl / ⌘ + Enter 运行。
- 查看最多 10,000 行、16 MiB 的有类型结果。空结果、部分结果、失败与超时分别提示。
- 导出当前结果的 CSV。按标准规则转义，公式前缀加保护。
- 命名保存、载入本人查询，按版本更新。每人最多 100 份。
- 保存失联后恢复原命令。只有确认写入后显示「已保存」。
- 账号或 SQL 改变后，旧响应不能覆盖当前内容。支持 390px 和键盘。

API 只鉴权和转发。SQL 在独立私有服务的子进程运行；保存走独立 PageControl 写入桥。查询库只含精确列的独立三表快照，包含盯盘与通知的完整原库不能直接作为查询库。

## 资源策略

每个子进程在导入 DuckDB 前设置 2 GiB 地址空间与 512 MiB 单文件硬限制。DuckDB 内存为 512 MiB，只有一个计算线程。父进程在执行达到 30 秒后停止并回收子进程。

磁盘临时数据的合计上限为 512 MiB。当前策略关闭 DuckDB 临时目录，实际允许量为 0 字节。配置锁定，SQL 不能开启临时目录。超过内存的查询失败，提示「请缩小日期、股票或字段范围」。大排序不会改用磁盘继续运行。

服务最多同时执行 2 个请求、等待 8 个。第 11 个明确拒绝。客户端不能提高上限。

## 原候选实际验证

证据目录：`data/verification/research-query-20261005/`。表中的原失败记录保留，不能当作通过记录。

| 验证 | 实际结果 | 耗时、退出码与记录 |
|---|---|---|
| 最终查询、真实 UDS、保存事务、API、CLI、生成合同与路由清单 | 43 passed，2 skipped | pytest 5.96 秒，命令 6.58 秒，退出 0；`query-focused-final-03.log` |
| macOS 内存/临时目录补修 | 红测 4 项失败，修复后 7 passed、2 skipped | 绿测 0.64 秒，命令 0.82 秒，退出 0；`spill-disabled-red-01.log`、`spill-disabled-green-02.log` |
| 原 SDK | 3 passed | 2.45 秒，退出 0；`sdk-regression-01.log` |
| 全部 Web API 与六个直接控制面模块 | 1,102 passed，原 2 项清单失败 | 246.41 秒，原退出 1；`shared-web-control-regression-01.log` |
| 上行两项清单修复 | 在最终 43 项中通过；OpenAPI 与 79 条路由相符 | 仅更新受影响的清单和预期，复用其余 1,102 项 |
| 前端完整检查 | Biome、两套 tsc 和 705 个 Vitest 通过 | Vitest 24.67 秒，退出 0；`frontend-check-01.log` |
| 最终查询前端定向回归 | 9 passed | Vitest 3.45 秒，命令 3.93 秒，退出 0；`ui-query-final-03.log` |
| 真实浏览器 | 1440px、390px 两项通过；原保存恢复用例修复后通过 | 两宽度见 `browser-01.log`；恢复用例 1.5 秒、退出 0，见 `browser-recovery-03.log` |
| 最终 TypeScript 与 Vite 构建 | 通过 | 命令 4.91 秒，退出 0；`build-final-02.log` |
| 提交后 dist 重建比对 | 通过，重建产物与提交相同 | 命令 1.17 秒，退出 0；`dist-verify-final-02.log` |
| 首屏体积 | gzip 325.0 KiB，小于 550 KiB | 退出 0；`size-final-01.log` |
| Ruff 与本片格式 | 通过 | 退出 0；`ruff-final-09.log`、`ruff-format-final-10.log` |

两项 skip 是 Linux 硬限制测试。macOS 入口按合同拒绝执行查询，不能用 macOS 结果证明 Linux 内核强制。

浏览器使用 `web/e2e/research-query.config.ts` 的三项本片用例，真实加载构建产物，API 返回合成数据。默认整套 `pnpm e2e` 未运行。宽度、CSP、键盘、文本、CSV 和保存恢复证据不等同于生产数据验收。

第一次最终 pytest 命令写错了一项测试选择名，未运行测试、退出 4；记录在 `query-focused-final-02.log`，已修正后运行全部本片用例。浏览器原失败包含选择器错误及构建同时重建 dist 导致的静态文件 404；失败记录保留，后续改为串行构建与浏览器验证。

### 真实 Linux 补充

root 在自有独立 `/tmp` 目录运行合成输入。Python 3.14.4、DuckDB 1.5.2；未读取生产库或凭据，未调用数据提供方。

最终源包逐项核对了 669 个 Python 文件。源绑定为 `87367891743b973bc866c888dfd91b7e13f729d6603bb54cd690a19401a3e65a`。脚本 `linux-no-spill-proof-r06.py` 的 SHA256 为 `f9d453008ef291a2767885952aafb1c7f0c0babb147caeabb595105effbe5737`。

实际命令退出 0，SSH 整命令 5.4747 秒，证明程序 4.370 秒：

- 实际子进程返回空临时目录、0 字节临时数据配置、512 MiB 内存、1 个线程、关闭外部访问及锁定配置。
- 同一 64 MiB 内存大排序明确失败；1,715 次采样的文件长度及实际分配量均为 0。
- 正常行情查询成功。2 个子进程同时运行，8 个请求等待，第 11 个为 busy。
- 10 个请求取消后，正常查询仍可运行。6 个子进程均已回收。
- 来源与快照摘要未变，scratch 清空。FD 与完成初始化后的基线相同。远端自有目录已删除。

原始记录：`root-linux-product-r06/command.json` 与 `stdout.json`。stdout SHA256 为 `6b11fedf5838c4e02d380c1402497ef2040b9dc3447645a55f1cefcc674e496e`。

原 r03 命令因初始化 FD 差值退出 1。它的 13 次查询状态、子进程回收、地址空间、单文件、行数和字节字段证据仍保留。固定 FD 随纯 DuckDB 建表和 schema 读取出现，之后两轮不增长，已按初始化基线处理。原临时数据配置的合计越界由 r05 实际证实；不称 r03 或 r05 整体通过。最终零临时数据策略由 r06 独立关闭此问题。

## 首次独立审查与定向补修

原报告在 `independent-product-review/review.md`。审查对象为 `90b61007d47bd906bb85a3e4f67521b12cdf2cbb`。五条问题都按原稳定 ID 修复，未增加审查阶段。

| ID | 修复 | 直接验证 |
|---|---|---|
| RQ-R01 | 按完整 HTTP JSON 计算 16 MiB 上限。超限时保留能返回的行前缀，仍返回 `partial`。单行过宽时可返回 0 行。 | 真实 API 响应覆盖正常元数据和 600 字中文详情、0 行和 2 行前缀。字段、类型、来源时刻和摘要保留。 |
| RQ-R02 | 超过 JavaScript 精确整数范围的值用 `integer` 类型和十进制文本返回。Pydantic 拒绝原始越界整数，防止转成浮点数。 | 实际 DuckDB 的正负边界经产品转换和 JSON 后，React 显示及 CSV 文本均保持精确。布尔值和原特殊类型回归保留。 |
| RQ-R03 | 回执只更新仍在使用的保存目标。载入、另存为和换表都会改变目标序号。旧命令仍结算原回执与恢复记录。 | 延迟保存 A → 载入 B → A 回执 → 保存 B，实际提交仍为 B/v7；同一序列的「另存为」分支按新查询提交。原账号、CAS 和恢复回归通过。 |
| RQ-R04 | 词法检查支持 dollar 字符串、E 转义字符串和嵌套注释。DuckDB 仍检查真实单条 SELECT 类型。 | 原三条合法 SELECT 实际返回正确值；后续第二条语句、文件访问、配置语句和未结束引号/注释仍拒绝。 |
| RQ-R05 | 拒绝额外持久 TYPE 和显式 INDEX。原表、列、view、function、sequence、schema 检查保留。 | 三种真实额外对象由 guard 和 loader 拒绝；清洁 builder 快照仍可用，来源摘要不变。 |

精确整数的 JSON 示例：`9007199254740993` 返回 `{"kind":"integer","text":"9007199254740993"}`；负数在文本中保留负号。`±9007199254740991` 仍返回 JSON 整数。CSV 使用同一结果文本，继续保护公式前缀。

本轮原始命令、stdout、stderr、退出码、耗时和摘要均保存在 `review-fix-01/`。以下为本轮实际结果；不替换原失败记录。

| 验证 | 实际结果 | 记录 |
|---|---|---|
| 实现前集中红测 | 后端 17 failed、8 passed；前端 2 failed、8 未选中 | `backend-red`、`frontend-red`；退出 1 |
| 五条相关后端集中绿测 | 25 passed，2.06 秒；命令 2.77 秒，退出 0 | `backend-five-green-02` |
| 直接查询、真实 UDS、权限、CAS、恢复、OpenAPI 与路由清单 | 67 passed、2 skipped，6.68 秒；命令 7.30 秒，退出 0 | `query-direct-green` |
| 完整 HTTP 最后复核 | 4 passed；只为新增测试类型注解复核原四例 | `r01-final`；退出 0 |
| React 保存目标的「另存为」分支 | 修复前 1 failed；失败请求仍指向 A | `r03-save-as-red`；退出 1 |
| 最终查询前端与同结果 CSV | 12 passed，3.21 秒；命令 3.62 秒 | `frontend-query-green-02`；退出 0 |
| 两套 TypeScript 检查 | 通过，命令 5.17 秒 | `frontend-typecheck-green-02`；退出 0 |
| 最终构建 | 通过，命令 4.39 秒 | `frontend-build-final`；退出 0 |
| 本片源代码、测试与前端格式 | Ruff 与 Biome 通过 | `python-code-lint-final`、`frontend-lint-final-02`；退出 0 |
| 提交后 dist 比对及首屏大小 | 重建产物与提交相同；gzip 325.0 KiB，小于 550 KiB；命令 0.94 秒 | `dist-verify`；退出 0 |

本轮另外保留了三个设置或测试错误：DuckDB 的 index 目录没有 `internal` 字段；新测试误用了 `getByRole` 的 `exact` 参数；移除该参数后测试需要重新格式化。这些失败已按实际接口修正。原 Linux 证明脚本带有继承的独立脚本风格提示，本轮未改写已封存的证明脚本。

复用原 1,102 个共享后端通过项、705 个前端通过项及三个浏览器用例。没有重跑全量或默认 E2E。查询页使用本轮定向结果，不能把旧整套结果称为本轮重跑。

### 新 Linux 源绑定

原 r06 的源绑定是旧候选的证据，不能作为本轮新源码的执行证明。变更仅为 `child.scalar`、查询合同/词法、快照对象校验和 HTTP 包装。硬限制安装、30 秒截止、容量、取消及零临时数据策略未改变。

新清单为 `linux-backend-freeze-r07.json`，669 个源文件的绑定为 `974ce9b49c3b20353e1b1aca3c822f71bcaf7732f5babc99c568cd2722d69e3a`。证明脚本为 `linux-review-boundaries-r07.py`，SHA256 为 `c5305d5c51bef2e45a615ddfb43064e75accb6255880577af2274af128c32ebb`。

父任务已在合成私有目录实际执行此脚本。命令退出 0，SSH 整命令 11.483 秒，证明程序 10.209 秒。记录在 `root-linux-product-r07/command.json` 与 `stdout.json`；本树保留的回执副本为 `review-fix-01/linux-r07-receipt.json`。stdout SHA256 为 `6f4767bd862491cf1fa488d5b2ad7ac413f161494ce5ca5d430b4549191ffe10`。

新脚本的六个 BIGINT 边界、三条合法词法 SELECT、额外 ENUM 拒绝均通过。宽行查询返回 1 行 `partial`，私有完整响应为 9,000,344 字节。默认配置与大排序失败符合原零磁盘临时数据策略；3,970 次采样的逻辑和分配字节均为 0。

2 个请求实际运行，8 个等待，第 11 个为 busy。取消后可再查，11 个子进程全部回收。来源与快照未变，scratch 和远端私有目录已清理；初始化后 FD 没有增加。完整 HTTP 和保存界面使用本轮本地真实 API/React 证据。原清单保留冻结时的待运行字段，实际执行结果以本回执为准。

## 复现入口

本地工作树：`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk`。

Python 借用只读环境：`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`，版本 3.13.12。用 `-I -B`，显式将本树 `src` 加入 `sys.path`。测试使用私有 `/private/tmp/rquant-query-tests`，关闭 dotenv，并使用全零虚拟 token 和空通知配置。

最终 pytest 输入为六个 `test_research_query*` / `test_web_research_query.py` 文件、`test_web_openapi_snapshot.py` 及 `test_web_proxy_identity.py::test_current_user_route_inventory_matches_documented_categories`。已有共享回归使用全部 `test_web_*.py`，加 `test_page_control.py`、`test_page_control_service.py`、`test_page_control_watchlist.py`、`test_factor_save_page_control.py`、`test_factor_run_page_control.py`、`test_lab_page_control.py`。

前端使用 Node 22.22.2、pnpm 10.33.0，本树离线锁定依赖。实际命令为 `pnpm check`、`pnpm exec vitest run src/api/researchQuery.test.ts src/pages/query/query.test.tsx`、`pnpm build`、`pnpm verify:dist`、`pnpm size`。浏览器为 `pnpm exec playwright test -c e2e/research-query.config.ts`；恢复补测只选对应原用例。

CLI 有 `rquant research-query build`、`serve` 和 `save-serve` 三个入口。build 要求显式来源、来源摘要、来源时刻及输出目录；两个服务要求各自私有 socket、受信 Web UID、共享 GID 与精确账号名单。参数通过实际 `--help` 验证。

## 当前停止状态与剩余门禁

本轮功能代码、生成产物和 Linux 源清单已提交。停写产品提交为 `025ef8ae5a7d21f3d5a4f2dd9563ce2e400cf30d`。最终候选再加入本验收记录与 `review-fix-01/` 原始证据，产品树相同；完整 HEAD 由父任务记录。原产品提交 `127a16d9c2b59516877e348f89f80a9b16553eb5` 的通过项和失败记录均保留。

implementer 停止修改，等待原 reviewer 定向复核 RQ-R01 至 RQ-R05 及直接回归。独立系统用户之间的实际安装、生产 slice、systemd/socket、nginx、生产查询快照发布和 Streamlit 入口停用尚未执行，需要后续按现有授权边界完成。此片通过不表示整个平台 goal 或 M2 已完成。

工作树保留供终审和合版；未创建新工作树。SDK 旧接口、旧示例与原产物保留。测试子进程与服务已正常退出；没有需要保留的本任务后台服务。
