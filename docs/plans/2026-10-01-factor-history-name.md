# 因子历史名称补充

## 目标与边界

接入明确按股票代码取得的历史名称区间，补足历史日名单中的名称/ST未知，使现有全市场成员归档可以使用完整的必要事实。沿用已有全市场/创业科创选择及成员归档，不改计算、网页或生产数据。

普通任务：只新增有界来源捕获与纯事实补充，写自有私有目录，不改权限/账本/生产契约。实际产品Codex桌面；根任务 `/root` 负责范围和验收，Codex原生implementer编码，最终一名独立reviewer集中审查。工作树 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-history-name`，分支 `cdx/20261001-factor-history-name`，干净基准 `14d834433f18b2c183b1e4ba128acc57195f5969`；本计划是根任务唯一初始改动。

写集限定：`src/rquant/factor/security_collect.py`，必要时一个同域名称采集模块；`src/rquant/adapter/tushare.py`仅必要的薄raw方法；collector同域测试和本计划。现有`namechange_raw`会重排列，不能冒充字段/值未改的原件；若加方法，应保留实际响应并走原backoff/transport observer。不得改 `security_status.py` 的既有物化/边界规则、universe、archive、worker、React、部署或全仓门禁。根任务维护最终清单与进度。

子代理仅在指定树和自有/private/tmp离线开发，禁用dotenv，不访问网络、凭据或生产环境，不继续委派。来源调查/live由root通过现有正规认证执行，不打印或归档凭据。遇到未知dirty改动、越界、前提实质变化或一轮修复后仍阻断时停止报告。

## 已有具体证据

官方 [namechange](https://tushare.pro/document/2?doc_id=100) 是历史名称变更，输入start/end是公告日期筛选；本片单代码完整历史请求不施加这些筛选，不把公告日期当名称有效起点。[stock_st](https://tushare.pro/document/2?doc_id=397) 的缺席不能推断非ST，本片不接入负向ST推断。

root 2026-10-01真实探针 `/private/tmp/rquant-factor-history-st-pfk1quhn` 的metadata和`03-namechange-response.json`：请求仅ts_code=301139.SZ，六字段ts_code/name/start_date/end_date/ann_date/change_reason，实际收到 `2026-10-01T07:15:54.828784+00:00`；SHA256 `5fd55bd90c5a4bc6be9b6b9ba7f32d9cb236b096e8c3f81c09a9baa4e85a1326`。三段：元道通信2022-07-08—2026-05-11，*ST元道2026-05-12—09-29，元道退09-30起（公告09-21、原因退市整理期）。旧collector因为09-30 bak_basic缺此股票历史名称而保留is_st=None，all拒绝；gem仍可用。

真实旧来源 `/private/tmp/rquant-factor-security-root-j2oc17a3/live-capture`（十五参考分区及09-28/29/30，18实际调用）可离线读取，禁止重写。现有成员归档和RO→configured worker已经验收，不重复造计算器或认证入口。

## 设计与可验证验收

1. **独立、有界、显式的名称来源。** 使用类型化请求/响应/回执，保留实际单代码参数、字段/值、实收时间、实算原件摘要。最多显式16代码、64实际dispatch（含重试），只留一份当前响应；局部行/字节预算只作资源门限，不冒称提供方完整性保证。live lazy创建主token客户端，普通import不初始化Settings/网络。可导入上述探针名义的namechange原件，核对metadata/请求/字段/摘要/时间，别把同目录stock_st当作使用的来源，也别丢弃额外参数后冒充完整历史请求。
2. **沿用已有完成发布和loader。** 复用已验收的私有写入、原子完成/中断拒绝工具；新目录不覆盖，坏摘要/错误代码/缺字段/中断明确拒绝，不再扩建一套持久化框架。
3. **纯名称区间补充。** 复用`normalize_namechange_history`和`normalize_name`的名称/ST解释。所需代码/日期恰有一段可解释有效区间（end包含当日；空end开放）才能补值；坏行、重叠/冲突或无覆盖保持未知/明确拒绝，不能拿当前名、邻日、ST表缺席或默认False补。只补既有已上市集合的历史名称，不增删证券、不改板块/上市日/退市日。补充与已有非空历史名矛盾时拒绝该日，不能静默覆盖。
4. **同一来源证据。** 源摘要包含实际使用的名称原件及旧日/参考资料；observed_at取所有实际使用响应最大实收时刻，保持historical_retrospective，不宣称PIT。无补充入口的旧批次摘要/行为保持。显式可选名称来源接到replay/archive，依旧先验所有请求日再发布；不用事后patch已选完股票池。
5. **具体行为验证。** 离线用真实原件：09-30旧all依旧拒绝；提供合法历史名称区间后all准入，301139.SZ当日is_st由「元道退」得到False，09-29/28仍True并被all排除。股票全集不变，gem成员结果不变，source SHA/观察时刻实际变化。测试包括区间首/尾日、无覆盖/重叠/错代码/坏摘要、额外过滤参数拒绝、真实缺口补充和已有archive消费；复用已有效的中断/私有文件及259旧回归，不追加无关矩阵。

## 执行与最终验证

先记录实际身份、分支、基准和唯一初始计划，写必要红测后局部实现，运行新增与直接受影响验证。保留准确nodeids及去重有效结果；Ruff/format/diff和3.11语法解析即可，不扩大到全仓/前端或把语法解析称runtime验证。冻结干净candidate；按AGENTS一次集中终审，普通任务至多一次原作者定向修复与原reviewer复核。

root在接受后执行一次实际单代码live、真实replay/archive；复用已有有效RO/worker证据，只有实测显示下游契约失效才追加计算验证。新增pytest时正常更新固定清单并跑两项必要合同门禁，不执行全部18,850项。记录所有自有进程/目录处置，合入本地集成并更新进度；长历史多批、CSI成员、行业/市值、18:40跟踪、生产配置与上线继续后续实施，整体goal不因本片完成而complete。

## 实现与验收记录（2026-10-01）

身份核对：Codex桌面原生子任务 `/root/factor_security_collection_impl`，父任务 `/root`；实际工具直接编码/运行验证，没有继续委派或跨工具。分支、HEAD与上述冻结基准一致，初始唯一dirty为root创建的本计划。写集为本计划、`src/rquant/factor/name_collect.py`、`src/rquant/factor/security_collect.py`、`src/rquant/adapter/tushare.py`及`tests/unit/test_factor_name_collect.py`。

名称采集独立类型模型保留原始列/标量、请求代码、请求/实收时刻和原件SHA；单代码不带公告日期过滤。上限16代码、64实际dispatch（含重试）、每响应2000行/1MiB，后两项仅为局部资源预算。live调用旧transport observer与backoff，主token客户端只在显式live时初始化。import仅选择namechange记录，逐个核对完整请求、原始字段、实际摘要和时间；同目录stock_st不参与推断。持久化复用旧私有写入、原子完成发布及中断拒绝工具。

`normalize_namechange_history`与`normalize_name`解释有效期；首尾日包含在区间内，空end开放，公告日期不充当生效日。缺覆盖保留unknown；坏行或当日多个区间拒绝，日名单非空名冲突拒绝。补充仅作用于已证实的当日上市集合。未传补充时，旧source模型/摘要/行为保持；实际用到的名称原件才加入新摘要，观察时刻取实收最大值，仍是historical_retrospective。`security_status.py`、universe、archive合同、worker均未改动。

入口：

```text
python -m rquant.factor.name_collect live --code 301139.SZ --root <新私有目录> --max-calls <显式预算>
python -m rquant.factor.name_collect import-probes --source-root <实际探针目录> --root <新私有目录>
python -m rquant.factor.security_collect replay --capture-root <旧来源目录> --name-root <名称目录> --selection all
python -m rquant.factor.security_collect archive --capture-root <旧来源目录> --name-root <名称目录> --selection all --as-of <实际截止时间> --input-root <新私有目录> --archive-root <新私有目录>
```

实际环境：`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`，Python 3.13.12；PYTHONPATH指向本树src。测试通过`env -i`建立仅含PATH、`RQUANT_DISABLE_DOTENV=1`、全零合成token、私有临时DATA_DIR/DUCKDB_PATH/PARQUET_DIR/LOG_DIR、TZ及PYTHONPATH的环境，无网络/真实凭据/生产访问。

证据根 `/private/tmp/rquant-factor-name-implementation-_ynrggha`：

- `red-final.log`：新增28项先红，缺名称模块/薄raw方法/可选接线。`green-initial.log`记录首轮26通过和两个测试接线错误（包导入、reader按既有合同重绑定来源）；修正后`green.log`为28 passed，2.98s。
- `focused-regression.log`及XML：`python -m pytest tests/unit/test_factor_name_collect.py tests/unit/test_factor_security_collect.py --tb=short --junitxml=...`，61 passed，4.92s，无skip/deselect。新增28项与直接旧collector33项互不重复，精确nodeids分别保存于`new-nodeids.txt`和`direct-old-nodeids.txt`。此前259旧依赖回归仍有效，复用结果，未重跑；此前33项collector不在259项中，累计独立口径为28+33+259=320项。本实现未修改清单或执行生成器。
- Ruff check、format --check、git diff --check通过；四个源/测试文件用`ast.parse(feature_version=(3,11))`通过3.11语法检查，不宣称3.11/3.12运行时验证。
- `actual-name-import.json`：只导入root实际301139.SZ名称原件；保留实际请求时刻`2026-10-01T07:15:53.671492+00:00`、实收`07:15:54.828784+00:00`及原件SHA `5fd55bd90c5a4bc6be9b6b9ba7f32d9cb236b096e8c3f81c09a9baa4e85a1326`。这是已有资料的离线导入，未冒称本实现者进行了live网络采集。
- `actual-all-replay.json`、`actual-evidence.py/json`：读取root旧18次实际捕获，三日全集不变、gem精确成员不变，源SHA实际改变，最大实收为上述名称实收时刻。两池三日归档已由旧reader执行require_completion。
- `actual-cli-archive.json`、`cli-reader-resource.txt`：实际CLI archive发布及已有reader再次消费，三日all成员5022/5024/5026，完成校验通过。

按最新日倒序核验（向前共推2个交易日）：

| 日期 | 股票全集 | 301139.SZ is_st | all成员 | 该股入all | gem成员 |
| --- | ---: | --- | ---: | --- | ---: |
| 2026-09-30 | 5572 | False（元道退） | 5026 | 是 | 2027 |
| 2026-09-29 | 5571 | True（*ST元道） | 5024 | 否 | 2027 |
| 2026-09-28 | 5569 | True（*ST元道） | 5022 | 否 | 2026 |

不传名称补充，09-30 all仍拒绝，09-29/28维持准入；gem三日成员均与旧资料一致。实际source SHA、归档reference及日成员结果完整记录在上述JSON中。调用/资料完整性只依据显式source合同及逐日必须事实，不从stock_st缺席或当前名称推出False，不宣称历史PIT。

资源：全部命令会话与`subprocess.run`均已退出，未启动后台服务；自有证据根没有遗留`*.tmp`发布文件。证据、捕获、归档及离线配置目录保留供root终审；root已有来源目录仅只读，未覆盖。下一步由root对干净候选安排一次独立终审，并负责单代码真实live、清单/两项必要门禁及集成决策。

## 根任务最终验收（2026-10-01）

受审候选 `7bef6f071c02863c9585f877adccc570d5552b5f`，干净基准14d834；原独立reviewer对本片做一次集中终审，accept、无finding，报告 `/private/tmp/rquant-factor-name-final-review-ZT48vv0q/review.md`。本地合入 `64ea9af254a56f1ca6b2445114c65881543a366d`；源码/新测试与候选精确一致。实现61相关passed/4.92s及259旧依赖证据仍有效、去重320；没有重复全仓审查或测试。

root通过已有应用Settings主token正规认证运行新live入口，显式301139.SZ/max-calls=1，实际dispatch=1，无备用token。实收 `2026-10-01T08:21:56.709748+00:00`，三段名称原件SHA仍为 `5fd55bd90c5a4bc6be9b6b9ba7f32d9cb236b096e8c3f81c09a9baa4e85a1326`；完成manifest SHA `5f7f7011fceb0bae08cf66c3aa4f462b95a2d80312cf175c49cd12f0d2b6f9cb`，字节/引用SHA独立复核。未打印或归档凭据。普通导入预检无Settings初始化。

最新日倒序核验三日：09-30全集5572/all5026/gem2027、该股False并入all；09-29全集5571/all5024/gem2027、09-28全集5569/all5022/gem2026，该股均True并被all排除。全集和gem精确成员不变，其他事实逐字段不变；source SHA实变、observed取本次名称实收。无名称入口旧09-30 all依旧拒绝。两池三日归档由现有reader require_completion：all摘要 `a431fee92af5d8d52f0f45f99e10e6d33a6e6ebbea1082f686a028fcf66f4e4f`，gem `8f8e84eba6f478f29b82a5c96ca8e07c96531ad6b7b8ed9e73004e9c7613264c`，均68,674 bytes。只补必要日事实及来源绑定，原reader/worker契约未改，复用此前真实RO/configured-worker证据。

固定清单正常生成18,878 cases/55 skips，SHA256 `cab14c58cb515f2b31d4e452c177e57ee640a2869ea108656cbe325cb2924962`；比较只新增本片28nodeids，无删除/重复，approved-skips字节不变。README/计数断言同步，两必要门禁Python3.13.12实际2 passed/8.81s，Ruff/diff通过。没有全量执行、新CI、3.11/3.12 runtime或PIT通过声明。

root证据 `/private/tmp/rquant-factor-name-root-7dyir3f0` 保留live原件/完整实际回放与归档、root-summary、comparison及两门禁日志。所有本片命令进程退出、无发布tmp/中断记录遗留；没有写生产数据、推送、部署或停Streamlit。另已只读预检现有RO的08-11—09-30共36交易日，作为下一片长历史装配候选，不以行数证明市场完整性。CSI官方09-09调样附件只读保存在 `/private/tmp/rquant-csi-official-announcement-y0r9h645`，尚缺完整基线/范围覆盖及精确生效日。长历史、CSI、行业/市值、18:40跟踪与生产验收继续实施，M3仍部分，整体goal active。
