# 本机研究 SDK

`rquant.research_sdk` 直接公开生产使用的特征 reader、因子检验函数和研究产物 reader。所有入口沿用现有 Pydantic 模型，没有额外的计算口径。此项完成前端计划 v2 C2.1 的共享库入口；M2 的 SQL 查询服务和云端 Notebook 环境仍需单独交付、验收。

| 用途 | SDK 入口 | 校验范围 |
| --- | --- | --- |
| 封存特征 | `FactorDailyFeatureSource`、`FactorDailyFeatureQuery`、`open_factor_daily_feature_source` | 来源模型与摘要、封存资料、实际字段及日期/代码范围 |
| 因子检验 | `evaluate_factor`、`evaluate_factor_time_series`、`assemble_factor_research_result` | 原有检验库与请求模型 |
| 分组收益和 IC 汇总 | `evaluate_factor_portfolios`、`summarize_factor_ic` | 原有诊断库与结果模型 |
| 完整研究结果 | `load_factor_research_artifact` | 原有内容地址、规范 JSON、模型、请求与结果的一致性 |
| 展示产物 | `load_factor_display_artifact` | 展示文件的原有内容地址与模型；本例额外检查它引用同一完整研究结果 |

完整结果的读取及请求复算不代表重新读取原始行情并完成独立重放。展示产物的读取也不代表完整研究重放。

## 读取一批特征

调用者显式提供 `FactorDailyFeatureSource` 和绝对路径的 `lake_root`。来源描述由已有封存流程生成；SDK 导入及读取不依赖应用配置、`.env` 或主库。

```python
from rquant.research_sdk import FactorDailyFeatureQuery, open_factor_daily_feature_source

# source 是调用者已加载的 FactorDailyFeatureSource；字段以 source.fields 为准。
query = FactorDailyFeatureQuery(
    source_sha256=source.sha256,
    trade_date=source.scope.end_date,
    stock_codes=source.scope.stock_codes[:2],
    fields=("ma5",),
)
with open_factor_daily_feature_source(source, lake_root=lake_root) as reader:
    batch = reader.query(query)
```

每个请求只读一天、1–500 个代码、1–50 个字段，最多返回 25,000 个事实。未知字段、来源中没有的字段、范围外日期或代码、重复字段或代码及超过批限制的请求会抛出异常。SDK 保留这一单批合同；批间迭代由调用者显式决定。

`source.fields` 给出实际目录、名称、单位与值语义。`batch.facts` 保留原值、`status`、`reason`、非有限值标签及已有诊断。`null`、缺失与非有限值各有其原始状态；0 是有效值。金额和百分比沿用封存单位，例如市值 `CNY_10000`、占比 `percent`（0–100）。

市场温度封存表按交易日只存一份全市场值。请求中的市场字段只在当前有界代码集合内展开；这不会用当前股票池重算占比。`with` 在正常返回和异常时都会关闭连接、删除 reader 的自有临时目录。沿用现有 reader 的私有目录合同：`lake_root` 须是属于当前用户、权限为 `0700` 的绝对路径，且允许 reader 在其下创建和清理临时副本。

## 可执行的 Notebook 等价示例

[docs/examples/research_sdk.py](examples/research_sdk.py) 使用 `# %%` 分成 Notebook 单元，也可在 Notebook 中导入 `inspect_research`。脚本只接收显式文件和根目录，来源描述最多 16 MiB。它查询特征、调用共享检验库复算已有请求，再读同一结果的展示产物。

本机已验证环境是 Python **3.13.12**，解释器为 `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python`。以下命令使用本次保留的合成封存样本，`-I -B` 并显式优先加载本树 `src`；不会安装或写入借用环境：

复制或从 Git 恢复样本时，先将当前用户所有的 `demo/lake` 和 `demo/artifacts` 目录设为 `0700`；Git 不保留目录权限。使用本树样本的命令如下：

```bash
chmod 700 \
  /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/data/verification/research-sdk-20261005/demo/lake \
  /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/data/verification/research-sdk-20261005/demo/artifacts
env RQUANT_DISABLE_DOTENV=1 \
  /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python -I -B -c \
  'import runpy,sys; sys.path.insert(0,sys.argv[1]); sys.argv=sys.argv[2:]; runpy.run_path(sys.argv[0],run_name="__main__")' \
  /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/src \
  /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/docs/examples/research_sdk.py \
  --source /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/data/verification/research-sdk-20261005/demo/source.json \
  --lake-root /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/data/verification/research-sdk-20261005/demo/lake \
  --date 2026-07-03 --codes 000001.SZ 000007.SZ \
  --fields ma5 market_above_ma20_ratio_pct market_high_60d_ratio_pct \
  --artifact-root /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk/data/verification/research-sdk-20261005/demo/artifacts \
  --research-sha256 b51e119b3ce81528bfed3f82038da98473b8bf71c3ca440bd31248dd9b8e6f2d \
  --display-sha256 7f4aacba90125fa05ca29d064a38e9dc632fbca01364e426e10905e8c3139a33
```

输入清单与校验值保存在同目录的 `demo/inputs.json`；为自己的来源替换全部显式输入。该样本的封存包由既有测试工厂和生产封存函数生成，研究结果沿用既有因子检验证据，没有复制 producer 或统计计算。
