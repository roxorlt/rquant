# 历史资料准备与新旧结果比较

更新：2026-10-08。本说明对应当前本地候选，项目版本字段为 `0.31.0`。尚未发布。

## 这两个命令做什么

- `prepare`：核对原分钟、上一交易日候选、日历和策略登记，输出历史重建准备结果。
- `compare`：读取新旧原结果和各自冻结的成交规则，逐项比较。每项差异须有数值依据。

两项均为离线命令。它们不安装数据，不启动研究计算，不修改原输入。输出是诊断资料，不是正式32日验收或模拟盘批准。

当前候选已有22项命令聚焦测试。原 `rquant.cli.main` 和工作树 `.venv/bin/rquant` 都已分别实际调用两项动作，输出相同。环境为Python `3.13.12`，项目版本字段为 `0.31.0`。这些结果可在相关代码未变时复用；Python3.11/3.12及Linux验证仍归整体验收。

## 运行示例

下面两份输入是已保存的合成诊断示例。第一份缺盘中可见事实，应返回“不可用”。第二份有3项可解释差异，未解释应为0。

先确认输出文件尚不存在。命令不覆盖已有输出。

```bash
rq_wt=/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration
rq_examples="$rq_wt/data/verification/minute-engine-completion-20261007/r11-use-path-preparation-01/implementation-01"

env -i PATH=/usr/bin:/bin PYTHONPATH="$rq_wt/src" \
  PYTHONDONTWRITEBYTECODE=1 RQUANT_DISABLE_DOTENV=1 PYTHON_DOTENV_DISABLED=1 \
  "$rq_wt/.venv/bin/rquant" minute-historical prepare \
  --input "$rq_examples/prepare-missing-facts.input.json" \
  --output "$rq_wt/data/verification/goal-progress-root/history-prepare-user.json"

env -i PATH=/usr/bin:/bin PYTHONPATH="$rq_wt/src" \
  PYTHONDONTWRITEBYTECODE=1 RQUANT_DISABLE_DOTENV=1 PYTHON_DOTENV_DISABLED=1 \
  "$rq_wt/.venv/bin/rquant" minute-historical compare \
  --input "$rq_examples/compare-frozen-synthetic.input.json" \
  --output "$rq_wt/data/verification/goal-progress-root/history-compare-user.json"
```

## 如何读结果

| 结果字段 | 含义 |
|---|---|
| `status: unavailable` | 必要资料缺失或不能按指定规则读取。查看结果中的具体原因。 |
| `status: diagnostic_complete` | 本次离线诊断完成。仍须查看差异、未解释数及正式验收标志。 |
| `formal_history_passed: false` | 正式历史验收没有通过。示例的两次输出都是此值。 |
| `diagnostic_only: true` | 只能作为诊断证据。 |

比较示例中的3项差异是合成结果的成交及费用差异。各自规则已重算验证，未解释为0。不能把这一示例称为真实32日结果。

## 换成真实资料

1. 从同目录的 `input-templates.json` 找到输入模板说明。
2. 用实际原文件和原校验值填写资料引用。列式资料还须填写列名及单元格位置。
3. 准备日历、实际策略登记及盘中可见事实。缺资料就保留缺失，不能推测补齐。
4. 比较须提供新旧各自的完整原流水、规则版本、输入和结果。不能从成交摘要推造缺失流水。
5. 指定新的输出路径，运行对应命令。保留原文件及失败输出。

完整资料输入模板只是格式说明，不证明资料已经齐全。当前真实小样本的原字段引用已核对；实际日历和策略登记仍缺，完整真实准备尚未执行。

最终口径：开始时刻标记的分钟转换为结束时刻；结束时刻标记保持。候选使用上一已完成交易日。报告显示“历史重建”。缺少当时可见资料时显示不可用。新旧引擎用各自冻结规则解释差异，未解释必须为零。实际20个开市日观察另行完成。
