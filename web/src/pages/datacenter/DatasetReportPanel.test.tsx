import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { Schemas } from "@/api/client";
import { DatasetReportContent } from "./DatasetReportPanel";

type Dataset = Schemas["AuditReportDataset"];

const dataset: Dataset = {
  dataset_id: "minute_bar",
  name: "股票分钟线",
  source_id: `sha256:${"a".repeat(64)}`,
  source_kind: "fixed_replica",
  source_state: "ready",
  source_column_present: true,
  scope: "audit_range",
  audit_start: "2026-01-30",
  observed_through: "2026-02-03",
  as_of: "2026-02-03T09:30:00+08:00",
  contract_sha256: "b".repeat(64),
  rule_version: "catalog-dataset-audit-v1",
  visibility: "minute_as_of",
  coverage_state: "measured",
  coverage_reason: "date_presence_only",
  coverage_label: "已统计",
  completeness_state: "missing_expected_scope",
  completeness_label: "完整范围未确认",
  freshness_state: "missing_expected_scope",
  freshness_reason: "session_grid_unknown",
  freshness_label: "缺少应有范围",
  freshness_lag_sessions: null,
  observed_age_seconds: 0,
  latest_visible_date: "2026-02-03",
  latest_visible_time: "2026-02-03T09:30:00",
  latest_ingested_at: "2026-02-03T09:35:00",
  observed_rows: 2,
  visible_rows: 1,
  pending_rows: 1,
  recorded_after_as_of_rows: 2,
  unknown_source_rows: 0,
  unknown_frequency_rows: 0,
  expected_open_days: 2,
  covered_open_days: 0,
  omitted_gap_count: 0,
  omitted_gap_open_days: 0,
  omitted_closed_day_count: 0,
  omitted_closed_day_rows: 0,
  omitted_row_changes: 0,
  monthly: [
    {
      month: "2026-01-01",
      expected_open_days: 1,
      covered_open_days: 0,
      coverage_ratio: "0.0000",
      status: "measured",
    },
    {
      month: "2026-02-01",
      expected_open_days: 1,
      covered_open_days: 0,
      coverage_ratio: "0.0000",
      status: "measured",
    },
  ],
  gaps: [{ start: "2026-01-30", end: "2026-02-02", missing_open_days: 2 }],
  closed_day_rows: [],
  fields: [
    { field_name: "close", name: "收盘价", observed_rows: 2, null_rows: 1, required_key: false },
  ],
  frequencies: [
    { frequency: "1min", row_count: 1, visible_rows: 1 },
    { frequency: "5min", row_count: 1, visible_rows: 0 },
  ],
  row_changes: [{ day: "2026-01-30", row_count: 0, previous_rows: null, change_rows: null }],
  rules: [
    {
      rule_id: "freshness",
      name: "更新延迟",
      state: "missing_expected_scope",
      state_label: "缺少应有范围",
      reason: "session_grid_unknown",
      checked_rows: 2,
      issue_count: 0,
      reason_label: "缺少权威交易时段和频率网格，不能用自然时间判断盘中延迟。",
    },
    {
      rule_id: "date_presence",
      name: "交易日记录",
      state: "measured",
      state_label: "已统计",
      reason: "date_presence_only",
      reason_label: "只检查日期记录。",
      checked_rows: 2,
      issue_count: 2,
    },
    {
      rule_id: "required_keys",
      name: "主键空值",
      state: "measured",
      state_label: "已统计",
      reason: "declared_keys",
      reason_label: "按合同主键检查。",
      checked_rows: 2,
      issue_count: 0,
    },
    {
      rule_id: "field_nulls",
      name: "字段空值",
      state: "measured",
      state_label: "已统计",
      reason: "null_counts_only",
      reason_label: "空值统计不代表异常。",
      checked_rows: 2,
      issue_count: 0,
    },
    {
      rule_id: "known_sources",
      name: "记录来源",
      state: "measured",
      state_label: "已统计",
      reason: "declared_sources",
      reason_label: "核对合同来源。",
      checked_rows: 2,
      issue_count: 0,
    },
    {
      rule_id: "known_frequency",
      name: "分钟频率",
      state: "measured",
      state_label: "已统计",
      reason: "declared_frequencies",
      reason_label: "核对支持频率。",
      checked_rows: 2,
      issue_count: 0,
    },
    {
      rule_id: "closed_day_rows",
      name: "休市日记录",
      state: "measured",
      state_label: "已统计",
      reason: "observations_only",
      reason_label: "只检查已有记录。",
      checked_rows: 2,
      issue_count: 0,
    },
    {
      rule_id: "row_count_change",
      name: "行数变化",
      state: "measured",
      state_label: "已统计",
      reason: "observations_only",
      reason_label: "未设置未知阈值。",
      checked_rows: 2,
      issue_count: 0,
    },
    {
      rule_id: "observation_cutoff",
      name: "观察时刻",
      state: "measured",
      state_label: "已统计",
      reason: "observations_only",
      reason_label: "只描述实际写入时间。",
      checked_rows: 2,
      issue_count: 0,
    },
  ],
  conclusion: "issues_observed",
  conclusion_label: "发现问题",
};

it("shows pending and zero observations without calling them healthy", () => {
  render(<DatasetReportContent dataset={dataset} />);
  expect(screen.getByText("完整范围未确认")).toBeInTheDocument();
  expect(screen.getByText("发现问题")).toBeInTheDocument();
  expect(screen.getByText("尚未确认可见")).toBeInTheDocument();
  expect(screen.queryByText("健康")).not.toBeInTheDocument();
  expect(screen.getByRole("table", { name: "数据集月度覆盖" })).toHaveTextContent("0.0%");
  expect(screen.getByText(/^1分钟/)).toBeInTheDocument();
  expect(screen.getByText(/^5分钟/)).toBeInTheDocument();
  expect(screen.queryByText("catalog-dataset-audit-v1")).not.toBeInTheDocument();
});

it("keeps scope explanations accessible by keyboard and details expandable", async () => {
  const user = userEvent.setup();
  render(<DatasetReportContent dataset={dataset} />);
  const explanation = screen.getByRole("button", { name: "来源与范围说明" });
  explanation.focus();
  fireEvent.focus(explanation);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("2026-02-03");
  await user.click(screen.getByText("行数与空值"));
  expect(screen.getByRole("table", { name: "数据集字段空值" })).toHaveTextContent("收盘价");
  expect(screen.getByRole("table", { name: "数据集字段空值" })).toHaveTextContent("50.0%");
});

it("DAUD-FINAL-01 keeps row and null explanations behind focusable tips", async () => {
  const user = userEvent.setup();
  render(<DatasetReportContent dataset={dataset} />);
  await user.click(screen.getByText("行数与空值"));
  expect(screen.queryByText("按已有记录对比，不设未知的异常阈值")).not.toBeInTheDocument();
  expect(screen.queryByText("合同键与代表字段；可选字段没有新设阈值")).not.toBeInTheDocument();
  expect(screen.getByText(/2 条记录在报告时点后写入/)).toBeInTheDocument();
  expect(
    within(screen.getByRole("table", { name: "数据集行数变化" })).getByText("0"),
  ).toBeInTheDocument();
  expect(screen.getByRole("table", { name: "数据集字段空值" })).toHaveTextContent("50.0%");

  const rowTip = screen.getByRole("button", { name: "行数变化说明" });
  await user.click(rowTip);
  expect(rowTip).toHaveFocus();
  expect(await screen.findByText("只比较已有记录，增减不一定是问题。")).toBeInTheDocument();
  const fieldTip = screen.getByRole("button", { name: "字段空值说明" });
  await user.click(fieldTip);
  expect(fieldTip).toHaveFocus();
  expect(
    await screen.findByText("检查必要字段和部分常用字段，空值需结合字段含义判断。"),
  ).toBeInTheDocument();
});

it("separates current reference N/A from unknown visibility and empty data", () => {
  render(
    <DatasetReportContent
      dataset={{
        ...dataset,
        dataset_id: "ths_member",
        name: "板块成分",
        visibility: "unknown",
        scope: "current_snapshot",
        coverage_state: "not_applicable",
        coverage_label: "不适用",
        completeness_state: "not_applicable",
        completeness_label: "不适用",
        freshness_state: "not_evaluated",
        freshness_label: "未评估",
        freshness_reason: "visibility_unknown",
        observed_rows: 0,
        visible_rows: 0,
        latest_visible_date: null,
        latest_visible_time: null,
        latest_ingested_at: null,
        observed_age_seconds: null,
        recorded_after_as_of_rows: 0,
        coverage_reason: "current_snapshot",
        pending_rows: 0,
        expected_open_days: null,
        covered_open_days: null,
        fields: [],
        frequencies: [],
        monthly: [],
        gaps: [],
        row_changes: [],
        conclusion: "not_fully_assessed",
        conclusion_label: "尚未完整检查",
        rules: dataset.rules.map((rule) => ({
          ...rule,
          state: "not_evaluated",
          state_label: "未评估",
          reason: "no_observations",
          reason_label: "没有可检查记录。",
          checked_rows: 0,
          issue_count: 0,
        })),
      }}
    />,
  );
  expect(screen.getByText("当前快照不检查历史逐日覆盖")).toBeInTheDocument();
  expect(screen.getByText("没有可检查记录，不能据此判断健康")).toBeInTheDocument();
  expect(screen.getByText("尚未完整检查")).toBeInTheDocument();
});
