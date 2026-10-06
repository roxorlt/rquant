import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { ScreenExecutionView, ScreenQueryDefinition } from "@/api/screen";
import * as screenApi from "@/api/screen";
import { AppProviders } from "@/app/App";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { ScreenQueryHistory } from "./ScreenQueryHistory";

const scope = "a".repeat(64);
const definition: ScreenQueryDefinition = { schema_version: 1, description: "旧的完整选股", mode: "daily", trade_date: "2026-09-30", cutoff: null, source_kind: "replica", source_identity: "b".repeat(64), conditions: [{ name: "above_ma", args: { period: 37, offset: 2 } }], ranking: { top_n: 13, conditions: [{ metric: "CIRC_MV[0]", weight: 100, ascending: true }] } };
const entry: ScreenExecutionView = { execution_id: "old-command", sequence: 1, command_hash: "c".repeat(64), plan_hash: "d".repeat(64), original_command: { kind: "execute_screen_query", command_id: "old-command", requested_at: "2026-10-01T01:00:00Z", page_size: 37, definition }, definition, started_at: "2026-10-01T01:00:01Z", completed_at: null, status: "processing", base_count: null, total: null, unknown_count: null, ranked_count: null, steps: [], artifact_sha256: null, member_rank_sha256: null, failure_code: null };

it("换设备读取服务端记录并回填完整条件；未知数量显示待确认", async () => {
  server.use(
    http.get("*/api/v1/screen/query/history", () => HttpResponse.json({ available: true, owner_scope_tag: scope, history: { owner_scope_tag: scope, items: [entry], next_cursor: null }, presets: [] })),
    http.get("*/api/v1/screen/query/executions/old-command", () => HttpResponse.json({ available: true, owner_scope_tag: scope, execution: entry, presets: [] })),
  );
  const restored = vi.fn();
  const recovered = vi.fn();
  render(<AppProviders queryClient={testQueryClient()}><ScreenQueryHistory viewer="alice" open onClose={() => undefined} onRestore={restored} onInspect={() => undefined} onRecover={recovered} /></AppProviders>);
  expect(await screen.findByText("旧的完整选股")).toBeVisible();
  expect(screen.getByText("结果待确认")).toBeVisible();
  expect(screen.queryByText("命中 0 只")).toBeNull();
  await userEvent.click(screen.getByRole("button", { name: "回填条件" }));
  await waitFor(() => expect(restored).toHaveBeenCalledWith(definition));
  await userEvent.click(screen.getByRole("button", { name: "恢复原请求" }));
  await waitFor(() => expect(recovered).toHaveBeenCalledWith({ action: "execute", command: entry.original_command }));
});

it("按固定分页游标读取更早历史，不使用本地最近五句", async () => {
  window.sessionStorage.setItem("rquant.screen.recent-descriptions.v1", JSON.stringify(["别人的旧句子"]));
  const seen: string[] = [];
  server.use(http.get("*/api/v1/screen/query/history", ({ request }) => {
    const cursor = new URL(request.url).searchParams.get("cursor") ?? "";
    seen.push(cursor);
    return HttpResponse.json({ available: true, owner_scope_tag: scope, history: { owner_scope_tag: scope, items: [{ ...entry, execution_id: cursor ? "older" : "newest", definition: { ...definition, description: cursor ? "更早一条" : "最新一条" } }], next_cursor: cursor ? null : "signed-fixed-cursor" }, presets: [] });
  }));
  render(<AppProviders queryClient={testQueryClient()}><ScreenQueryHistory viewer="alice" open onClose={() => undefined} onRestore={() => undefined} onInspect={() => undefined} onRecover={() => undefined} /></AppProviders>);
  await screen.findByText("最新一条");
  await userEvent.click(screen.getByRole("button", { name: "更早记录" }));
  expect(await screen.findByText("更早一条")).toBeVisible();
  expect(seen).toEqual(["", "signed-fixed-cursor"]);
  expect(screen.queryByText("别人的旧句子")).toBeNull();
});

it("离开本人历史后忽略迟到的详情回复",async()=>{
  server.use(http.get("*/api/v1/screen/query/history",()=>HttpResponse.json({available:true,owner_scope_tag:scope,history:{owner_scope_tag:scope,items:[entry],next_cursor:null},presets:[]})));
  let release!: (data: screenApi.ScreenQueryReadData)=>void;
  const response=new Promise<screenApi.ScreenQueryReadData>((resolve)=>{release=resolve;});
  const fetch=vi.spyOn(screenApi,"fetchScreenExecution").mockImplementation(()=>response);
  const restored=vi.fn();
  const view=render(<AppProviders queryClient={testQueryClient()}><ScreenQueryHistory viewer="alice" open onClose={()=>undefined} onRestore={restored} onInspect={()=>undefined} onRecover={()=>undefined}/></AppProviders>);
  await screen.findByText("旧的完整选股");
  await userEvent.click(screen.getByRole("button",{name:"回填条件"}));
  expect(fetch).toHaveBeenCalledOnce();
  view.unmount();
  await act(async()=>{release({available:true,owner_scope_tag:scope,execution:entry,presets:[],daily_run_evidence:[]});await response;});
  expect(restored).not.toHaveBeenCalled();
  fetch.mockRestore();
});
