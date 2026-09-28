import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { FormulaPoolDetail } from "./FormulaPoolDetail";

const pool: Schemas["FormulaPoolItem"] = {
  pool_name: "user/趋势池",
  display_name: "趋势池",
  formula: "CLOSE>MA(CLOSE,2)",
  syntax_version: "tdx-v1",
  created_at: "2026-09-24T07:31:00Z",
  status_label: "已有结果",
  version: "f".repeat(64),
  latest_result: {
    trade_date: "2026-09-24",
    market_total: 100,
    match_count: 51,
    no_match_count: 49,
    unknown_count: 0,
    unknown_reasons: [],
  },
};

it("数据代切换后舍弃旧页游标与成员，从新代第一页读取", async () => {
  const first = metaEnvelope().serving.generation_id;
  const second = "b".repeat(64);
  let current = first;
  const cursors: (string | null)[] = [];
  server.use(
    http.get("*/api/v1/pools/formula/*/members", ({ request }) => {
      const cursor = new URL(request.url).searchParams.get("cursor");
      cursors.push(cursor);
      return HttpResponse.json({
        data: {
          pool_name: pool.pool_name,
          trade_date: "2026-09-24",
          total: 51,
          offset: cursor ? 50 : 0,
          match_codes: [current === second ? "600002.SH" : cursor ? "600051.SH" : "600001.SH"],
          next_cursor: cursor ? null : "second-page",
        },
        serving: { ...metaEnvelope().serving, generation_id: current },
      });
    }),
  );
  const user = userEvent.setup();
  const queryClient = testQueryClient();
  const view = render(
    <AppProviders queryClient={queryClient}>
      <FormulaPoolDetail pool={pool} generation={first} onSelectStock={() => undefined} />
    </AppProviders>,
  );
  const members = await screen.findByRole("region", { name: "公式池成员" });
  expect(await within(members).findByRole("button", { name: /600001.SH/ })).toBeVisible();
  await user.click(within(members).getByRole("button", { name: "下一页" }));
  expect(await within(members).findByRole("button", { name: /600051.SH/ })).toBeVisible();
  current = second;
  view.rerender(
    <AppProviders queryClient={queryClient}>
      <FormulaPoolDetail pool={pool} generation={second} onSelectStock={() => undefined} />
    </AppProviders>,
  );
  expect(within(members).queryByRole("button", { name: /600051.SH/ })).toBeNull();
  expect(await within(members).findByRole("button", { name: /600002.SH/ })).toBeVisible();
  expect(within(members).getByRole("button", { name: "上一页" })).toBeDisabled();
  expect(cursors).toEqual([null, "second-page", null]);
});
