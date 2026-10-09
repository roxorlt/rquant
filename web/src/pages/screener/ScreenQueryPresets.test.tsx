import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type {
  ScreenPresetSaveRequest,
  ScreenQueryDefinition,
  ScreenQueryPreset,
  ScreenQueryReadData,
} from "@/api/screen";
import { AppProviders } from "@/app/App";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { ScreenQueryPresets } from "./ScreenQueryPresets";

const definition: ScreenQueryDefinition = {
  schema_version: 1,
  description: "完整条件",
  mode: "daily",
  trade_date: "2026-09-30",
  cutoff: null,
  source_kind: "replica",
  source_identity: "a".repeat(64),
  conditions: [{ name: "above_ma", args: { period: 37, offset: 0 } }],
  ranking: { top_n: 17, conditions: [{ metric: "CIRC_MV[0]", ascending: true, weight: 100 }] },
};
const preset: ScreenQueryPreset = {
  preset_id: "preset-1",
  name: "常用一",
  definition,
  version: 3,
  updated_at: "2026-10-05T01:00:00Z",
  command_hash: "b".repeat(64),
};

it("回填所有条件与排名；改名保存带原版本，确认前不写入", async () => {
  server.use(
    http.get("*/api/v1/screen/query/presets", () =>
      HttpResponse.json({ available: true, owner_scope_tag: "c".repeat(64), presets: [preset] }),
    ),
  );
  const save = vi.fn<(request: ScreenPresetSaveRequest) => Promise<ScreenQueryReadData | null>>(
    async () => null,
  );
  const restore = vi.fn();
  render(
    <AppProviders queryClient={testQueryClient()}>
      <ScreenQueryPresets
        viewer="alice"
        open
        definition={definition}
        busy={false}
        onClose={() => undefined}
        onRestore={restore}
        onSave={save}
      />
    </AppProviders>,
  );
  await screen.findByText("常用一");
  await userEvent.click(screen.getByRole("button", { name: "回填常用一" }));
  expect(restore).toHaveBeenCalledWith(definition);
  await userEvent.click(screen.getByRole("button", { name: "改名常用一" }));
  const name = screen.getByRole("textbox", { name: "条件名称" });
  await userEvent.clear(name);
  await userEvent.type(name, "新的名字");
  await userEvent.click(screen.getByRole("button", { name: "保存条件" }));
  expect(save).not.toHaveBeenCalled();
  await userEvent.click(screen.getByRole("button", { name: "确认保存" }));
  await waitFor(() => expect(save).toHaveBeenCalledOnce());
  expect(save.mock.calls[0]?.[0]).toMatchObject({
    expected_version: 3,
    preset: { preset_id: "preset-1", name: "新的名字", definition },
  });
  expect(await screen.findByText("保存待确认，请核对原请求。")).toBeVisible();
});
