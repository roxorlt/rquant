import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import {
  PriceAlertRuntimeFacts,
  priceText,
  usePriceAlertRuntimeFacts,
} from "./PriceAlertRuntimeFacts";

const prefix = "*/api/v1/monitor/price-rules";
const closeLifecycle = vi.hoisted(() => ({ release: (): void => undefined }));
vi.mock("@/ui", async (importOriginal) => {
  const original = await importOriginal<typeof import("@/ui")>();
  function DeferredConfirm(
    props: Parameters<typeof original.ConfirmDialog>[0] & { afterClose?: () => void },
  ) {
    const { afterClose, ...rest } = props;
    if (!props.open && afterClose) closeLifecycle.release = afterClose;
    return <original.ConfirmDialog {...rest} />;
  }
  return { ...original, ConfirmDialog: DeferredConfirm };
});
function runtime(): Schemas["Envelope_PriceAlertRuntimeData_"] {
  const serving = metaEnvelope().serving;
  return {
    serving,
    data: {
      availability: "ready",
      generation_id: serving.generation_id,
      status: "attention",
      status_label: "注意",
      message: "当前仅记录提醒。",
      evaluated_at: "2026-09-24T07:31:00Z",
      quote_updated_at: "2026-09-24T07:31:00Z",
      applied_at: "2026-09-24T07:31:00Z",
      mode: "record_only",
      items: [
        {
          rule_id: "r",
          version: 1,
          membership_version: 1,
          status: "attention",
          status_label: "注意",
          message: "当前仅记录提醒。",
          evaluated_at: "2026-09-24T07:31:00Z",
          last_triggered_at: "2026-09-24T07:30:59Z",
          next_allowed_at: "2026-09-24T07:31:59Z",
          state: "triggered",
        },
      ],
    },
  };
}
beforeEach(() => {
  server.use(
    http.get(prefix, () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          message: "",
          can_write: false,
          write_message: "",
          members: [],
          priority_options: [],
          items: [],
        },
      }),
    ),
    http.get(`${prefix}/runtime`, () => HttpResponse.json(runtime())),
    http.get(`${prefix}/events`, () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          availability: "ready",
          generation_id: metaEnvelope().serving.generation_id,
          message: "",
          items: [
            {
              event_id: "a".repeat(64),
              rule_id: "r",
              rule_version: 1,
              membership_version: 1,
              rule_name: "到价提醒",
              ts_code: "600000.SH",
              comparison: "gte",
              threshold: "10.000000000000000000001",
              price: "10.000000000000000000002",
              triggered_at: "2026-09-24T07:31:00Z",
              route_message: "",
              notifications: [
                {
                  channel: "pushdeer",
                  state: "admitted",
                  label: "已准入发送",
                  message: "可能仍会发送；尚无通知结果。",
                  updated_at: "2026-09-24T07:31:00Z",
                },
              ],
            },
          ],
        },
      }),
    ),
  );
});
it("shows separate honest notification facts and hides internal identities", async () => {
  renderApp("/monitor");
  expect(await screen.findByRole("region", { name: "最近到价提醒" })).toBeInTheDocument();
  expect(await screen.findByText("已准入发送")).toBeInTheDocument();
  expect(screen.getByText("10.00")).toBeInTheDocument();
  expect(screen.queryByText("已送达")).toBeNull();
  expect(document.body.textContent).not.toContain("a".repeat(64));
});
it("uses exact two-decimal display without losing the original text", () => {
  expect(priceText("1.005")).toBe("1.01");
  expect(priceText("1.004999999999999999999")).toBe("1.00");
  expect(priceText("999.999")).toBe("1,000.00");
});
it("retires current facts after a 409 instead of showing old healthy stats", async () => {
  server.use(
    http.get(`${prefix}/runtime`, () => HttpResponse.json({ detail: "同步中" }, { status: 409 })),
    http.get(`${prefix}/events`, () => HttpResponse.json({ detail: "同步中" }, { status: 409 })),
  );
  renderApp("/monitor");
  expect(await screen.findByText("设置已更新，等待运行端同步。")).toBeInTheDocument();
  expect(screen.queryByText("已准入发送")).toBeNull();
});

function Harness({ owner, generation }: { owner: string | null; generation: string | null }) {
  const facts = usePriceAlertRuntimeFacts(owner, generation);
  return <PriceAlertRuntimeFacts facts={facts} />;
}
it.each(["owner", "generation"] as const)(
  "discards a delayed response after the %s changes",
  async (change) => {
    let release: () => void = () => {};
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    let requested = 0;
    const initial = metaEnvelope().serving.generation_id;
    const next = change === "generation" ? "b".repeat(64) : initial;
    server.use(
      http.get(`${prefix}/runtime`, async () => {
        const first = ++requested === 1;
        if (first) await pending;
        const response = runtime();
        response.serving.generation_id = first ? initial : next;
        response.data.generation_id = first ? initial : next;
        response.data.status_label = first ? "正常" : "注意";
        response.data.status = first ? "normal" : "attention";
        response.data.message = first ? "旧账号的运行记录。" : "当前账号的运行记录。";
        return HttpResponse.json(response);
      }),
      http.get(`${prefix}/events`, () =>
        HttpResponse.json({
          serving: { ...metaEnvelope().serving, generation_id: requested > 1 ? next : initial },
          data: {
            availability: "ready",
            generation_id: requested > 1 ? next : initial,
            message: "",
            items: [],
          },
        }),
      ),
    );
    const queryClient = testQueryClient();
    const { rerender } = render(
      <AppProviders queryClient={queryClient}>
        <Harness owner="alice" generation={initial} />
      </AppProviders>,
    );
    await waitFor(() => expect(requested).toBe(1));
    rerender(
      <AppProviders queryClient={queryClient}>
        <Harness owner={change === "owner" ? "bob" : "alice"} generation={next} />
      </AppProviders>,
    );
    await waitFor(() => expect(requested).toBe(2));
    expect(await screen.findByText("注意")).toBeInTheDocument();
    await act(async () => {
      release();
      await pending;
    });
    await waitFor(() => expect(queryClient.isFetching()).toBe(0));
    expect(screen.getByText("注意")).toBeInTheDocument();
    expect(screen.queryByText("正常")).toBeNull();
    expect(document.body.textContent).not.toContain("旧账号");
  },
);

function editableListing() {
  server.use(
    http.get(prefix, () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          message: "",
          can_write: true,
          write_message: "",
          members: [{ ts_code: "600000.SH", version: 1, expires_at: null }],
          priority_options: [{ value: "P2", label: "普通" }],
          items: [
            {
              rule_id: "r",
              version: 1,
              membership_version: 1,
              ts_code: "600000.SH",
              name: "到价提醒",
              priority: "P2",
              priority_label: "普通",
              enabled: true,
              comparison: "gte",
              threshold: "10.123456",
              valid_from: "09:30:00",
              valid_until: "14:57:00",
              updated_at: "2026-09-24T07:31:00Z",
              scope_status: "bound",
              status_label: "未运行",
              scope_message: "等待检查。",
            },
          ],
        },
      }),
    ),
  );
}

it("closes the drawer after the confirmation lifecycle, then restores the original focus", async () => {
  editableListing();
  renderApp("/monitor");
  await screen.findByRole("button", { name: "编辑 到价提醒" });
  const edit = screen.getByRole("button", { name: "编辑 到价提醒" });
  fireEvent.click(edit);
  fireEvent.change(await screen.findByLabelText("规则名称"), { target: { value: "未保存的草稿" } });
  fireEvent.click(screen.getByRole("button", { name: "取消编辑" }));
  await screen.findByRole("button", { name: "放弃修改" });
  fireEvent.click(screen.getByRole("button", { name: "放弃修改" }));
  expect(screen.getByLabelText("规则名称")).toHaveValue("未保存的草稿");
  act(() => closeLifecycle.release());
  await waitFor(() => expect(screen.queryByLabelText("规则名称")).toBeNull());
  await waitFor(() => expect(screen.getByRole("button", { name: "编辑 到价提醒" })).toHaveFocus());
});

it("a delayed confirmation close cannot discard the next owner's new draft", async () => {
  editableListing();
  const app = renderApp("/monitor");
  await screen.findByRole("button", { name: "编辑 到价提醒" });
  fireEvent.click(screen.getByRole("button", { name: "编辑 到价提醒" }));
  fireEvent.change(await screen.findByLabelText("规则名称"), { target: { value: "旧账号的草稿" } });
  fireEvent.click(screen.getByRole("button", { name: "取消编辑" }));
  await screen.findByRole("button", { name: "放弃修改" });
  fireEvent.click(screen.getByRole("button", { name: "放弃修改" }));
  expect(screen.getByLabelText("规则名称")).toHaveValue("旧账号的草稿");
  const release = closeLifecycle.release;
  server.use(metaHandler(metaEnvelope({ viewer: "bob" })));
  act(() => app.queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "bob" })));
  await waitFor(() => expect(screen.queryByLabelText("规则名称")).toBeNull());
  await waitFor(() => expect(screen.getByRole("button", { name: "新建规则" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
  fireEvent.change(await screen.findByLabelText("规则名称"), { target: { value: "新账号的草稿" } });
  act(() => release());
  expect(screen.getByLabelText("规则名称")).toHaveValue("新账号的草稿");
});
