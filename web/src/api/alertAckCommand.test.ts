import { HttpResponse, http } from "msw";
import { server } from "@/test/server";
import { AckNoEffectError, submitAlertAckCommand } from "./alertAckCommand";
import type { ApiError, Schemas } from "./client";

const body: Schemas["AckCommandRequest"] = {
  command_id: "ack-web-test",
  requested_at: "2026-09-28T07:00:00.000Z",
  generation_id: "a".repeat(64),
  alert_id: "b".repeat(64),
};

it("posts only the original typed fields with the same-origin CSRF header", async () => {
  let sent: unknown;
  let csrf: string | null = null;
  const timeout = vi.spyOn(AbortSignal, "timeout");
  server.use(
    http.post("*/api/v1/monitor/ack", async ({ request }) => {
      sent = await request.json();
      csrf = request.headers.get("X-Rquant-Csrf");
      return HttpResponse.json({
        command_id: body.command_id,
        status: "succeeded",
        confirmation_id: "first",
        message: "已受理，正在同步",
      });
    }),
  );
  const receipt = await submitAlertAckCommand(body);
  expect(sent).toEqual(body);
  expect(csrf).toBe("1");
  expect(timeout).toHaveBeenCalledWith(12_000);
  expect(receipt.confirmation_id).toBe("first");
});

it("preserves rejection status and treats a lost connection as uncertain", async () => {
  server.use(
    http.post("*/api/v1/monitor/ack", () =>
      HttpResponse.json({ detail: "已换代" }, { status: 409 }),
    ),
  );
  await expect(submitAlertAckCommand(body)).rejects.toMatchObject({
    name: "ApiError",
    status: 409,
  } satisfies Partial<ApiError>);
  server.use(
    http.post("*/api/v1/monitor/ack", () =>
      HttpResponse.json(
        { detail: "数据已更新", code: "stale_generation_no_effect" },
        { status: 409 },
      ),
    ),
  );
  await expect(submitAlertAckCommand(body)).rejects.toBeInstanceOf(AckNoEffectError);
  server.use(http.post("*/api/v1/monitor/ack", () => Response.error()));
  await expect(submitAlertAckCommand(body)).rejects.toMatchObject({
    name: "ApiError",
    status: 503,
  } satisfies Partial<ApiError>);
});
