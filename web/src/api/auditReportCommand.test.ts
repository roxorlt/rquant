import { HttpResponse, http } from "msw";
import { server } from "@/test/server";
import { submitAuditReportCommand } from "./auditReportCommand";
import type { ApiError, Schemas } from "./client";

const body: Schemas["AuditReportCommandRequest"] = {
  command_id: "audit-web-test",
  requested_at: "2026-09-28T07:00:00.000Z",
  audit_start: "2024-09-01",
  observed_through: "2025-04-30",
};

it("posts the four generated-contract fields with CSRF and a bounded timeout", async () => {
  let sent: unknown = null;
  let csrf: string | null = null;
  const timeout = vi.spyOn(AbortSignal, "timeout");
  server.use(
    http.post("*/api/v1/data/audit-report/commands", async ({ request }) => {
      sent = await request.json();
      csrf = request.headers.get("X-Rquant-Csrf");
      return HttpResponse.json({
        command_id: body.command_id,
        status: "queued",
        task_id: "a".repeat(32),
        message: "已排队",
      });
    }),
  );

  const receipt = await submitAuditReportCommand(body);

  expect(sent).toEqual(body);
  expect(Object.keys(sent as Record<string, unknown>).sort()).toEqual([
    "audit_start",
    "command_id",
    "observed_through",
    "requested_at",
  ]);
  expect(csrf).toBe("1");
  expect(timeout).toHaveBeenCalledWith(12_000);
  expect(receipt.status).toBe("queued");
});

it("preserves an HTTP rejection status for the session to classify", async () => {
  server.use(
    http.post("*/api/v1/data/audit-report/commands", () =>
      HttpResponse.json({ detail: "日期不合规" }, { status: 422 }),
    ),
  );

  await expect(submitAuditReportCommand(body)).rejects.toMatchObject({
    name: "ApiError",
    status: 422,
  } satisfies Partial<ApiError>);
});

it("classifies an unavailable connection as uncertain", async () => {
  server.use(http.post("*/api/v1/data/audit-report/commands", () => Response.error()));

  await expect(submitAuditReportCommand(body)).rejects.toMatchObject({
    name: "ApiError",
    status: 503,
  } satisfies Partial<ApiError>);
});
