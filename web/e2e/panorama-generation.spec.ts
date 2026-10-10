import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import { REPLAY_ROOT, REPO_ROOT, SERVING_ROOT, UV_RUN } from "./env.ts";

test("全景页在新数据代发布后 20 秒内刷新", async ({ page }) => {
  test.skip(Boolean(REPLAY_ROOT), "回放副本只读，不能向其中发布下一代");
  const pulseGenerations = new Set<string>();
  page.on("response", (response) => {
    if (response.url().includes("/api/v1/panorama/pulse") && response.status() === 200) {
      const generation = response.headers()["x-rquant-generation"];
      if (generation) {
        pulseGenerations.add(generation);
      }
    }
  });
  await page.goto("./#/panorama");
  await expect(page.getByRole("region", { name: "市场脉搏" })).toContainText("涨停");
  const marker = page.locator(".gen-tag");
  const before = await marker.getAttribute("data-generation");
  expect(before).toMatch(/^[0-9a-f]{12}$/);

  const output = execSync(
    `${UV_RUN} python scripts/build_web_fixture.py --out "${SERVING_ROOT}" --scenario panorama --publish-next`,
    { cwd: REPO_ROOT, env: { ...process.env, RQUANT_DISABLE_DOTENV: "1" }, encoding: "utf8" },
  );
  const published = JSON.parse(output.trim().split("\n").pop() ?? "{}") as {
    generation_id: string;
  };
  expect(published.generation_id.slice(0, 12)).not.toBe(before);

  await expect
    .poll(
      async () =>
        (await marker.getAttribute("data-generation")) === published.generation_id.slice(0, 12) &&
        pulseGenerations.has(published.generation_id),
      { timeout: 20_000 },
    )
    .toBe(true);
});
