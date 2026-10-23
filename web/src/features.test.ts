import { alertUnackEnabled, screenerSchemesEnabled } from "./features";

afterEach(() => {
  vi.unstubAllEnvs();
});

it("stays off unless the Vite env is 1 or true", () => {
  expect(screenerSchemesEnabled()).toBe(false);
  expect(alertUnackEnabled()).toBe(false);

  vi.stubEnv("VITE_FEATURE_SCREENER_SCHEMES", "1");
  vi.stubEnv("VITE_FEATURE_ALERT_UNACK", "true");
  expect(screenerSchemesEnabled()).toBe(true);
  expect(alertUnackEnabled()).toBe(true);

  vi.stubEnv("VITE_FEATURE_SCREENER_SCHEMES", "0");
  vi.stubEnv("VITE_FEATURE_ALERT_UNACK", "");
  expect(screenerSchemesEnabled()).toBe(false);
  expect(alertUnackEnabled()).toBe(false);
});
