import { screenerSchemesEnabled } from "./features";

afterEach(() => {
  vi.unstubAllEnvs();
});

it("stays off unless the Vite env is 1 or true", () => {
  expect(screenerSchemesEnabled()).toBe(false);

  vi.stubEnv("VITE_FEATURE_SCREENER_SCHEMES", "1");
  expect(screenerSchemesEnabled()).toBe(true);

  vi.stubEnv("VITE_FEATURE_SCREENER_SCHEMES", "0");
  expect(screenerSchemesEnabled()).toBe(false);
});
