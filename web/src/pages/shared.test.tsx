import { act, render, screen } from "@testing-library/react";
import type { ServingQueryResult } from "@/api/useServingQuery";
import { QueryView } from "./shared";

function loadingQuery(loading: boolean): ServingQueryResult<{ n: number }> {
  return {
    data: loading ? undefined : { n: 1 },
    serving: undefined,
    isLoading: loading,
    isFetching: loading,
    error: null,
    refetch: () => undefined,
  };
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
});

afterEach(() => {
  vi.useRealTimers();
});

it("shows the skeleton only after loading lasts past 300ms", () => {
  const view = (loading: boolean) => (
    <QueryView query={loadingQuery(loading)}>{(data) => <div>内容 {data.n}</div>}</QueryView>
  );
  const { rerender } = render(view(true));

  act(() => {
    vi.advanceTimersByTime(299);
  });
  expect(screen.queryByRole("status", { name: "正在加载" })).not.toBeInTheDocument();

  act(() => {
    vi.advanceTimersByTime(2);
  });
  expect(screen.getByRole("status", { name: "正在加载" })).toBeInTheDocument();

  rerender(view(false));
  expect(screen.queryByRole("status", { name: "正在加载" })).not.toBeInTheDocument();
  expect(screen.getByText("内容 1")).toBeInTheDocument();

  rerender(view(true));
  act(() => {
    vi.advanceTimersByTime(200);
  });
  rerender(view(false));
  act(() => {
    vi.advanceTimersByTime(400);
  });
  expect(screen.queryByRole("status", { name: "正在加载" })).not.toBeInTheDocument();
});
