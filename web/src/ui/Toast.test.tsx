import { act, fireEvent, render, screen } from "@testing-library/react";
import { ToastHost, dismissToast, pauseToast, resumeToast, showToast } from "./Toast";

afterEach(() => {
  dismissToast();
  vi.useRealTimers();
});

it("keeps only the latest toast and drops undo after 6 seconds", () => {
  vi.useFakeTimers();
  render(<ToastHost />);
  act(() => {
    showToast({ text: "已确认 1 条告警", duration: 6000, action: { label: "撤销", onClick: () => undefined } });
    showToast({ text: "已确认另一条", duration: 6000, action: { label: "撤销", onClick: () => undefined } });
  });
  expect(screen.getAllByRole("status")).toHaveLength(1);
  expect(screen.getByRole("status")).toHaveTextContent("已确认另一条");
  expect(screen.getByRole("button", { name: "撤销" })).toBeInTheDocument();
  act(() => {
    vi.advanceTimersByTime(6000);
  });
  expect(screen.queryByRole("button", { name: "撤销" })).not.toBeInTheDocument();
});

it("pauses the undo timer while the toast is hovered", () => {
  vi.useFakeTimers();
  render(<ToastHost />);
  act(() => {
    showToast({ text: "已确认 1 条告警", duration: 6000, action: { label: "撤销", onClick: () => undefined } });
  });
  fireEvent.mouseEnter(screen.getByRole("status"));
  act(() => {
    vi.advanceTimersByTime(6000);
  });
  expect(screen.getByRole("button", { name: "撤销" })).toBeInTheDocument();
  fireEvent.mouseLeave(screen.getByRole("status"));
  act(() => {
    vi.advanceTimersByTime(6000);
  });
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
});

it("exposes pause and resume for focus", () => {
  vi.useFakeTimers();
  render(<ToastHost />);
  act(() => showToast({ text: "已撤销确认", duration: 4000 }));
  act(() => pauseToast());
  act(() => {
    vi.advanceTimersByTime(4000);
  });
  expect(screen.getByText("已撤销确认")).toBeInTheDocument();
  act(() => resumeToast());
  act(() => {
    vi.advanceTimersByTime(4000);
  });
  expect(screen.queryByText("已撤销确认")).not.toBeInTheDocument();
});
