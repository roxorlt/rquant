import { fireEvent, render, screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { CommandPalette } from "./CommandPalette";

function setup() {
  const router = createMemoryRouter([{ path: "*", element: <CommandPalette /> }]);
  render(<RouterProvider router={router} />);
  return router;
}

describe("CommandPalette", () => {
  it("opens on Ctrl+K and jumps to a page", () => {
    const router = setup();
    fireEvent.keyDown(window, { key: "k", ctrlKey: true });
    const input = screen.getByLabelText("跳转到页面或搜索股票");
    fireEvent.change(input, { target: { value: "回测" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(router.state.location.pathname).toBe("/backtest");
  });

  it("puts a code search first for a stock code", () => {
    const router = setup();
    fireEvent.keyDown(window, { key: "/" });
    const input = screen.getByLabelText("跳转到页面或搜索股票");
    fireEvent.change(input, { target: { value: "600519" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(router.state.location.search).toBe("?q=600519");
  });
});
