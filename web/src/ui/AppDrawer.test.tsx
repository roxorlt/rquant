import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { AppDrawer } from "./AppDrawer";

function Harness() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        打开日志
      </button>
      <AppDrawer open={open} onClose={() => setOpen(false)} title="每日行情采集" label="日志">
        <pre className="logview">一行日志</pre>
      </AppDrawer>
    </>
  );
}

it("closes from the button, the mask, and Escape, then returns focus", () => {
  render(<Harness />);
  const open = screen.getByRole("button", { name: "打开日志" });
  open.focus();
  fireEvent.click(open);
  expect(screen.getByRole("dialog", { name: "每日行情采集" })).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "关闭" }));
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(open).toHaveFocus();

  fireEvent.click(open);
  fireEvent.click(screen.getByRole("button", { name: "关闭遮罩" }));
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

  fireEvent.click(open);
  fireEvent.keyDown(window, { key: "Escape" });
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
});

it("starts on the title and keeps Tab inside the drawer", () => {
  render(<Harness />);
  const open = screen.getByRole("button", { name: "打开日志" });
  open.focus();
  fireEvent.click(open);
  const title = screen.getByRole("heading", { name: "每日行情采集" });
  expect(title).toHaveFocus();
  const close = screen.getByRole("button", { name: "关闭" });
  close.focus();
  fireEvent.keyDown(window, { key: "Tab" });
  expect(close).toHaveFocus();
  expect(document.body).not.toHaveFocus();
  fireEvent.keyDown(window, { key: "Escape" });
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(open).toHaveFocus();
});

it("closes when the grabber is swiped down", () => {
  render(<Harness />);
  fireEvent.click(screen.getByRole("button", { name: "打开日志" }));
  const grab = document.querySelector(".drawer-grab");
  expect(grab).not.toBeNull();
  fireEvent.pointerDown(grab as Element, { clientY: 10 });
  fireEvent.pointerUp(grab as Element, { clientY: 400 });
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
});
