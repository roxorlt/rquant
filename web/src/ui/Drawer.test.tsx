import { render } from "@testing-library/react";
import type { ReactNode } from "react";
import { SideDrawer } from "./Drawer";

const drawer = vi.hoisted(() => ({
  afterOpenChange: undefined as ((open: boolean) => void) | undefined,
}));
vi.mock("antd", () => ({
  Drawer: (props: { afterOpenChange?: (open: boolean) => void; children: ReactNode }) => {
    drawer.afterOpenChange = props.afterOpenChange;
    return <div>{props.children}</div>;
  },
}));

it("forwards the optional original drawer lifecycle without creating a focus policy", () => {
  const observed = vi.fn();
  const view = render(
    <SideDrawer open onClose={() => undefined} afterOpenChange={observed} title="局部抽屉">
      正文
    </SideDrawer>,
  );
  expect(drawer.afterOpenChange).toBe(observed);
  drawer.afterOpenChange?.(true);
  view.rerender(
    <SideDrawer open={false} onClose={() => undefined} afterOpenChange={observed} title="局部抽屉">
      正文
    </SideDrawer>,
  );
  drawer.afterOpenChange?.(false);
  expect(observed.mock.calls).toEqual([[true], [false]]);
  view.rerender(
    <SideDrawer open={false} onClose={() => undefined} title="原页面">
      正文
    </SideDrawer>,
  );
  expect(drawer.afterOpenChange).toBeUndefined();
});
