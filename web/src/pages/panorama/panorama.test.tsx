import { screen } from "@testing-library/react";
import { renderApp } from "@/test/render";

describe("市场全景", () => {
  it("提供市场全景和爆量记录两个可切换的页面", async () => {
    renderApp("/panorama");
    expect(await screen.findByRole("tab", { name: "市场全景" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "爆量记录" })).toBeInTheDocument();
  });
});
