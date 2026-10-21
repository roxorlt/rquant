import { clampShift, shiftAbove } from "./Tip";

it("keeps a tip at least 12px inside the viewport", () => {
  expect(
    clampShift({ left: 0, top: 20, right: 240, bottom: 155 }, { width: 390, height: 844 }, 12),
  ).toEqual({ x: 12, y: 0 });
  expect(
    clampShift({ left: 16, top: 40, right: 256, bottom: 175 }, { width: 390, height: 844 }, 12),
  ).toEqual({ x: 0, y: 0 });
  expect(
    clampShift({ left: 200, top: 800, right: 440, bottom: 935 }, { width: 390, height: 844 }, 12),
  ).toEqual({ x: -62, y: -103 });
});

it("moves a tip above the name field it would cover", () => {
  const tip = { left: 12, top: 280, right: 252, bottom: 415 };
  const name = { left: 16, top: 345, right: 374, bottom: 411 };
  expect(shiftAbove(tip, [name], 8)).toEqual({ x: 0, y: -78 });
});
