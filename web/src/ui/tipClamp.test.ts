import { clampShift, pinBeside, shiftAbove } from "./Tip";

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

it("pins a scope tip against the label and inside the viewport", () => {
  const anchor = { left: 16, top: 425, right: 48, bottom: 445 };
  const parked = { left: 0, top: 137, right: 240, bottom: 272 };
  const shift = pinBeside(parked, anchor, { width: 390, height: 844 }, 12, 4);
  expect(shift).toEqual({ x: 12, y: 149 });
  const placed = {
    left: parked.left + shift.x,
    top: parked.top + shift.y,
    right: parked.right + shift.x,
    bottom: parked.bottom + shift.y,
  };
  expect(anchor.top - placed.bottom).toBe(4);
  expect(placed.left).toBeGreaterThanOrEqual(12);
  expect(placed.right).toBeLessThanOrEqual(390 - 12);
});

it("moves a tip above the name field it would cover", () => {
  const tip = { left: 12, top: 280, right: 252, bottom: 415 };
  const name = { left: 16, top: 345, right: 374, bottom: 411 };
  expect(shiftAbove(tip, [name], 8)).toEqual({ x: 0, y: -78 });
});
