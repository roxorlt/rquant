import { BottomSheet } from "@/ui";
import { NavList } from "./NavList";

export const PHONE_NAV_ID = "phone-nav";

/** Bottom navigation sheet on phones (≤ 760 px), opened from the top bar. */
export function PhoneNav({ open, onClose }: { open: boolean; onClose: () => void }) {
  return (
    <BottomSheet open={open} onClose={onClose} title="页面">
      <nav className="sheet-nav" id={PHONE_NAV_ID} aria-label="页面导航">
        <NavList onNavigate={onClose} />
      </nav>
    </BottomSheet>
  );
}
