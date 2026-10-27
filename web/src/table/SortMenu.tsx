import { DropdownMenu } from "@/ui/DropdownMenu";

/** Phone title-row control. The visible label stays 「排序」. */
export function SortMenu({
  value,
  options,
  onChange,
}: {
  value: string;
  options: readonly { value: string; label: string }[];
  onChange: (value: string) => void;
}) {
  return (
    <DropdownMenu
      items={options.map((option) => ({
        key: option.value,
        label: option.label,
        checked: option.value === value,
        onSelect: () => onChange(option.value),
      }))}
    >
      <button type="button" className="btn ghost sm">
        排序
      </button>
    </DropdownMenu>
  );
}
