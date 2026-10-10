import { DatePicker as AntDatePicker } from "antd";
import dayjs, { type Dayjs } from "dayjs";

export interface DatePickerProps {
  /** "YYYY-MM-DD". */
  value: string | null;
  onChange: (value: string) => void;
  /** Only these days can be picked ("YYYY-MM-DD"); every past day when omitted. */
  allowed?: readonly string[];
  label: string;
}

/** A single-day picker in the Shanghai calendar; unlisted days are disabled. */
export function DatePicker({ value, onChange, allowed, label }: DatePickerProps) {
  const allowedSet = allowed ? new Set(allowed) : null;
  return (
    <AntDatePicker
      aria-label={label}
      value={value ? dayjs(value) : null}
      allowClear={false}
      inputReadOnly
      format="YYYY-MM-DD"
      disabledDate={(day: Dayjs) =>
        allowedSet ? !allowedSet.has(day.format("YYYY-MM-DD")) : day.isAfter(dayjs(), "day")
      }
      onChange={(day: Dayjs | null) => {
        if (day) {
          onChange(day.format("YYYY-MM-DD"));
        }
      }}
    />
  );
}
