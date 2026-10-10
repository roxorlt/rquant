import { useEffect, useState } from "react";

export interface SearchInputProps {
  value: string;
  /** Called after typing pauses (300 ms) and on Enter. */
  onSearch: (value: string) => void;
  placeholder: string;
  label: string;
}

/** A search box that settles before it asks the API. */
export function SearchInput({ value, onSearch, placeholder, label }: SearchInputProps) {
  const [draft, setDraft] = useState(value);
  useEffect(() => setDraft(value), [value]);
  useEffect(() => {
    if (draft === value) {
      return undefined;
    }
    const timer = window.setTimeout(() => onSearch(draft.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [draft, value, onSearch]);
  return (
    <input
      className="inp search-inp"
      type="search"
      value={draft}
      placeholder={placeholder}
      aria-label={label}
      maxLength={32}
      onChange={(event) => setDraft(event.target.value)}
      onKeyDown={(event) => {
        if (event.key === "Enter") {
          onSearch(draft.trim());
        }
      }}
    />
  );
}
