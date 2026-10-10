import { App as AntApp } from "antd";
import { useCallback } from "react";

/** A short, non-blocking notice (the prototype's toast). */
export function useToast(): (text: string) => void {
  const { message } = AntApp.useApp();
  return useCallback(
    (text: string) => {
      void message.open({ type: "info", content: text, duration: 2.8 });
    },
    [message],
  );
}
