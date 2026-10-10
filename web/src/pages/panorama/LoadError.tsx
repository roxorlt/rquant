import { Button } from "@/ui";

export function LoadError({ label, onRetry }: { label: string; onRetry: () => void }) {
  return (
    <div className="empty-state" role="alert">
      <p className="empty-title">{label}暂时无法加载</p>
      <Button size="sm" onClick={onRetry}>
        重试
      </Button>
    </div>
  );
}
