/** Loading placeholders shaped like the content they stand for (no spinners). */

export function SkeletonLine({ width = "100%" }: { width?: string }) {
  return <span className="skel skel-line" style={{ width }} aria-hidden="true" />;
}

export function SkeletonKpis({ count = 5 }: { count?: number }) {
  return (
    <div className="kpis" aria-hidden="true">
      {Array.from({ length: count }, (_, index) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: static placeholders never reorder.
        <div className="kpi" key={index}>
          <SkeletonLine width="40%" />
          <span className="skel skel-val" />
          <SkeletonLine width="70%" />
        </div>
      ))}
    </div>
  );
}

export function SkeletonRows({ rows = 5 }: { rows?: number }) {
  return (
    <div className="skel-rows" aria-hidden="true">
      {Array.from({ length: rows }, (_, index) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: static placeholders never reorder.
        <SkeletonLine key={index} width={`${92 - (index % 3) * 14}%`} />
      ))}
    </div>
  );
}

/** A whole page while its first response is on the way. */
export function PageSkeleton({ label = "正在加载" }: { label?: string }) {
  return (
    <div className="page-skel" aria-busy="true" role="status" aria-label={label}>
      <div className="skel-head">
        <SkeletonLine width="80px" />
        <span className="skel skel-title" />
      </div>
      <SkeletonKpis />
      <div className="panel">
        <div className="panel-b">
          <SkeletonRows rows={6} />
        </div>
      </div>
    </div>
  );
}
