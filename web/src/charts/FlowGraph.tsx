import { useEffect, useMemo, useState } from "react";
import { layoutFlow } from "./flowLayout";

export interface FlowGraphNode {
  id: string;
  label: string;
  kind: "condition" | "pool";
  width?: number;
  height?: number;
}

export interface FlowGraphEdge {
  source: string;
  target: string;
}

export interface FlowGraphProps {
  nodes: readonly FlowGraphNode[];
  edges: readonly FlowGraphEdge[];
  label: string;
  selectedId?: string | null;
  onSelect?: (id: string) => void;
}

const NARROW_QUERY = "(max-width: 760px)";
const NODE_WIDTH = 190;
const NODE_HEIGHT = 82;
const PADDING = 22;

function narrowScreen(): boolean {
  return typeof window.matchMedia === "function" && window.matchMedia(NARROW_QUERY).matches;
}

function edgePath(
  source: { x: number; y: number; width: number; height: number },
  target: { x: number; y: number; width: number; height: number },
  narrow: boolean,
): string {
  if (narrow) {
    const x1 = source.x + source.width / 2;
    const y1 = source.y + source.height;
    const x2 = target.x + target.width / 2;
    const y2 = target.y;
    const mid = (y1 + y2) / 2;
    return `M ${x1} ${y1} C ${x1} ${mid}, ${x2} ${mid}, ${x2} ${y2}`;
  }
  const x1 = source.x + source.width;
  const y1 = source.y + source.height / 2;
  const x2 = target.x;
  const y2 = target.y + target.height / 2;
  const mid = (x1 + x2) / 2;
  return `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`;
}

/** Read-only pool map. The node IDs stay in code and never enter page copy. */
export function FlowGraph({ nodes, edges, label, selectedId, onSelect }: FlowGraphProps) {
  const [narrow, setNarrow] = useState(narrowScreen);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return undefined;
    const query = window.matchMedia(NARROW_QUERY);
    const onChange = (event: MediaQueryListEvent) => setNarrow(event.matches);
    query.addEventListener("change", onChange);
    return () => query.removeEventListener("change", onChange);
  }, []);

  const graph = useMemo(() => {
    const positions = layoutFlow(nodes, edges, {
      direction: narrow ? "TB" : "LR",
      rankSep: narrow ? 48 : 64,
    });
    const placed = new Map<string, { x: number; y: number; width: number; height: number }>();
    let width = 0;
    let height = 0;
    for (const node of nodes) {
      const position = positions.get(node.id) ?? { x: 0, y: 0 };
      const item = {
        x: position.x + PADDING,
        y: position.y + PADDING,
        width: node.width ?? NODE_WIDTH,
        height: node.height ?? NODE_HEIGHT,
      };
      placed.set(node.id, item);
      width = Math.max(width, item.x + item.width + PADDING);
      height = Math.max(height, item.y + item.height + PADDING);
    }
    return { placed, width, height };
  }, [nodes, edges, narrow]);

  return (
    // biome-ignore lint/a11y/useSemanticElements: the read-only graph is not a form fieldset.
    <div className="flow-graph" role="group" aria-label={label}>
      <div className="flow-graph-canvas" style={{ width: graph.width, height: graph.height }}>
        <svg
          className="flow-graph-lines"
          width={graph.width}
          height={graph.height}
          aria-hidden="true"
        >
          {edges.map((edge) => {
            const source = graph.placed.get(edge.source);
            const target = graph.placed.get(edge.target);
            return source && target ? (
              <path
                className="flow-graph-edge"
                key={`${edge.source}->${edge.target}`}
                d={edgePath(source, target, narrow)}
              />
            ) : null;
          })}
        </svg>
        {nodes.map((node) => {
          const position = graph.placed.get(node.id);
          if (!position) return null;
          return (
            <button
              type="button"
              className={`flow-graph-node ${node.kind}`}
              key={node.id}
              data-id={node.id}
              aria-pressed={node.id === selectedId}
              style={{
                left: position.x,
                top: position.y,
                width: position.width,
                height: position.height,
              }}
              onClick={() => onSelect?.(node.id)}
              disabled={onSelect === undefined}
            >
              {node.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
