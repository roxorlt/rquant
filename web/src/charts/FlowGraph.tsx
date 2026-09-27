import { Background, type Edge, type Node, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useEffect, useMemo, useState } from "react";
import { useTheme } from "@/theme/ThemeProvider";
import { layoutFlow } from "./flowLayout";

export interface FlowGraphNode {
  id: string;
  label: string;
}

export interface FlowGraphEdge {
  source: string;
  target: string;
}

export interface FlowGraphProps {
  nodes: readonly FlowGraphNode[];
  edges: readonly FlowGraphEdge[];
  label: string;
  onSelect?: (id: string) => void;
}

const NARROW_QUERY = "(max-width: 760px)";

function narrowScreen(): boolean {
  return typeof window.matchMedia === "function" && window.matchMedia(NARROW_QUERY).matches;
}

/** Read-only pool and condition graph; dagre places nodes without server positions. */
export function FlowGraph({ nodes, edges, label, onSelect }: FlowGraphProps) {
  const { resolved } = useTheme();
  const [narrow, setNarrow] = useState(narrowScreen);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return undefined;
    const query = window.matchMedia(NARROW_QUERY);
    const onChange = (event: MediaQueryListEvent) => setNarrow(event.matches);
    query.addEventListener("change", onChange);
    return () => query.removeEventListener("change", onChange);
  }, []);
  const flowNodes = useMemo<Node[]>(() => {
    const positions = layoutFlow(nodes, edges, {
      direction: narrow ? "TB" : "LR",
      rankSep: narrow ? 48 : 64,
    });
    return nodes.map((node) => ({
      id: node.id,
      position: positions.get(node.id) ?? { x: 0, y: 0 },
      data: { label: node.label },
    }));
  }, [nodes, edges, narrow]);
  const flowEdges = useMemo<Edge[]>(
    () =>
      edges.map((edge) => ({
        id: `${edge.source}->${edge.target}`,
        source: edge.source,
        target: edge.target,
      })),
    [edges],
  );
  return (
    // biome-ignore lint/a11y/useSemanticElements: the graph container is not a form fieldset.
    <div
      className="flow-graph"
      role="group"
      aria-label={label}
      onKeyDown={(event) => {
        if (!onSelect || (event.key !== "Enter" && event.key !== " ")) return;
        if (!(event.target instanceof Element)) return;
        const node = event.target.closest<HTMLElement>(".react-flow__node[data-id]");
        if (!node?.dataset.id) return;
        event.preventDefault();
        onSelect(node.dataset.id);
      }}
    >
      <ReactFlow
        key={narrow ? "stacked" : "wide"}
        nodes={flowNodes}
        edges={flowEdges}
        colorMode={resolved}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        onNodeClick={onSelect ? (_event, node) => onSelect(node.id) : undefined}
      >
        <Background />
      </ReactFlow>
    </div>
  );
}
