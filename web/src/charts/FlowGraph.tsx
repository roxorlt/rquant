import { Background, type Edge, type Node, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useMemo } from "react";
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

/**
 * The one React Flow wrapper (pool canvas: node = pool, edge = depends_on).
 * Read-only in M0; positions come from dagre, never from the server.
 */
export function FlowGraph({ nodes, edges, label, onSelect }: FlowGraphProps) {
  const { resolved } = useTheme();
  const flowNodes = useMemo<Node[]>(() => {
    const positions = layoutFlow(nodes, edges);
    return nodes.map((node) => ({
      id: node.id,
      position: positions.get(node.id) ?? { x: 0, y: 0 },
      data: { label: node.label },
    }));
  }, [nodes, edges]);
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
    <div className="flow-graph" role="group" aria-label={label}>
      <ReactFlow
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
