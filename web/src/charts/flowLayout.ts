import { Graph, layout } from "@dagrejs/dagre";

export interface FlowLayoutNode {
  id: string;
  width?: number;
  height?: number;
}

export interface FlowLayoutEdge {
  source: string;
  target: string;
}

export interface FlowLayoutOptions {
  direction?: "LR" | "TB";
  nodeWidth?: number;
  nodeHeight?: number;
  rankSep?: number;
  nodeSep?: number;
}

export interface FlowPosition {
  x: number;
  y: number;
}

/**
 * Automatic layout for the pool canvas (node = pool, edge = depends_on).
 * Positions are computed, never stored, so nothing about the layout has to be
 * written back to the server. Returns top-left corners, as React Flow expects.
 */
export function layoutFlow(
  nodes: readonly FlowLayoutNode[],
  edges: readonly FlowLayoutEdge[],
  options: FlowLayoutOptions = {},
): Map<string, FlowPosition> {
  const {
    direction = "LR",
    nodeWidth = 180,
    nodeHeight = 64,
    rankSep = 64,
    nodeSep = 24,
  } = options;
  const graph = new Graph();
  graph.setGraph({ rankdir: direction, ranksep: rankSep, nodesep: nodeSep });
  graph.setDefaultEdgeLabel(() => ({}));
  for (const node of nodes) {
    graph.setNode(node.id, {
      width: node.width ?? nodeWidth,
      height: node.height ?? nodeHeight,
    });
  }
  for (const edge of edges) {
    graph.setEdge(edge.source, edge.target);
  }
  layout(graph);
  const positions = new Map<string, FlowPosition>();
  for (const node of nodes) {
    const placed = graph.node(node.id) as { x: number; y: number; width: number; height: number };
    positions.set(node.id, { x: placed.x - placed.width / 2, y: placed.y - placed.height / 2 });
  }
  return positions;
}
