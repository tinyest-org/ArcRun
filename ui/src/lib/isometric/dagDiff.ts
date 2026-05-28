import type { DagNode, DagEdge } from './types';

export function structureChanged(
  nodes: DagNode[],
  edges: DagEdge[],
  currentNodeIds: Set<string>,
  currentEdgeIds: Set<string>,
): boolean {
  if (nodes.length !== currentNodeIds.size) return true;
  for (const n of nodes) {
    if (!currentNodeIds.has(n.id)) return true;
  }
  const newEdgeIds = new Set(edges.map((e) => `${e.source}-${e.target}`));
  if (newEdgeIds.size !== currentEdgeIds.size) return true;
  for (const eid of newEdgeIds) {
    if (!currentEdgeIds.has(eid)) return true;
  }
  return false;
}
