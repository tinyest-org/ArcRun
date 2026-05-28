import type { DagNode, DagEdge } from './types';
import { computeTopologicalDepth } from './layout';

export interface CriticalPath {
  nodeIds: Set<string>;
  edgeIds: Set<string>;
}

export function computeCriticalPath(nodes: DagNode[], edges: DagEdge[]): CriticalPath {
  if (nodes.length === 0) return { nodeIds: new Set(), edgeIds: new Set() };

  const depthMap = computeTopologicalDepth(nodes, edges);

  let maxDepth = 0;
  let endNodeId = nodes[0].id;
  for (const [id, depth] of depthMap) {
    if (depth > maxDepth) {
      maxDepth = depth;
      endNodeId = id;
    }
  }

  if (maxDepth === 0) return { nodeIds: new Set(), edgeIds: new Set() };

  const nodeIds = new Set(nodes.map((n) => n.id));
  const parents = new Map<string, string[]>();
  for (const edge of edges) {
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) continue;
    const list = parents.get(edge.target) ?? [];
    list.push(edge.source);
    parents.set(edge.target, list);
  }

  const cpNodes = new Set<string>();
  const cpEdges = new Set<string>();

  let current = endNodeId;
  cpNodes.add(current);

  while (depthMap.get(current)! > 0) {
    const parentList = parents.get(current) ?? [];
    const currentDepth = depthMap.get(current)!;
    let bestParent: string | null = null;
    for (const parentId of parentList) {
      if (depthMap.get(parentId) === currentDepth - 1) {
        bestParent = parentId;
        break;
      }
    }
    if (!bestParent) break;
    cpNodes.add(bestParent);
    cpEdges.add(`${bestParent}-${current}`);
    current = bestParent;
  }

  return { nodeIds: cpNodes, edgeIds: cpEdges };
}
