import type { DagNode, DagEdge } from './types';

export interface GroupLayout {
  key: { group: string; step: number };
  nodes: DagNode[];
  groupIndex: number;
  stepIndex: number;
  cols: number;
  rows: number;
  layers: number;
}

export interface LayoutResult {
  groups: GroupLayout[];
  groupNames: string[];
  maxStep: number;
  depthMap: Map<string, number>;
  maxCols: number;
  maxRows: number;
  maxLayers: number;
}

export function computeTopologicalDepth(
  nodes: DagNode[],
  edges: DagEdge[],
): Map<string, number> {
  const nodeIds = new Set(nodes.map((n) => n.id));
  const inEdges = new Map<string, { parentId: string }[]>();
  const outEdges = new Map<string, string[]>();
  const depth = new Map<string, number>();

  for (const id of nodeIds) {
    inEdges.set(id, []);
    outEdges.set(id, []);
  }

  for (const edge of edges) {
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) continue;
    inEdges.get(edge.target)!.push({ parentId: edge.source });
    outEdges.get(edge.source)!.push(edge.target);
  }

  const inDegree = new Map<string, number>();
  for (const id of nodeIds) {
    inDegree.set(id, inEdges.get(id)!.length);
  }

  const queue: string[] = [];
  for (const id of nodeIds) {
    if (inDegree.get(id) === 0) {
      depth.set(id, 0);
      queue.push(id);
    }
  }

  let head = 0;
  while (head < queue.length) {
    const current = queue[head++];
    const currentDepth = depth.get(current)!;

    for (const childId of outEdges.get(current)!) {
      const childDepth = depth.get(childId);
      const newDepth = currentDepth + 1;
      if (childDepth === undefined || newDepth > childDepth) {
        depth.set(childId, newDepth);
      }

      const remaining = inDegree.get(childId)! - 1;
      inDegree.set(childId, remaining);
      if (remaining === 0) {
        queue.push(childId);
      }
    }
  }

  for (const id of nodeIds) {
    if (!depth.has(id)) {
      depth.set(id, 0);
    }
  }

  return depth;
}

function defaultShellPriority(_status: string): number {
  return 1;
}

function sortNodesForRadialPlacement(
  nodes: DagNode[],
  cols: number,
  rows: number,
  shellPriority: (status: string) => number,
): DagNode[] {
  const n = nodes.length;
  if (n <= 1) return nodes;

  const sliceSize = cols * rows;
  const layers = Math.ceil(n / sliceSize);
  const centerCol = (cols - 1) / 2;
  const centerRow = (rows - 1) / 2;
  const centerLayer = (layers - 1) / 2;

  const indexDistances: { index: number; dist: number }[] = [];
  for (let i = 0; i < n; i++) {
    const layer = Math.floor(i / sliceSize);
    const inSlice = i % sliceSize;
    const col = inSlice % cols;
    const row = Math.floor(inSlice / cols);

    const dx = cols > 1 ? (col - centerCol) / centerCol : 0;
    const dz = rows > 1 ? (row - centerRow) / centerRow : 0;
    const dy = layers > 1 ? (layer - centerLayer) / centerLayer : 0;
    const dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
    indexDistances.push({ index: i, dist });
  }

  indexDistances.sort((a, b) => b.dist - a.dist);

  const sortedNodes = [...nodes].sort(
    (a, b) => shellPriority(a.status) - shellPriority(b.status),
  );

  const result = new Array<DagNode>(n);
  for (let i = 0; i < n; i++) {
    result[indexDistances[i].index] = sortedNodes[i];
  }

  return result;
}

export function computeGroupLayout(
  nodes: DagNode[],
  edges: DagEdge[],
  shellPriority: (status: string) => number = defaultShellPriority,
): LayoutResult {
  const depthMap = computeTopologicalDepth(nodes, edges);

  const groupSet = new Set<string>();
  for (const n of nodes) groupSet.add(n.group);
  const groupNames = Array.from(groupSet).sort();
  const groupIndex = new Map<string, number>();
  groupNames.forEach((k, i) => groupIndex.set(k, i));

  let maxStep = 0;
  for (const d of depthMap.values()) {
    if (d > maxStep) maxStep = d;
  }

  const groupMap = new Map<string, DagNode[]>();
  for (const node of nodes) {
    const step = depthMap.get(node.id) ?? 0;
    const key = `${node.group}:${step}`;
    let group = groupMap.get(key);
    if (!group) {
      group = [];
      groupMap.set(key, group);
    }
    group.push(node);
  }

  const groups: GroupLayout[] = [];
  let maxCols = 1;
  let maxRows = 1;
  let maxLayers = 1;
  for (const [key, groupNodes] of groupMap) {
    const [group, stepStr] = key.split(':');
    const step = parseInt(stepStr, 10);
    const n = groupNodes.length;
    const dim = Math.ceil(Math.cbrt(n));
    const cols = dim;
    const rows = Math.ceil(Math.sqrt(n / cols));
    const layers = Math.ceil(n / (cols * rows));
    if (cols > maxCols) maxCols = cols;
    if (rows > maxRows) maxRows = rows;
    if (layers > maxLayers) maxLayers = layers;
    groups.push({
      key: { group, step },
      nodes: sortNodesForRadialPlacement(groupNodes, cols, rows, shellPriority),
      groupIndex: groupIndex.get(group)!,
      stepIndex: step,
      cols,
      rows,
      layers,
    });
  }

  return { groups, groupNames, maxStep, depthMap, maxCols, maxRows, maxLayers };
}

export interface StatusGroupLayout {
  status: string;
  step: number;
  nodes: DagNode[];
  statusIndex: number;
  stepIndex: number;
  cols: number;
  rows: number;
  layers: number;
}

export interface StatusLayoutResult {
  groups: StatusGroupLayout[];
  statuses: string[];
  maxStep: number;
  maxCols: number;
  maxRows: number;
  maxLayers: number;
}

export function computeStatusLayout(
  nodes: DagNode[],
  edges: DagEdge[],
  orderedStatuses?: string[],
): StatusLayoutResult {
  const depthMap = computeTopologicalDepth(nodes, edges);

  let maxStep = 0;
  for (const d of depthMap.values()) {
    if (d > maxStep) maxStep = d;
  }

  const groupMap = new Map<string, DagNode[]>();
  for (const node of nodes) {
    const step = depthMap.get(node.id) ?? 0;
    const key = `${step}:${node.status}`;
    let group = groupMap.get(key);
    if (!group) {
      group = [];
      groupMap.set(key, group);
    }
    group.push(node);
  }

  const presentStatuses = new Set<string>();
  for (const node of nodes) presentStatuses.add(node.status);

  let statuses: string[];
  if (orderedStatuses) {
    statuses = orderedStatuses.filter((s) => presentStatuses.has(s));
    for (const s of presentStatuses) {
      if (!statuses.includes(s)) statuses.push(s);
    }
  } else {
    statuses = Array.from(presentStatuses).sort();
  }

  const statusIndex = new Map<string, number>();
  statuses.forEach((s, i) => statusIndex.set(s, i));

  const groups: StatusGroupLayout[] = [];
  let maxCols = 1;
  let maxRows = 1;
  let maxLayers = 1;

  for (const [key, groupNodes] of groupMap) {
    const colonIdx = key.indexOf(':');
    const step = parseInt(key.substring(0, colonIdx), 10);
    const status = key.substring(colonIdx + 1);
    groupNodes.sort((a, b) => a.id.localeCompare(b.id));
    const n = groupNodes.length;
    const dim = Math.ceil(Math.cbrt(n));
    const cols = dim;
    const rows = Math.ceil(Math.sqrt(n / cols));
    const layers = Math.ceil(n / (cols * rows));
    if (cols > maxCols) maxCols = cols;
    if (rows > maxRows) maxRows = rows;
    if (layers > maxLayers) maxLayers = layers;
    groups.push({
      status,
      step,
      nodes: groupNodes,
      statusIndex: statusIndex.get(status)!,
      stepIndex: step,
      cols,
      rows,
      layers,
    });
  }

  return { groups, statuses, maxStep, maxCols, maxRows, maxLayers };
}
