export interface DagNode<T = unknown> {
  id: string;
  label: string;
  group: string;
  status: string;
  metadata?: T;
}

export interface DagEdge {
  source: string;
  target: string;
  strong?: boolean;
}

export interface DagData<T = unknown> {
  nodes: DagNode<T>[];
  edges: DagEdge[];
}

export type ColorMap = Record<string, string>;

export interface IsometricConfig {
  colorMap: ColorMap;
  orderedStatuses?: string[];
  statusShellPriority?: (status: string) => number;
  formatTooltip?: (node: DagNode) => string[];
}
