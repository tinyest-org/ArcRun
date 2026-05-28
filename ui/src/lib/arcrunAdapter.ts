import type { BasicTask, Link, DagResponse, TaskStatus } from '../types';
import type { DagNode, DagEdge, DagData, ColorMap } from './isometric';
import { STATUS_COLORS, ALL_STATUSES } from '../constants';

export function taskToNode(task: BasicTask): DagNode<BasicTask> {
  return {
    id: task.id,
    label: task.name,
    group: task.kind,
    status: task.status,
    metadata: task,
  };
}

export function linkToEdge(link: Link): DagEdge {
  return {
    source: link.parent_id,
    target: link.child_id,
    strong: link.requires_success,
  };
}

export function dagResponseToData(response: DagResponse): DagData<BasicTask> {
  return {
    nodes: response.tasks.map(taskToNode),
    edges: response.links.map(linkToEdge),
  };
}

export function nodeToTask(node: DagNode<BasicTask>): BasicTask {
  return node.metadata!;
}

export const arcrunColorMap: ColorMap = { ...STATUS_COLORS };

export const arcrunOrderedStatuses: string[] = [...ALL_STATUSES];

export function arcrunShellPriority(status: string): number {
  switch (status as TaskStatus) {
    case 'Running':
    case 'Failure':
    case 'Canceled':
      return 0;
    case 'Pending':
    case 'Claimed':
    case 'Waiting':
    case 'Paused':
      return 1;
    case 'Success':
      return 2;
    default:
      return 1;
  }
}
