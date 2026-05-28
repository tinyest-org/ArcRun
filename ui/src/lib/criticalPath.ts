import type { BasicTask, Link } from '../types';
import { computeCriticalPath as genericCriticalPath } from './isometric';
import { taskToNode, linkToEdge } from './arcrunAdapter';

export type { CriticalPath } from './isometric';

export function computeCriticalPath(tasks: BasicTask[], links: Link[]) {
  return genericCriticalPath(tasks.map(taskToNode), links.map(linkToEdge));
}
