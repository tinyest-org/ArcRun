import type { BasicTask, Link } from '../types';
import { structureChanged as genericStructureChanged } from './isometric';
import { taskToNode, linkToEdge } from './arcrunAdapter';

export function structureChanged(
  tasks: BasicTask[],
  links: Link[],
  currentNodeIds: Set<string>,
  currentEdgeIds: Set<string>,
): boolean {
  return genericStructureChanged(tasks.map(taskToNode), links.map(linkToEdge), currentNodeIds, currentEdgeIds);
}
