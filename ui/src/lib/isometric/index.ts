export { default as IsometricDag } from './IsometricDag';
export type { IsometricDagProps } from './IsometricDag';

export type { DagNode, DagEdge, DagData, ColorMap, IsometricConfig } from './types';

export { computeCriticalPath } from './criticalPath';
export type { CriticalPath } from './criticalPath';

export { computeTopologicalDepth, computeGroupLayout, computeStatusLayout } from './layout';
export type { GroupLayout, LayoutResult, StatusGroupLayout, StatusLayoutResult } from './layout';

export { structureChanged } from './dagDiff';

export {
  buildDagSceneObjects,
  buildStatusSceneObjects,
  computeStatusAnimationTargets,
  applyCriticalPathHighlighting,
} from './sceneBuilder';
export type { SceneRefs } from './sceneBuilder';

export {
  TASK_BOX_SIZE,
  TASK_SPACING,
  GROUP_PADDING,
  colorToHex,
  createTextSprite,
  disposeObject,
} from './threeHelpers';
