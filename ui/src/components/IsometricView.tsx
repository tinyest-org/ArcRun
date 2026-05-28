import { createMemo } from 'solid-js';
import type { DagResponse, BasicTask } from '../types';
import type { CriticalPath, DagNode } from '../lib/isometric';
import { IsometricDag } from '../lib/isometric';
import {
  dagResponseToData,
  nodeToTask,
  arcrunColorMap,
  arcrunOrderedStatuses,
  arcrunShellPriority,
} from '../lib/arcrunAdapter';
import { formatDuration } from '../lib/format';

interface Props {
  data: DagResponse | null;
  criticalPath?: CriticalPath | null;
  onNodeClick: (task: BasicTask) => void;
  onBackgroundClick: () => void;
  groupBy?: 'dag' | 'status';
  onResetCamera?: (fn: () => void) => void;
}

function formatArcrunTooltip(node: DagNode<BasicTask>): string[] {
  const task = node.metadata!;
  const lines = [`${task.name}`, `${task.status} | ${task.kind}`];
  if (task.success || task.failures) {
    let counters = `✓ ${task.success}  ✗ ${task.failures}`;
    if (task.expected_count) {
      const pct = Math.min(
        100,
        Math.round(((task.success + task.failures) / task.expected_count) * 100),
      );
      counters += `  (${pct}%)`;
    }
    lines.push(counters);
  }
  if (task.started_at) {
    const start = new Date(task.started_at).getTime();
    const end = task.ended_at ? new Date(task.ended_at).getTime() : Date.now();
    lines.push(`⏱ ${formatDuration(end - start)}`);
  }
  return lines;
}

export default function IsometricView(props: Props) {
  const dagData = createMemo(() =>
    props.data ? dagResponseToData(props.data) : null,
  );

  function handleNodeClick(node: DagNode) {
    props.onNodeClick(nodeToTask(node as DagNode<BasicTask>));
  }

  return (
    <IsometricDag
      data={dagData()}
      colorMap={arcrunColorMap}
      criticalPath={props.criticalPath}
      onNodeClick={handleNodeClick}
      onBackgroundClick={props.onBackgroundClick}
      groupBy={props.groupBy}
      orderedStatuses={arcrunOrderedStatuses}
      statusShellPriority={arcrunShellPriority}
      formatTooltip={formatArcrunTooltip as (node: DagNode) => string[]}
      onResetCamera={props.onResetCamera}
    />
  );
}
