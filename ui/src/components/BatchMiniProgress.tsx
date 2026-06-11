import { For } from 'solid-js';
import type { BatchStatusCounts } from '../types';

interface Props {
  counts: BatchStatusCounts;
  total: number;
  class?: string;
}

/** Stacked mini progress bar summarizing a batch's status distribution. */
export default function BatchMiniProgress(props: Props) {
  const segments = () => {
    const c = props.counts;
    const total = props.total || 1;
    return [
      { count: c.success, class: 'bg-emerald-500' },
      { count: c.failure, class: 'bg-red-500' },
      { count: c.canceled, class: 'bg-gray-400' },
      { count: c.running + c.claimed, class: 'bg-blue-500' },
      { count: c.pending, class: 'bg-amber-500' },
      { count: c.paused, class: 'bg-purple-500' },
    ]
      .filter((s) => s.count > 0)
      .map((s) => ({ ...s, pct: (s.count / total) * 100 }));
  };

  return (
    <div class={`flex h-1 w-full overflow-hidden rounded-full bg-white/10 ${props.class ?? ''}`}>
      <For each={segments()}>
        {(seg) => <div class={`h-full ${seg.class}`} style={{ width: `${seg.pct}%` }} />}
      </For>
    </div>
  );
}
