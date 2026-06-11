import { Show } from 'solid-js';
import type { TaskStatus } from '../types';
import { STATUS_BG_CLASSES } from '../constants';

interface Props {
  status: TaskStatus;
}

export default function StatusBadge(props: Props) {
  return (
    <span
      class={`inline-flex items-center gap-1.5 rounded px-2 py-0.5 text-xs font-semibold uppercase text-white ${STATUS_BG_CLASSES[props.status] ?? 'bg-gray-500'}`}
    >
      <Show when={props.status === 'Running'}>
        <span class="status-pulse-dot" aria-hidden="true" />
      </Show>
      {props.status}
    </span>
  );
}
