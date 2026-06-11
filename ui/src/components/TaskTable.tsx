import { createSignal, createMemo, createEffect, For, Show } from 'solid-js';
import type { DagResponse, BasicTask } from '../types';
import { STATUS_BG_CLASSES } from '../constants';
import { formatTime, formatDurationFromDates } from '../lib/format';

interface Props {
  data: DagResponse | null;
  onTaskClick: (task: BasicTask) => void;
  onBulkCancel?: (taskIds: string[]) => void;
}

type SortKey =
  | 'name'
  | 'kind'
  | 'status'
  | 'created_at'
  | 'started_at'
  | 'ended_at'
  | 'duration'
  | 'success'
  | 'failures'
  | 'progress';

const COLUMNS: { key: SortKey; label: string; filterable?: boolean }[] = [
  { key: 'name', label: 'Name', filterable: true },
  { key: 'kind', label: 'Kind', filterable: true },
  { key: 'status', label: 'Status' },
  { key: 'created_at', label: 'Created' },
  { key: 'started_at', label: 'Started' },
  { key: 'ended_at', label: 'Ended' },
  { key: 'duration', label: 'Duration' },
  { key: 'success', label: 'Success' },
  { key: 'failures', label: 'Failures' },
  { key: 'progress', label: 'Progress' },
];

function progressPct(t: BasicTask): number | null {
  if (t.expected_count == null || t.expected_count === 0) return null;
  return Math.min(100, Math.round(((t.success + t.failures) / t.expected_count) * 100));
}

function durationMs(t: BasicTask): number | null {
  if (!t.started_at) return null;
  const end = t.ended_at ? new Date(t.ended_at).getTime() : Date.now();
  return end - new Date(t.started_at).getTime();
}

function compare(a: BasicTask, b: BasicTask, key: SortKey): number {
  if (key === 'progress') {
    const ap = progressPct(a);
    const bp = progressPct(b);
    if (ap == null && bp == null) return 0;
    if (ap == null) return 1;
    if (bp == null) return -1;
    return ap - bp;
  }
  if (key === 'duration') {
    const ad = durationMs(a);
    const bd = durationMs(b);
    if (ad == null && bd == null) return 0;
    if (ad == null) return 1;
    if (bd == null) return -1;
    return ad - bd;
  }
  const av = a[key];
  const bv = b[key];
  if (av == null && bv == null) return 0;
  if (av == null) return 1;
  if (bv == null) return -1;
  if (typeof av === 'number' && typeof bv === 'number') return av - bv;
  return String(av).localeCompare(String(bv));
}


export default function TaskTable(props: Props) {
  const [sortKey, setSortKey] = createSignal<SortKey>('name');
  const [sortAsc, setSortAsc] = createSignal(true);
  const [nameFilter, setNameFilter] = createSignal('');
  const [kindFilter, setKindFilter] = createSignal('');
  const [selectedIds, setSelectedIds] = createSignal<Set<string>>(new Set());

  // Prune stale selections when data changes
  createEffect(() => {
    const taskIds = new Set(props.data?.tasks.map((t) => t.id) ?? []);
    setSelectedIds((prev) => {
      const next = new Set([...prev].filter((id) => taskIds.has(id)));
      return next.size === prev.size ? prev : next;
    });
  });

  function handleSort(key: SortKey) {
    if (sortKey() === key) {
      setSortAsc((v) => !v);
    } else {
      setSortKey(key);
      setSortAsc(true);
    }
  }

  const filteredAndSortedTasks = createMemo(() => {
    let tasks = props.data?.tasks;
    if (!tasks) return [];

    // Apply column filters
    const nf = nameFilter().toLowerCase();
    const kf = kindFilter().toLowerCase();
    if (nf || kf) {
      tasks = tasks.filter((t) => {
        if (nf && !t.name.toLowerCase().includes(nf)) return false;
        if (kf && !t.kind.toLowerCase().includes(kf)) return false;
        return true;
      });
    }

    const key = sortKey();
    const asc = sortAsc();
    return [...tasks].sort((a, b) => {
      const c = compare(a, b, key);
      return asc ? c : -c;
    });
  });

  const hasFilters = () => nameFilter() || kindFilter();

  function toggleSelect(id: string) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleSelectAll() {
    const tasks = filteredAndSortedTasks();
    const sel = selectedIds();
    const allSelected = tasks.length > 0 && tasks.every((t) => sel.has(t.id));
    if (allSelected) {
      setSelectedIds(new Set<string>());
    } else {
      setSelectedIds(new Set(tasks.map((t) => t.id)));
    }
  }

  const cancelableSelected = () => {
    const sel = selectedIds();
    const tasks = props.data?.tasks ?? [];
    return tasks.filter(
      (t) => sel.has(t.id) && !['Success', 'Failure', 'Canceled'].includes(t.status),
    );
  };

  return (
    <div class="flex-1 overflow-auto p-4">
      <Show
        when={props.data}
        fallback={
          <div class="flex h-full items-center justify-center text-white/40">No data loaded</div>
        }
      >
        <table class="w-full border-collapse text-sm text-white/90">
          <thead class="sticky top-0 z-10 bg-[#0a0a1a]">
            <tr class="border-b border-white/10">
              <th class="w-8 px-2 py-2">
                <input
                  type="checkbox"
                  class="cursor-pointer accent-rose-500"
                  checked={filteredAndSortedTasks().length > 0 && filteredAndSortedTasks().every((t) => selectedIds().has(t.id))}
                  onChange={toggleSelectAll}
                />
              </th>
              <For each={COLUMNS}>
                {(col) => (
                  <th
                    class="cursor-pointer select-none px-3 py-2 text-left text-xs font-semibold uppercase text-white/50 hover:text-white/80"
                    onClick={() => handleSort(col.key)}
                  >
                    {col.label}
                    <Show when={sortKey() === col.key}>
                      <span class="ml-1">{sortAsc() ? '\u25B2' : '\u25BC'}</span>
                    </Show>
                  </th>
                )}
              </For>
            </tr>
            {/* Filter row */}
            <tr class="border-b border-white/5">
              <td class="px-2 py-1" />
              <td class="px-3 py-1">
                <input
                  type="text"
                  value={nameFilter()}
                  onInput={(e) => setNameFilter(e.currentTarget.value)}
                  placeholder="Filter..."
                  class="w-full rounded border border-white/10 bg-white/5 px-2 py-0.5 text-xs text-white/80 placeholder-white/30 outline-none focus:border-white/30"
                />
              </td>
              <td class="px-3 py-1">
                <input
                  type="text"
                  value={kindFilter()}
                  onInput={(e) => setKindFilter(e.currentTarget.value)}
                  placeholder="Filter..."
                  class="w-full rounded border border-white/10 bg-white/5 px-2 py-0.5 text-xs text-white/80 placeholder-white/30 outline-none focus:border-white/30"
                />
              </td>
              <td colspan={8} class="px-3 py-1">
                <Show when={hasFilters()}>
                  <button
                    class="text-xs text-white/40 hover:text-white/70"
                    onClick={() => {
                      setNameFilter('');
                      setKindFilter('');
                    }}
                  >
                    Clear filters
                  </button>
                </Show>
              </td>
            </tr>
          </thead>
          <tbody>
            <For each={filteredAndSortedTasks()}>
              {(task) => (
                <tr
                  class="cursor-pointer border-b border-white/5 transition-colors hover:bg-white/5"
                  classList={{ 'bg-white/[0.03]': selectedIds().has(task.id) }}
                  onClick={() => props.onTaskClick(task)}
                >
                  <td class="w-8 px-2 py-2" onClick={(e) => e.stopPropagation()}>
                    <input
                      type="checkbox"
                      class="cursor-pointer accent-rose-500"
                      checked={selectedIds().has(task.id)}
                      onChange={() => toggleSelect(task.id)}
                    />
                  </td>
                  <td class="max-w-[240px] truncate px-3 py-2 font-medium">{task.name}</td>
                  <td class="px-3 py-2 text-white/60">{task.kind}</td>
                  <td class="px-3 py-2">
                    <span
                      class={`inline-block rounded px-2 py-0.5 text-xs font-semibold uppercase text-white ${STATUS_BG_CLASSES[task.status] ?? 'bg-gray-500'}`}
                    >
                      {task.status}
                    </span>
                  </td>
                  <td class="px-3 py-2 text-white/50">{formatTime(task.created_at)}</td>
                  <td class="px-3 py-2 text-white/50">{formatTime(task.started_at)}</td>
                  <td class="px-3 py-2 text-white/50">{formatTime(task.ended_at)}</td>
                  <td
                    class="px-3 py-2 font-mono text-xs"
                    classList={{
                      'text-blue-400': task.status === 'Running',
                      'text-white/50': task.status !== 'Running',
                    }}
                  >
                    {formatDurationFromDates(task.started_at, task.ended_at)}
                  </td>
                  <td class="px-3 py-2 text-center">{task.success}</td>
                  <td class="px-3 py-2 text-center">{task.failures}</td>
                  <td class="px-3 py-2 text-center">
                    {(() => {
                      const pct = progressPct(task);
                      if (pct == null) return <span class="text-white/30">{'\u2014'}</span>;
                      return (
                        <div class="flex items-center gap-1.5">
                          <div class="h-1.5 w-16 overflow-hidden rounded-full bg-white/10">
                            <div class="h-full bg-emerald-500" style={{ width: `${pct}%` }} />
                          </div>
                          <span class="text-xs text-white/60">{pct}%</span>
                        </div>
                      );
                    })()}
                  </td>
                </tr>
              )}
            </For>
            <Show when={filteredAndSortedTasks().length === 0 && hasFilters()}>
              <tr>
                <td colspan={11} class="px-3 py-4 text-center text-xs text-white/40">
                  No tasks match the current filters
                </td>
              </tr>
            </Show>
          </tbody>
        </table>
        <div class="mt-2 px-3 text-xs text-white/40">
          {(() => {
            const shown = filteredAndSortedTasks().length;
            const total = props.data?.tasks.length ?? 0;
            return hasFilters() && shown !== total
              ? `${shown} of ${total} tasks`
              : `${total} task${total !== 1 ? 's' : ''}`;
          })()}
        </div>
      </Show>

      {/* Floating bulk action bar */}
      <Show when={selectedIds().size > 0}>
        <div class="sticky bottom-4 mx-auto flex w-fit items-center gap-3 rounded-lg border border-white/15 bg-[#12122a] px-4 py-2 shadow-lg">
          <span class="text-xs text-white/60">
            {selectedIds().size} selected
          </span>
          <Show when={props.onBulkCancel && cancelableSelected().length > 0}>
            <button
              class="rounded bg-red-600/80 px-3 py-1 text-xs font-medium text-white hover:bg-red-600"
              onClick={() => {
                const ids = cancelableSelected().map((t) => t.id);
                props.onBulkCancel?.(ids);
                setSelectedIds(new Set<string>());
              }}
            >
              Cancel {cancelableSelected().length} task{cancelableSelected().length !== 1 ? 's' : ''}
            </button>
          </Show>
          <button
            class="text-xs text-white/40 hover:text-white/70"
            onClick={() => setSelectedIds(new Set<string>())}
          >
            Clear
          </button>
        </div>
      </Show>
    </div>
  );
}
