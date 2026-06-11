import { Show, createSignal, onCleanup } from 'solid-js';
import type { DagResponse, BasicTask, TaskStatus } from '../types';
import StatsBar from './StatsBar';
import StatusFilter from './StatusFilter';
import KindFilter from './KindFilter';
import BatchProgress from './BatchProgress';
import OverflowMenu, { type MenuEntry } from './OverflowMenu';
import {
  IconArrowLeft,
  IconCheck,
  IconGraph,
  IconBox,
  IconTable,
  IconGantt,
  IconMaximize,
  IconZap,
  IconRefresh,
  IconSearch,
  IconDownload,
  IconSliders,
  IconBan,
  IconHelp,
  IconX,
} from './icons';

type ViewMode = 'dag' | 'iso' | 'table' | 'timeline';

interface Props {
  batchId: string;
  batchIdShort: string;
  dagData: DagResponse | null;
  viewMode: ViewMode;
  isoGroupBy: 'dag' | 'status';
  showCriticalPath: boolean;
  openTaskCount: number;
  hasActiveTasks: boolean;
  activeFilters: Set<TaskStatus>;
  activeKinds: Set<string>;
  loading: boolean;
  lastRefreshed: number | null;
  onRefresh: () => void;
  onNavigateHome: () => void;
  onSetViewMode: (mode: ViewMode) => void;
  onSetIsoGroupBy: (g: 'dag' | 'status') => void;
  onToggleCriticalPath: () => void;
  onCloseAllWindows: () => void;
  onEditRules: () => void;
  onCancelBatch: () => void;
  onExportCsv: () => void;
  onShowHelp: () => void;
  onFitView: () => void;
  onOpenPalette: () => void;
  onToggleStatusFilter: (status: TaskStatus) => void;
  onToggleKindFilter: (kind: string) => void;
  onClearFilters: () => void;
}

const VIEW_MODES: { mode: ViewMode; label: string; title: string; icon: typeof IconGraph }[] = [
  { mode: 'dag', label: '2D', title: '2D DAG view (1)', icon: IconGraph },
  { mode: 'iso', label: '3D', title: '3D isometric view (2)', icon: IconBox },
  { mode: 'table', label: 'Table', title: 'Table view (3)', icon: IconTable },
  { mode: 'timeline', label: 'Timeline', title: 'Timeline / Gantt view (4)', icon: IconGantt },
];

/** Live "updated Xs ago" label + manual refresh button. */
function RefreshIndicator(props: { loading: boolean; lastRefreshed: number | null; onRefresh: () => void }) {
  const [now, setNow] = createSignal(Date.now());
  const tick = setInterval(() => setNow(Date.now()), 1000);
  onCleanup(() => clearInterval(tick));

  const label = () => {
    if (props.lastRefreshed == null) return '';
    const secs = Math.max(0, Math.floor((now() - props.lastRefreshed) / 1000));
    return secs < 2 ? 'just now' : `${secs}s ago`;
  };

  return (
    <div class="flex items-center gap-1.5">
      <button
        title="Refresh now (R)"
        aria-label="Refresh now"
        class="icon-btn"
        onClick={props.onRefresh}
      >
        <span classList={{ 'animate-spin-slow': props.loading }} class="inline-flex">
          <IconRefresh size={14} />
        </span>
      </button>
      <Show when={props.lastRefreshed != null}>
        <span class="hide-mobile w-14 text-[10px] tabular-nums text-white/30" aria-live="off">
          {label()}
        </span>
      </Show>
    </div>
  );
}

export default function DagToolbar(props: Props) {
  const [copied, setCopied] = createSignal(false);

  function copyBatchId() {
    navigator.clipboard?.writeText(props.batchId).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }

  const menuEntries = (): MenuEntry[] => {
    const entries: MenuEntry[] = [];
    if (props.dagData) {
      entries.push({ label: 'Export CSV', icon: IconDownload, onClick: props.onExportCsv });
    }
    if (props.dagData && props.hasActiveTasks) {
      entries.push({ label: 'Edit rules', icon: IconSliders, onClick: props.onEditRules });
    }
    if (props.openTaskCount > 0) {
      entries.push({
        label: `Close all windows (${props.openTaskCount})`,
        icon: IconX,
        onClick: props.onCloseAllWindows,
      });
    }
    entries.push({ label: 'Keyboard shortcuts', icon: IconHelp, onClick: props.onShowHelp });
    if (props.dagData && props.hasActiveTasks) {
      entries.push('separator');
      entries.push({
        label: 'Cancel batch...',
        icon: IconBan,
        danger: true,
        onClick: props.onCancelBatch,
      });
    }
    return entries;
  };

  return (
    <header class="glass-navbar relative z-10 flex flex-col gap-2 px-4 py-2.5">
      {/* Row 1: identity / view switcher / actions */}
      <div class="flex flex-wrap items-center gap-x-3 gap-y-2">
        {/* Left: back + batch identity + progress */}
        <div class="flex min-w-0 flex-1 items-center gap-2">
          <button
            title="Back to batches"
            aria-label="Back to batches"
            class="icon-btn"
            onClick={props.onNavigateHome}
          >
            <IconArrowLeft size={15} />
          </button>
          <button
            class="flex items-center gap-1.5 truncate font-mono text-sm text-rose-400 transition-colors hover:text-rose-300"
            title={copied() ? 'Copied!' : `${props.batchId} — click to copy`}
            aria-label="Copy batch ID to clipboard"
            onClick={copyBatchId}
          >
            <Show when={copied()} fallback={props.batchIdShort}>
              <IconCheck size={13} /> Copied
            </Show>
          </button>
          <span class="hide-mobile">
            <BatchProgress tasks={props.dagData?.tasks ?? []} />
          </span>
        </div>

        {/* Center: segmented view switcher */}
        <div
          class="flex overflow-hidden rounded-lg border border-white/15"
          role="tablist"
          aria-label="View mode"
        >
          {VIEW_MODES.map((vm, i) => (
            <button
              role="tab"
              aria-selected={props.viewMode === vm.mode}
              title={vm.title}
              class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium transition-colors"
              classList={{
                'border-l border-white/15': i > 0,
                'bg-white/15 text-white': props.viewMode === vm.mode,
                'text-white/50 hover:bg-white/5 hover:text-white/80': props.viewMode !== vm.mode,
              }}
              disabled={!props.dagData}
              onClick={() => props.onSetViewMode(vm.mode)}
            >
              <vm.icon size={13} />
              <span class="hide-mobile">{vm.label}</span>
            </button>
          ))}
        </div>

        {/* Right: contextual + global actions */}
        <div class="flex flex-1 items-center justify-end gap-1.5">
          <Show when={props.viewMode === 'iso' && props.dagData}>
            <div
              class="flex overflow-hidden rounded-md border border-white/15"
              role="group"
              aria-label="Isometric grouping"
            >
              <button
                class="px-2 py-1 text-[11px] font-medium transition-colors"
                classList={{
                  'bg-white/15 text-white': props.isoGroupBy === 'dag',
                  'text-white/50 hover:bg-white/5 hover:text-white/80': props.isoGroupBy !== 'dag',
                }}
                onClick={() => props.onSetIsoGroupBy('dag')}
              >
                DAG
              </button>
              <button
                class="border-l border-white/15 px-2 py-1 text-[11px] font-medium transition-colors"
                classList={{
                  'bg-white/15 text-white': props.isoGroupBy === 'status',
                  'text-white/50 hover:bg-white/5 hover:text-white/80':
                    props.isoGroupBy !== 'status',
                }}
                onClick={() => props.onSetIsoGroupBy('status')}
              >
                Status
              </button>
            </div>
          </Show>

          <Show when={props.viewMode === 'dag' || props.viewMode === 'iso'}>
            <button
              title="Fit to viewport (F)"
              aria-label="Fit to viewport"
              class="icon-btn"
              disabled={!props.dagData}
              onClick={props.onFitView}
            >
              <IconMaximize size={14} />
            </button>
          </Show>

          <Show when={props.dagData}>
            <button
              title="Toggle critical path (C)"
              aria-label="Toggle critical path"
              aria-pressed={props.showCriticalPath}
              class="icon-btn"
              classList={{
                'border-amber-400/40! bg-amber-400/15! text-amber-300!': props.showCriticalPath,
              }}
              onClick={props.onToggleCriticalPath}
            >
              <IconZap size={14} />
            </button>
          </Show>

          <RefreshIndicator
            loading={props.loading}
            lastRefreshed={props.lastRefreshed}
            onRefresh={props.onRefresh}
          />

          <button
            title="Command palette (Cmd+K)"
            aria-label="Open command palette"
            class="icon-btn"
            onClick={props.onOpenPalette}
          >
            <IconSearch size={14} />
          </button>

          <OverflowMenu entries={menuEntries()} title="More actions" />
        </div>
      </div>

      {/* Row 2: filters + stats */}
      <Show when={props.dagData}>
        <div class="flex w-full flex-wrap items-center gap-3">
          <StatusFilter
            tasks={props.dagData!.tasks}
            activeFilters={props.activeFilters}
            onToggle={props.onToggleStatusFilter}
          />
          <Show when={new Set(props.dagData!.tasks.map((t: BasicTask) => t.kind)).size > 1}>
            <div class="h-4 w-px bg-white/15" />
            <KindFilter
              tasks={props.dagData!.tasks}
              activeKinds={props.activeKinds}
              onToggle={props.onToggleKindFilter}
            />
          </Show>
          <Show when={props.activeFilters.size > 0 || props.activeKinds.size > 0}>
            <button
              class="text-xs text-white/40 underline-offset-2 transition-colors hover:text-white/70 hover:underline"
              onClick={props.onClearFilters}
            >
              Clear filters
            </button>
          </Show>
          <span class="hide-mobile ml-auto">
            <StatsBar
              tasks={props.dagData?.tasks ?? []}
              linkCount={props.dagData?.links.length ?? 0}
            />
          </span>
        </div>
      </Show>
    </header>
  );
}
