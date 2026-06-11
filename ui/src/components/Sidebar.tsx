import { createSignal, createEffect, on, onMount, onCleanup, For, Show } from 'solid-js';
import { useNavigate, useLocation } from '@solidjs/router';
import { listBatches } from '../api';
import {
  getRecentBatches,
  getSidebarCollapsed,
  setSidebarCollapsed as persistSidebarCollapsed,
} from '../storage';
import type { BatchSummary } from '../types';
import { useTheme } from '../App';
import { setPaletteOpen } from '../lib/commands';
import BatchMiniProgress from './BatchMiniProgress';
import {
  IconSearch,
  IconLayers,
  IconSun,
  IconMoon,
  IconPanelLeft,
} from './icons';

const SUMMARY_REFRESH_INTERVAL = 15000;
const MAX_RECENTS_SHOWN = 8;

/**
 * Persistent console sidebar: navigation, command palette trigger, and
 * recently viewed batches with live status (polled from /batches).
 */
export default function Sidebar() {
  const navigate = useNavigate();
  const location = useLocation();
  const { theme, toggle: toggleTheme } = useTheme();

  const [collapsed, setCollapsed] = createSignal(getSidebarCollapsed());
  const [recentIds, setRecentIds] = createSignal<string[]>(getRecentBatches());
  const [summaries, setSummaries] = createSignal<Map<string, BatchSummary>>(new Map());

  const activeBatchId = () => {
    const match = location.pathname.match(/^\/batch\/(.+)$/);
    return match ? decodeURIComponent(match[1]) : null;
  };

  async function refreshSummaries() {
    try {
      const results = await listBatches(0, 30);
      setSummaries(new Map(results.map((b) => [b.batch_id, b])));
    } catch {
      // Sidebar is best-effort; pages surface their own errors.
    }
  }

  let pollTimer: ReturnType<typeof setInterval> | undefined;

  onMount(() => {
    refreshSummaries();
    pollTimer = setInterval(refreshSummaries, SUMMARY_REFRESH_INTERVAL);
  });
  onCleanup(() => clearInterval(pollTimer));

  // Re-read recents when navigating (DagPage records visits in localStorage)
  createEffect(
    on(
      () => location.pathname,
      () => setRecentIds(getRecentBatches()),
    ),
  );

  function toggleCollapsed() {
    setCollapsed((v) => {
      persistSidebarCollapsed(!v);
      return !v;
    });
  }

  const isHome = () => location.pathname === '/';

  return (
    <aside
      class="hide-mobile z-20 flex shrink-0 flex-col border-r transition-all duration-200"
      style={{
        width: collapsed() ? '3.25rem' : '14rem',
        'border-color': 'var(--border-primary)',
        background: 'var(--bg-surface)',
      }}
    >
      {/* Brand + collapse toggle */}
      <div
        class="flex items-center px-2.5 py-3"
        classList={{ 'justify-center': collapsed(), 'justify-between': !collapsed() }}
      >
        <Show when={!collapsed()}>
          <button
            class="px-1 text-base font-semibold tracking-tight transition-opacity hover:opacity-80"
            style={{ color: 'var(--accent)' }}
            onClick={() => navigate('/')}
          >
            ArcRun
          </button>
        </Show>
        <button
          title={collapsed() ? 'Expand sidebar' : 'Collapse sidebar'}
          aria-label={collapsed() ? 'Expand sidebar' : 'Collapse sidebar'}
          aria-expanded={!collapsed()}
          class="icon-btn"
          onClick={toggleCollapsed}
        >
          <IconPanelLeft size={15} />
        </button>
      </div>

      {/* Command palette trigger */}
      <div class="px-2">
        <button
          title="Command palette (Cmd+K)"
          aria-label="Open command palette"
          class="flex w-full items-center gap-2 rounded-md border border-white/10 bg-white/5 px-2 py-1.5 text-xs text-white/40 transition-colors hover:border-white/20 hover:text-white/60"
          classList={{ 'justify-center': collapsed() }}
          onClick={() => setPaletteOpen(true)}
        >
          <IconSearch size={13} />
          <Show when={!collapsed()}>
            <span class="flex-1 text-left">Search...</span>
            <kbd class="rounded border border-white/15 bg-white/5 px-1 py-0.5 font-mono text-[9px] text-white/40">
              &#8984;K
            </kbd>
          </Show>
        </button>
      </div>

      {/* Navigation */}
      <nav class="mt-3 px-2">
        <button
          title="All batches"
          class="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs font-medium transition-colors"
          classList={{
            'justify-center': collapsed(),
            'bg-white/10 text-white': isHome(),
            'text-white/60 hover:bg-white/5 hover:text-white/90': !isHome(),
          }}
          onClick={() => navigate('/')}
        >
          <IconLayers size={14} />
          <Show when={!collapsed()}>
            <span>All batches</span>
          </Show>
        </button>
      </nav>

      {/* Recent batches with live status */}
      <Show when={!collapsed() && recentIds().length > 0}>
        <div class="mt-4 flex min-h-0 flex-1 flex-col overflow-hidden px-2">
          <h2 class="mb-1.5 px-2 text-[10px] font-medium uppercase tracking-wider text-white/30">
            Recent
          </h2>
          <div class="flex-1 space-y-0.5 overflow-y-auto">
            <For each={recentIds().slice(0, MAX_RECENTS_SHOWN)}>
              {(bid) => {
                const summary = () => summaries().get(bid);
                const isActive = () => activeBatchId() === bid;
                return (
                  <button
                    title={bid}
                    class="block w-full rounded-md px-2 py-1.5 text-left transition-colors"
                    classList={{
                      'bg-white/10': isActive(),
                      'hover:bg-white/5': !isActive(),
                    }}
                    onClick={() => navigate(`/batch/${bid}`)}
                  >
                    <div class="flex items-center justify-between gap-2">
                      <span
                        class="truncate font-mono text-[11px]"
                        classList={{
                          'text-rose-400': isActive(),
                          'text-white/70': !isActive(),
                        }}
                      >
                        {bid.substring(0, 13)}
                      </span>
                      <Show when={summary()}>
                        {(s) => (
                          <span class="shrink-0 text-[9px] tabular-nums text-white/30">
                            {s().total_tasks}
                          </span>
                        )}
                      </Show>
                    </div>
                    <Show when={summary()}>
                      {(s) => (
                        <BatchMiniProgress
                          counts={s().status_counts}
                          total={s().total_tasks}
                          class="mt-1"
                        />
                      )}
                    </Show>
                  </button>
                );
              }}
            </For>
          </div>
        </div>
      </Show>
      <Show when={collapsed()}>
        <div class="flex-1" />
      </Show>

      {/* Footer: theme toggle */}
      <div class="border-t px-2 py-2" style={{ 'border-color': 'var(--border-secondary)' }}>
        <button
          title={`Switch to ${theme() === 'dark' ? 'light' : 'dark'} mode`}
          aria-label={`Switch to ${theme() === 'dark' ? 'light' : 'dark'} mode`}
          class="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs text-white/50 transition-colors hover:bg-white/5 hover:text-white/80"
          classList={{ 'justify-center': collapsed() }}
          onClick={toggleTheme}
        >
          {theme() === 'dark' ? <IconSun size={14} /> : <IconMoon size={14} />}
          <Show when={!collapsed()}>
            <span>{theme() === 'dark' ? 'Light mode' : 'Dark mode'}</span>
          </Show>
        </button>
      </div>
    </aside>
  );
}
