import { createSignal, createEffect, createMemo, on, onMount, onCleanup, For, Show } from 'solid-js';
import { useNavigate } from '@solidjs/router';
import {
  paletteOpen,
  setPaletteOpen,
  pageCommands,
  taskSource,
  type PaletteCommand,
} from '../lib/commands';
import { listBatches } from '../api';
import { getRecentBatches } from '../storage';
import { useTheme } from '../App';
import { STATUS_COLORS } from '../constants';
import type { BasicTask, BatchSummary } from '../types';
import { IconSearch, IconLayers, IconClock, IconCornerDownLeft } from './icons';

interface PaletteItem {
  key: string;
  group: string;
  label: string;
  hint?: string;
  /** Colored dot shown before the label (task status). */
  dotColor?: string;
  mono?: boolean;
  run: () => void;
}

const MAX_TASKS = 8;
const MAX_BATCHES = 8;
const SEARCH_DEBOUNCE_MS = 250;

function matches(query: string, ...haystacks: (string | undefined)[]): boolean {
  if (!query) return true;
  const q = query.toLowerCase();
  return haystacks.some((h) => h && h.toLowerCase().includes(q));
}

export default function CommandPalette() {
  const navigate = useNavigate();
  const { toggle: toggleTheme } = useTheme();

  const [query, setQuery] = createSignal('');
  const [selected, setSelected] = createSignal(0);
  const [batchResults, setBatchResults] = createSignal<BatchSummary[]>([]);
  const [searching, setSearching] = createSignal(false);

  let inputRef: HTMLInputElement | undefined;
  let listRef: HTMLDivElement | undefined;
  let debounceTimer: ReturnType<typeof setTimeout> | undefined;
  let searchSeq = 0;

  const globalCommands = (): PaletteCommand[] => [
    {
      id: 'go-home',
      label: 'Go to all batches',
      group: 'Navigation',
      keywords: 'home list batches',
      action: () => navigate('/'),
    },
    {
      id: 'toggle-theme',
      label: 'Toggle light / dark theme',
      group: 'Preferences',
      keywords: 'dark light mode theme',
      action: toggleTheme,
    },
  ];

  // Async batch search by name (only with a query of 2+ chars)
  createEffect(
    on(
      () => [paletteOpen(), query()] as const,
      ([open, q]) => {
        clearTimeout(debounceTimer);
        const trimmed = q.trim();
        if (!open || trimmed.length < 2) {
          setBatchResults([]);
          setSearching(false);
          return;
        }
        setSearching(true);
        const seq = ++searchSeq;
        debounceTimer = setTimeout(async () => {
          try {
            const results = await listBatches(0, MAX_BATCHES, { name: trimmed });
            if (seq === searchSeq) setBatchResults(results);
          } catch {
            if (seq === searchSeq) setBatchResults([]);
          } finally {
            if (seq === searchSeq) setSearching(false);
          }
        }, SEARCH_DEBOUNCE_MS);
      },
    ),
  );

  const items = createMemo<PaletteItem[]>(() => {
    if (!paletteOpen()) return [];
    const q = query().trim();
    const out: PaletteItem[] = [];

    // Commands (page-contextual first, then global)
    for (const cmd of [...pageCommands(), ...globalCommands()]) {
      if (!matches(q, cmd.label, cmd.keywords)) continue;
      out.push({
        key: `cmd:${cmd.id}`,
        group: cmd.group,
        label: cmd.label,
        hint: cmd.hint,
        run: cmd.action,
      });
    }

    // Tasks in the current batch (only when searching)
    const source = taskSource();
    if (source && q) {
      const found = source.tasks().filter((t) => matches(q, t.name, t.kind, t.id));
      for (const task of found.slice(0, MAX_TASKS)) {
        out.push({
          key: `task:${task.id}`,
          group: 'Tasks in this batch',
          label: task.name,
          hint: task.status,
          dotColor: STATUS_COLORS[task.status],
          run: () => source.open(task),
        });
      }
    }

    // Batches: recents when idle, server search results when querying
    if (!q) {
      for (const bid of getRecentBatches().slice(0, MAX_BATCHES)) {
        out.push({
          key: `batch:${bid}`,
          group: 'Recent batches',
          label: bid,
          mono: true,
          run: () => navigate(`/batch/${bid}`),
        });
      }
    } else {
      for (const batch of batchResults()) {
        out.push({
          key: `batch:${batch.batch_id}`,
          group: 'Batches',
          label: batch.batch_id,
          hint: `${batch.total_tasks} tasks`,
          mono: true,
          run: () => navigate(`/batch/${batch.batch_id}`),
        });
      }
    }

    return out;
  });

  // Reset selection whenever the result list changes
  createEffect(on(items, () => setSelected(0)));

  // Open/close lifecycle
  createEffect(
    on(paletteOpen, (open) => {
      if (open) {
        setQuery('');
        setSelected(0);
        queueMicrotask(() => inputRef?.focus());
      }
    }),
  );

  // Keep the selected item in view
  createEffect(() => {
    const idx = selected();
    const el = listRef?.querySelector<HTMLElement>(`[data-index="${idx}"]`);
    el?.scrollIntoView({ block: 'nearest' });
  });

  function close() {
    setPaletteOpen(false);
  }

  function runItem(item: PaletteItem) {
    close();
    item.run();
  }

  function handleGlobalKeyDown(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      setPaletteOpen(!paletteOpen());
    }
  }

  onMount(() => window.addEventListener('keydown', handleGlobalKeyDown));
  onCleanup(() => {
    window.removeEventListener('keydown', handleGlobalKeyDown);
    clearTimeout(debounceTimer);
  });

  function handleOverlayKeyDown(e: KeyboardEvent) {
    const list = items();
    switch (e.key) {
      case 'Escape':
        e.preventDefault();
        e.stopPropagation();
        close();
        break;
      case 'ArrowDown':
        e.preventDefault();
        if (list.length > 0) setSelected((i) => (i + 1) % list.length);
        break;
      case 'ArrowUp':
        e.preventDefault();
        if (list.length > 0) setSelected((i) => (i - 1 + list.length) % list.length);
        break;
      case 'Enter': {
        e.preventDefault();
        const item = list[selected()];
        if (item) runItem(item);
        break;
      }
    }
  }

  return (
    <Show when={paletteOpen()}>
      <div
        class="fixed inset-0 z-[110] bg-black/60 backdrop-blur-[2px]"
        onClick={(e) => {
          if (e.target === e.currentTarget) close();
        }}
        onKeyDown={handleOverlayKeyDown}
      >
        <div
          role="dialog"
          aria-modal="true"
          aria-label="Command palette"
          class="fade-in-up mx-auto mt-[14vh] w-full max-w-lg overflow-hidden rounded-xl border border-white/15 bg-[#12122a] shadow-2xl"
        >
          {/* Search input */}
          <div class="flex items-center gap-2.5 border-b border-white/10 px-4 py-3">
            <span class="text-white/40">
              <IconSearch size={15} />
            </span>
            <input
              ref={inputRef}
              type="text"
              value={query()}
              onInput={(e) => setQuery(e.currentTarget.value)}
              placeholder="Search batches, tasks, or type a command..."
              class="flex-1 bg-transparent text-sm text-white/90 placeholder-white/30 outline-none"
              aria-label="Command palette search"
            />
            <Show when={searching()}>
              <span class="text-[10px] text-white/30">searching...</span>
            </Show>
          </div>

          {/* Results */}
          <div ref={listRef} class="max-h-[50vh] overflow-y-auto p-1.5">
            <Show
              when={items().length > 0}
              fallback={
                <div class="px-3 py-6 text-center text-xs text-white/40">
                  {searching() ? 'Searching...' : 'No results'}
                </div>
              }
            >
              <For each={items()}>
                {(item, i) => (
                  <>
                    <Show when={i() === 0 || items()[i() - 1].group !== item.group}>
                      <div class="px-2.5 pb-1 pt-2 text-[10px] font-medium uppercase tracking-wider text-white/30">
                        {item.group}
                      </div>
                    </Show>
                    <button
                      data-index={i()}
                      class="flex w-full items-center gap-2 rounded-md px-2.5 py-1.5 text-left text-sm transition-colors"
                      classList={{
                        'bg-white/10 text-white': selected() === i(),
                        'text-white/70': selected() !== i(),
                      }}
                      onMouseMove={() => setSelected(i())}
                      onClick={() => runItem(item)}
                    >
                      <Show when={item.dotColor}>
                        <span
                          class="inline-block h-2 w-2 shrink-0 rounded-full"
                          style={{ background: item.dotColor }}
                        />
                      </Show>
                      <Show when={item.key.startsWith('batch:')}>
                        <span class="shrink-0 text-white/30">
                          {item.group === 'Recent batches' ? (
                            <IconClock size={12} />
                          ) : (
                            <IconLayers size={12} />
                          )}
                        </span>
                      </Show>
                      <span
                        class="flex-1 truncate"
                        classList={{ 'font-mono text-xs': item.mono }}
                      >
                        {item.label}
                      </span>
                      <Show when={item.hint}>
                        <span class="shrink-0 text-[10px] text-white/40">{item.hint}</span>
                      </Show>
                      <Show when={selected() === i()}>
                        <span class="shrink-0 text-white/30">
                          <IconCornerDownLeft size={11} />
                        </span>
                      </Show>
                    </button>
                  </>
                )}
              </For>
            </Show>
          </div>

          {/* Footer */}
          <div class="flex items-center gap-3 border-t border-white/10 px-4 py-2 text-[10px] text-white/30">
            <span>
              <kbd class="rounded border border-white/15 bg-white/5 px-1 font-mono">&#8593;&#8595;</kbd>{' '}
              navigate
            </span>
            <span>
              <kbd class="rounded border border-white/15 bg-white/5 px-1 font-mono">&#8629;</kbd>{' '}
              select
            </span>
            <span>
              <kbd class="rounded border border-white/15 bg-white/5 px-1 font-mono">esc</kbd> close
            </span>
          </div>
        </div>
      </div>
    </Show>
  );
}
