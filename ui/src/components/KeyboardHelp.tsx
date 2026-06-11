import { Show, For } from 'solid-js';
import { Card } from 'glass-ui-solid';

interface Props {
  open: boolean;
  onClose: () => void;
}

const SHORTCUTS: { key: string; description: string }[] = [
  { key: '⌘K', description: 'Command palette' },
  { key: '1', description: '2D DAG view' },
  { key: '2', description: '3D isometric view' },
  { key: '3', description: 'Table view' },
  { key: '4', description: 'Timeline view' },
  { key: 'F', description: 'Fit / reset camera' },
  { key: 'C', description: 'Toggle critical path' },
  { key: 'R', description: 'Refresh now' },
  { key: 'Esc', description: 'Close panels / modals' },
  { key: '?', description: 'Show this help' },
];

export default function KeyboardHelp(props: Props) {
  return (
    <Show when={props.open}>
      <div
        class="fixed inset-0 z-[100] flex items-center justify-center bg-black/60"
        onClick={(e) => {
          if (e.target === e.currentTarget) props.onClose();
        }}
      >
        <Card class="w-full max-w-sm p-6">
          <h2 class="mb-4 text-lg font-semibold text-white">Keyboard Shortcuts</h2>
          <div class="space-y-2">
            <For each={SHORTCUTS}>
              {(s) => (
                <div class="flex items-center justify-between">
                  <span class="text-sm text-white/70">{s.description}</span>
                  <kbd class="rounded border border-white/20 bg-white/10 px-2 py-0.5 font-mono text-xs text-white/80">
                    {s.key}
                  </kbd>
                </div>
              )}
            </For>
          </div>
          <div class="mt-5 text-right">
            <button
              class="rounded-md border border-white/20 px-3 py-1 text-xs text-white/60 hover:bg-white/10"
              onClick={props.onClose}
            >
              Close
            </button>
          </div>
        </Card>
      </div>
    </Show>
  );
}
