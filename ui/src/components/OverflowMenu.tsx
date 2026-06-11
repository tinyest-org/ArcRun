import { createSignal, onCleanup, For, Show } from 'solid-js';
import type { JSX } from 'solid-js';
import { IconMore } from './icons';

export interface MenuAction {
  label: string;
  icon?: (props: { size?: number; class?: string }) => JSX.Element;
  danger?: boolean;
  onClick: () => void;
}

export type MenuEntry = MenuAction | 'separator';

interface Props {
  entries: MenuEntry[];
  title?: string;
}

/** Icon-trigger dropdown for secondary actions. Closes on outside click or Escape. */
export default function OverflowMenu(props: Props) {
  const [open, setOpen] = createSignal(false);
  let container: HTMLDivElement | undefined;

  function handleDocumentClick(e: MouseEvent) {
    if (container && !container.contains(e.target as Node)) setOpen(false);
  }

  function handleKeyDown(e: KeyboardEvent) {
    if (e.key === 'Escape') {
      e.stopPropagation();
      setOpen(false);
    }
  }

  document.addEventListener('mousedown', handleDocumentClick);
  onCleanup(() => document.removeEventListener('mousedown', handleDocumentClick));

  function run(action: MenuAction) {
    setOpen(false);
    action.onClick();
  }

  return (
    <div class="relative" ref={container} onKeyDown={handleKeyDown}>
      <button
        title={props.title ?? 'More actions'}
        aria-label={props.title ?? 'More actions'}
        aria-haspopup="menu"
        aria-expanded={open()}
        class="icon-btn"
        classList={{ 'icon-btn-active': open() }}
        onClick={() => setOpen((v) => !v)}
      >
        <IconMore size={15} />
      </button>
      <Show when={open()}>
        <div
          role="menu"
          class="fade-in-up absolute right-0 top-full z-50 mt-1.5 min-w-48 rounded-lg border border-white/15 bg-[#12122a] p-1 shadow-xl"
        >
          <For each={props.entries}>
            {(entry) =>
              entry === 'separator' ? (
                <div class="mx-2 my-1 h-px bg-white/10" />
              ) : (
                <button
                  role="menuitem"
                  class="flex w-full items-center gap-2 rounded-md px-2.5 py-1.5 text-left text-xs transition-colors"
                  classList={{
                    'text-white/80 hover:bg-white/10': !entry.danger,
                    'text-red-400 hover:bg-red-500/15': entry.danger,
                  }}
                  onClick={() => run(entry)}
                >
                  <Show when={entry.icon}>
                    {(icon) => <span class="opacity-70">{icon()({ size: 13 })}</span>}
                  </Show>
                  {entry.label}
                </button>
              )
            }
          </For>
        </div>
      </Show>
    </div>
  );
}
