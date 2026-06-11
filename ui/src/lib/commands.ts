/**
 * Global command palette store.
 *
 * Pages register their contextual commands (and optionally a task source)
 * on mount and clear them on cleanup; the CommandPalette component renders
 * whatever is currently registered.
 */
import { createSignal } from 'solid-js';
import type { BasicTask } from '../types';

export interface PaletteCommand {
  id: string;
  label: string;
  /** Section header in the palette list. */
  group: string;
  /** Optional right-aligned hint (keyboard shortcut). */
  hint?: string;
  /** Extra match terms beyond the label. */
  keywords?: string;
  action: () => void;
}

export interface TaskSource {
  tasks: () => BasicTask[];
  open: (task: BasicTask) => void;
}

export const [paletteOpen, setPaletteOpen] = createSignal(false);
export const [pageCommands, setPageCommands] = createSignal<PaletteCommand[]>([]);
export const [taskSource, setTaskSource] = createSignal<TaskSource | null>(null);
