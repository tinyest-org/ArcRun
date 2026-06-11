import { For } from 'solid-js';

export interface FilterItem {
  key: string;
  label: string;
  count: number;
  color?: string;
}

interface Props {
  items: FilterItem[];
  active: Set<string>;
  onToggle: (key: string) => void;
  activeClass?: string;
}

export default function ToggleFilter(props: Props) {
  const activeStyle = props.activeClass ?? 'border-cyan-400/40 bg-cyan-400/15 text-cyan-300';

  return (
    <div class="flex flex-wrap gap-1.5">
      <For each={props.items}>
        {(item) => {
          const isActive = () => props.active.has(item.key);
          return (
            <button
              class="flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium transition-colors"
              aria-pressed={isActive()}
              title={`${isActive() ? 'Remove' : 'Add'} filter: ${item.label}`}
              classList={{
                'border-white/20 bg-white/5 text-white/60 hover:bg-white/10': !isActive(),
                [activeStyle]: isActive(),
              }}
              style={isActive() && item.color ? { 'background-color': `${item.color}33` } : undefined}
              onClick={() => props.onToggle(item.key)}
            >
              {item.color && (
                <span
                  class="inline-block h-2 w-2 rounded-full"
                  style={{ 'background-color': item.color }}
                />
              )}
              {item.label}
              <span class="text-white/40">{item.count}</span>
            </button>
          );
        }}
      </For>
    </div>
  );
}
