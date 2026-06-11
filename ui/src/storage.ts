const RECENT_BATCHES_KEY = 'arcrun-recent-batches';
const MAX_RECENT = 10;

export function getRecentBatches(): string[] {
  try {
    const raw = localStorage.getItem(RECENT_BATCHES_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function addRecentBatch(id: string): void {
  const recents = getRecentBatches().filter((b) => b !== id);
  recents.unshift(id);
  if (recents.length > MAX_RECENT) recents.length = MAX_RECENT;
  localStorage.setItem(RECENT_BATCHES_KEY, JSON.stringify(recents));
}

// Sidebar
const SIDEBAR_KEY = 'arcrun-sidebar-collapsed';

export function getSidebarCollapsed(): boolean {
  return localStorage.getItem(SIDEBAR_KEY) === '1';
}

export function setSidebarCollapsed(collapsed: boolean): void {
  localStorage.setItem(SIDEBAR_KEY, collapsed ? '1' : '0');
}

// Theme
const THEME_KEY = 'arcrun-theme';
export type Theme = 'dark' | 'light';

export function getTheme(): Theme {
  const v = localStorage.getItem(THEME_KEY);
  return v === 'light' ? 'light' : 'dark';
}

export function setTheme(theme: Theme): void {
  localStorage.setItem(THEME_KEY, theme);
}
