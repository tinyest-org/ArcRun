import "glass-ui-solid/styles.css";
import "glass-ui-solid/theme.css";
import { createSignal, createContext, useContext, onMount } from 'solid-js';
import type { JSX } from 'solid-js';
import ToastContainer from './components/ToastContainer';
import ErrorBoundary from './components/ErrorBoundary';
import Sidebar from './components/Sidebar';
import CommandPalette from './components/CommandPalette';
import { getTheme, setTheme as persistTheme, type Theme } from './storage';

interface ThemeCtx {
  theme: () => Theme;
  toggle: () => void;
}

const ThemeContext = createContext<ThemeCtx>({
  theme: () => 'dark' as Theme,
  toggle: () => {},
});

export function useTheme() {
  return useContext(ThemeContext);
}

export default function App(props: { children?: JSX.Element }) {
  const [theme, setTheme] = createSignal<Theme>(getTheme());

  onMount(() => {
    document.documentElement.setAttribute('data-theme', theme());
    document.documentElement.classList.toggle('dark', theme() === 'dark');
  });

  function toggle() {
    const next = theme() === 'dark' ? 'light' : 'dark';
    setTheme(next);
    persistTheme(next);
    document.documentElement.setAttribute('data-theme', next);
    document.documentElement.classList.toggle('dark', next === 'dark');
  }

  return (
    <ThemeContext.Provider value={{ theme, toggle }}>
      <div
        class="flex h-screen transition-colors duration-300"
        style={{ background: 'var(--bg-app)' }}
      >
        <ErrorBoundary>
          <Sidebar />
          <div class="flex min-w-0 flex-1 flex-col overflow-hidden">
            {props.children}
          </div>
        </ErrorBoundary>
        <CommandPalette />
        <ToastContainer />
      </div>
    </ThemeContext.Provider>
  );
}
