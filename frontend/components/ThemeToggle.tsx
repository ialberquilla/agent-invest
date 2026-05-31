"use client";

import { useSyncExternalStore } from "react";
import { Moon, Sun } from "lucide-react";

const THEME_KEY = "agent-invest:theme";

// The theme lives on <html class="dark">, applied before paint by the inline
// script in the document head. We read it via useSyncExternalStore so the
// toggle stays in sync without a setState-in-effect cascade.
function subscribe(onChange: () => void) {
  const observer = new MutationObserver(onChange);
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["class"],
  });
  return () => observer.disconnect();
}

function getIsDark() {
  return document.documentElement.classList.contains("dark");
}

export function ThemeToggle() {
  // Default to dark on the server snapshot to match the pre-paint default.
  const isDark = useSyncExternalStore(subscribe, getIsDark, () => true);

  function toggle() {
    const next = !isDark;
    document.documentElement.classList.toggle("dark", next);
    try {
      localStorage.setItem(THEME_KEY, next ? "dark" : "light");
    } catch {
      // Ignore storage failures; the in-memory toggle still applies.
    }
  }

  return (
    <button
      type="button"
      onClick={toggle}
      className="flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-sm text-sidebar-foreground transition-colors hover:bg-sidebar-accent/60"
    >
      {isDark ? (
        <Sun className="size-4 shrink-0" />
      ) : (
        <Moon className="size-4 shrink-0" />
      )}
      {isDark ? "Light mode" : "Dark mode"}
    </button>
  );
}
