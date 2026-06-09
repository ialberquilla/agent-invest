"use client";

import { useSyncExternalStore } from "react";
import { Moon, Sun } from "lucide-react";

import { cn } from "@/lib/utils";

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

export function ThemeToggle({
  compact = false,
  surface = "sidebar",
}: {
  compact?: boolean;
  surface?: "sidebar" | "topbar";
}) {
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

  const label = isDark ? "Light mode" : "Dark mode";

  return (
    <button
      type="button"
      onClick={toggle}
      aria-label={label}
      title={compact ? label : undefined}
      className={cn(
        "flex items-center text-sm transition-colors",
        surface === "topbar"
          ? "rounded-full border border-border/70 bg-muted/45 text-muted-foreground shadow-xs hover:bg-muted hover:text-foreground"
          : "w-full rounded-lg py-2 text-sidebar-foreground hover:bg-sidebar-accent/60",
        compact ? "size-8 justify-center p-0" : "gap-2.5 px-2.5 py-2",
      )}
    >
      {isDark ? (
        <Sun className="size-4 shrink-0" />
      ) : (
        <Moon className="size-4 shrink-0" />
      )}
      {compact ? null : label}
    </button>
  );
}
