"use client";

import { MessageSquare, Plus } from "lucide-react";

import { ScrollArea } from "@/components/ui/scroll-area";
import { ThemeToggle } from "@/components/ThemeToggle";
import { KnownStrategy } from "@/lib/local-store";
import { cn } from "@/lib/utils";

type StrategySidebarProps = {
  strategies: KnownStrategy[];
  activeStrategyId: string;
  disabled?: boolean;
  onSelectStrategy: (strategyId: string) => void;
  onNewStrategy: () => void | Promise<void>;
};

export function StrategySidebar({
  strategies,
  activeStrategyId,
  disabled = false,
  onSelectStrategy,
  onNewStrategy,
}: StrategySidebarProps) {
  return (
    <aside className="hidden h-dvh w-64 shrink-0 flex-col bg-sidebar text-sidebar-foreground md:flex">
      <div className="flex items-center gap-2 px-3 py-3">
        <span className="flex size-7 shrink-0 items-center justify-center rounded-md bg-sidebar-accent font-heading text-sm font-bold">
          P
        </span>
        <span className="font-heading text-sm font-semibold tracking-tight">
          <span>Pond</span>
          <span className="text-muted-foreground">3r</span>
        </span>
      </div>

      <div className="px-2">
        <button
          type="button"
          onClick={() => void onNewStrategy()}
          disabled={disabled}
          className="flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-sm font-medium transition-colors hover:bg-sidebar-accent disabled:pointer-events-none disabled:opacity-50"
        >
          <Plus className="size-4 shrink-0" />
          New strategy
        </button>
      </div>

      <div className="px-4 pb-1 pt-4">
        <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
          Strategies
        </p>
      </div>

      <ScrollArea className="min-h-0 flex-1">
        <div className="flex flex-col gap-0.5 px-2 pb-3">
          {strategies.length === 0 ? (
            <p className="px-2.5 py-2 text-xs text-muted-foreground">
              Known strategies will appear here.
            </p>
          ) : null}

          {strategies.map((strategy) => {
            const isActive = strategy.strategy_id === activeStrategyId;

            return (
              <button
                key={strategy.strategy_id}
                type="button"
                onClick={() => onSelectStrategy(strategy.strategy_id)}
                disabled={disabled}
                title={strategy.strategy_id}
                className={cn(
                  "flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm transition-colors disabled:pointer-events-none disabled:opacity-50",
                  isActive
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent/60",
                )}
              >
                <MessageSquare className="size-4 shrink-0 text-muted-foreground" />
                <span className="truncate">{strategy.label}</span>
              </button>
            );
          })}
        </div>
      </ScrollArea>

      <div className="border-t border-sidebar-border p-2">
        <ThemeToggle />
      </div>
    </aside>
  );
}
