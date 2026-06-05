"use client";

import { BarChart3, MessageSquare, Plus, Wallet } from "lucide-react";

import { ScrollArea } from "@/components/ui/scroll-area";
import { ThemeToggle } from "@/components/ThemeToggle";
import type { DeployedStrategy, KnownStrategy, PinnedScreener } from "@/lib/local-store";
import { cn } from "@/lib/utils";

type StrategySidebarProps = {
  strategies: KnownStrategy[];
  screeners: PinnedScreener[];
  deployedStrategies: DeployedStrategy[];
  activeStrategyId: string;
  activeScreenerId?: string | null;
  disabled?: boolean;
  onSelectStrategy: (strategyId: string) => void;
  onSelectScreener: (screener: PinnedScreener) => void;
  onNewStrategy: () => void | Promise<void>;
};

export function StrategySidebar({
  strategies = [],
  screeners = [],
  deployedStrategies = [],
  activeStrategyId,
  activeScreenerId = null,
  disabled = false,
  onSelectStrategy,
  onSelectScreener,
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
          <span className="text-sidebar-foreground/75">3r</span>
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
        <p className="text-[11px] font-semibold uppercase tracking-wider text-sidebar-foreground/75">
          Pinned screeners
        </p>
      </div>

      <div className="flex max-h-40 flex-col gap-0.5 overflow-y-auto px-2 pb-3">
        {screeners.length === 0 ? (
          <p className="px-2.5 py-2 text-xs text-sidebar-foreground/75">
            Pin a screener to keep it here.
          </p>
        ) : null}

        {screeners.map((screener) => {
          const isActive = screener.id === activeScreenerId;

          return (
            <button
              key={screener.id}
              type="button"
              onClick={() => onSelectScreener(screener)}
              disabled={disabled}
              title={screener.id}
              className={cn(
                "relative flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-50",
                isActive
                  ? "bg-sidebar-accent text-sidebar-accent-foreground shadow-sm ring-1 ring-sidebar-border before:absolute before:bottom-2 before:left-0 before:top-2 before:w-1 before:rounded-full before:bg-sidebar-primary"
                  : "text-sidebar-foreground hover:bg-sidebar-accent/60",
              )}
            >
              <BarChart3
                className={cn(
                  "size-4 shrink-0",
                  isActive
                    ? "text-sidebar-accent-foreground"
                    : "text-sidebar-foreground/75",
                )}
              />
              <span className="truncate">{screener.label}</span>
            </button>
          );
        })}
      </div>

      <div className="px-4 pb-1 pt-2">
        <p className="text-[11px] font-semibold uppercase tracking-wider text-sidebar-foreground/75">
          Deployed strategies
        </p>
      </div>

      <div className="flex max-h-36 flex-col gap-0.5 overflow-y-auto px-2 pb-3">
        {deployedStrategies.length === 0 ? (
          <p className="px-2.5 py-2 text-xs text-sidebar-foreground/75">
            On-chain vaults will appear here.
          </p>
        ) : null}
        {deployedStrategies.map((strategy) => (
          <div
            key={strategy.mandate_id}
            title={strategy.vault_address}
            className="flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm font-medium text-sidebar-foreground"
          >
            <Wallet className="size-4 shrink-0 text-sidebar-foreground/75" />
            <span className="min-w-0 flex-1 truncate">{strategy.label}</span>
            <span className="rounded-full bg-sidebar-accent px-1.5 py-0.5 text-[10px] uppercase text-sidebar-foreground/75">
              {strategy.status}
            </span>
          </div>
        ))}
      </div>

      <div className="px-4 pb-1 pt-2">
        <p className="text-[11px] font-semibold uppercase tracking-wider text-sidebar-foreground/75">
          Strategies
        </p>
      </div>

      <ScrollArea className="min-h-0 flex-1">
        <div className="flex flex-col gap-0.5 px-2 pb-3">
          {strategies.length === 0 ? (
            <p className="px-2.5 py-2 text-xs text-sidebar-foreground/75">
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
                  "relative flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-50",
                  isActive
                    ? "bg-sidebar-accent text-sidebar-accent-foreground shadow-sm ring-1 ring-sidebar-border before:absolute before:bottom-2 before:left-0 before:top-2 before:w-1 before:rounded-full before:bg-sidebar-primary"
                    : "text-sidebar-foreground hover:bg-sidebar-accent/60",
                )}
              >
                <MessageSquare
                  className={cn(
                    "size-4 shrink-0",
                    isActive
                      ? "text-sidebar-accent-foreground"
                      : "text-sidebar-foreground/75",
                  )}
                />
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
