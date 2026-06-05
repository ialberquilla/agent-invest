"use client";

import { useState } from "react";
import {
  BarChart3,
  ChevronDown,
  MessageSquare,
  Pencil,
  Wallet,
} from "lucide-react";

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
  const [openSection, setOpenSection] = useState<"screeners" | "deployed" | null>(
    null,
  );

  function toggleSection(section: "screeners" | "deployed") {
    setOpenSection((current) => (current === section ? null : section));
  }

  return (
    <aside className="hidden h-dvh w-64 shrink-0 flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground md:flex">
      <div className="flex items-center gap-2 px-3 py-3">
        <span className="flex size-7 shrink-0 items-center justify-center rounded-md bg-sidebar-accent font-heading text-sm font-bold">
          P
        </span>
        <span className="font-heading text-sm font-semibold tracking-tight">
          <span>Pond</span>
          <span className="text-sidebar-foreground/75">3r</span>
        </span>
      </div>

      <nav className="flex flex-col gap-0.5 px-2">
        <button
          type="button"
          onClick={() => void onNewStrategy()}
          disabled={disabled}
          className="flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-sm font-medium transition-colors hover:bg-sidebar-accent disabled:pointer-events-none disabled:opacity-50"
        >
          <Pencil className="size-4 shrink-0" />
          New chat
        </button>

        <button
          type="button"
          onClick={() => toggleSection("screeners")}
          className="flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm font-medium transition-colors hover:bg-sidebar-accent"
          aria-expanded={openSection === "screeners"}
        >
          <BarChart3 className="size-4 shrink-0" />
          <span className="min-w-0 flex-1 truncate">Screeners</span>
          <ChevronDown
            className={cn(
              "size-3.5 shrink-0 text-sidebar-foreground/70 transition-transform",
              openSection === "screeners" && "rotate-180",
            )}
          />
        </button>

        {openSection === "screeners" ? (
          <div className="flex flex-col gap-0.5 pl-6">
            {screeners.length === 0 ? (
              <p className="px-2.5 py-2 text-sm text-sidebar-foreground/65">
                No screeners yet
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
                    "flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-50",
                    isActive
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
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
        ) : null}

        <button
          type="button"
          onClick={() => toggleSection("deployed")}
          className="flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm font-medium transition-colors hover:bg-sidebar-accent"
          aria-expanded={openSection === "deployed"}
        >
          <Wallet className="size-4 shrink-0" />
          <span className="min-w-0 flex-1 truncate">Deployed strategies</span>
          <ChevronDown
            className={cn(
              "size-3.5 shrink-0 text-sidebar-foreground/70 transition-transform",
              openSection === "deployed" && "rotate-180",
            )}
          />
        </button>

        {openSection === "deployed" ? (
          <div className="flex flex-col gap-0.5 pl-6">
            {deployedStrategies.length === 0 ? (
              <p className="px-2.5 py-2 text-sm text-sidebar-foreground/65">
                No deployed strategies yet
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
        ) : null}
      </nav>

      <div className="px-4 pb-1 pt-6">
        <p className="text-sm font-semibold">Recents</p>
      </div>

      <ScrollArea className="min-h-0 flex-1">
        <div className="flex flex-col gap-0.5 px-2 pb-3">
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
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
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
