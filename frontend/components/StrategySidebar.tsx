"use client";

import { useState } from "react";
import {
  Activity,
  BarChart3,
  ChevronDown,
  MessageSquare,
  PanelLeftClose,
  PanelLeftOpen,
  Pencil,
  Trash2,
  Wallet,
} from "lucide-react";

import { ScrollArea } from "@/components/ui/scroll-area";
import type {
  DeployedStrategy,
  KnownStrategy,
  PinnedScreener,
} from "@/lib/local-store";
import { cn } from "@/lib/utils";

type StrategySidebarProps = {
  strategies: KnownStrategy[];
  screeners: PinnedScreener[];
  deployedStrategies: DeployedStrategy[];
  deployedStrategyStatuses?: Record<
    string,
    { label: string; tone: "idle" | "pending" | "live" | "error" }
  >;
  activeStrategyId: string;
  activeScreenerId?: string | null;
  isWalletPositionsActive?: boolean;
  walletPositionCount?: number | null;
  walletPositionStatus?: "idle" | "loading" | "error";
  activeVaultMandateId?: string | null;
  disabled?: boolean;
  deleteDisabled?: boolean;
  onSelectStrategy: (strategyId: string) => void;
  onDeleteStrategy: (strategyId: string) => void;
  onSelectScreener: (screener: PinnedScreener) => void;
  onSelectWalletPositions: () => void;
  onSelectDeployedStrategy: (strategy: DeployedStrategy) => void;
  onNewStrategy: () => void | Promise<void>;
};

export function StrategySidebar({
  strategies = [],
  screeners = [],
  deployedStrategies = [],
  deployedStrategyStatuses = {},
  activeStrategyId,
  activeScreenerId = null,
  isWalletPositionsActive = false,
  walletPositionCount = null,
  walletPositionStatus = "idle",
  activeVaultMandateId = null,
  disabled = false,
  deleteDisabled = disabled,
  onSelectStrategy,
  onDeleteStrategy,
  onSelectScreener,
  onSelectWalletPositions,
  onSelectDeployedStrategy,
  onNewStrategy,
}: StrategySidebarProps) {
  const [openSection, setOpenSection] = useState<
    "screeners" | "deployed" | null
  >(null);
  const [isCollapsed, setIsCollapsed] = useState(false);

  function toggleSection(section: "screeners" | "deployed") {
    if (isCollapsed) {
      setIsCollapsed(false);
      setOpenSection(section);
      return;
    }

    setOpenSection((current) => (current === section ? null : section));
  }

  return (
    <aside
      className={cn(
        "hidden h-dvh shrink-0 flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground transition-all duration-200 md:flex",
        isCollapsed ? "w-14" : "w-64",
      )}
    >
      <div
        className={cn(
          "flex items-center px-3 py-3",
          isCollapsed ? "justify-center" : "gap-2",
        )}
      >
        {isCollapsed ? null : (
          <>
            <span className="flex size-7 shrink-0 items-center justify-center rounded-md bg-sidebar-accent font-heading text-sm font-bold">
              P
            </span>
            <span className="font-heading text-sm font-semibold tracking-tight">
              <span>Pond</span>
              <span className="text-sidebar-foreground/75">3r</span>
            </span>
          </>
        )}
        <button
          type="button"
          onClick={() => setIsCollapsed((current) => !current)}
          aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          title={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          className={cn(
            "flex size-8 shrink-0 items-center justify-center rounded-lg text-sidebar-foreground/75 transition-colors hover:bg-sidebar-accent hover:text-sidebar-foreground",
            !isCollapsed && "ml-auto",
          )}
        >
          {isCollapsed ? (
            <PanelLeftOpen className="size-4" />
          ) : (
            <PanelLeftClose className="size-4" />
          )}
        </button>
      </div>

      <nav className="flex flex-col gap-0.5 px-2">
        <button
          type="button"
          onClick={() => void onNewStrategy()}
          disabled={disabled}
          aria-label="New chat"
          title={isCollapsed ? "New chat" : undefined}
          className={cn(
            "flex w-full items-center rounded-lg py-2 text-sm font-medium transition-colors hover:bg-sidebar-accent disabled:pointer-events-none disabled:opacity-50",
            isCollapsed ? "justify-center px-0" : "gap-2.5 px-2.5",
          )}
        >
          <Pencil className="size-4 shrink-0" />
          {isCollapsed ? null : "New chat"}
        </button>

        <button
          type="button"
          onClick={() => toggleSection("screeners")}
          className={cn(
            "flex w-full items-center rounded-lg py-2 text-left text-sm font-medium transition-colors hover:bg-sidebar-accent",
            isCollapsed ? "justify-center px-0" : "gap-2.5 px-2.5",
          )}
          aria-expanded={openSection === "screeners"}
          aria-label="Screeners"
          title={isCollapsed ? "Screeners" : undefined}
        >
          <BarChart3 className="size-4 shrink-0" />
          {isCollapsed ? null : (
            <>
              <span className="min-w-0 flex-1 truncate">Screeners</span>
              <ChevronDown
                className={cn(
                  "size-3.5 shrink-0 text-sidebar-foreground/70 transition-transform",
                  openSection === "screeners" && "rotate-180",
                )}
              />
            </>
          )}
        </button>

        {!isCollapsed && openSection === "screeners" ? (
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
          onClick={onSelectWalletPositions}
          disabled={disabled}
          className={cn(
            "flex w-full items-center rounded-lg py-2 text-left text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-50",
            isCollapsed ? "justify-center px-0" : "gap-2.5 px-2.5",
            isWalletPositionsActive
              ? "bg-sidebar-accent text-sidebar-accent-foreground"
              : "hover:bg-sidebar-accent",
          )}
          aria-label="Direct positions"
          title={isCollapsed ? "Direct positions" : undefined}
        >
          <Activity className="size-4 shrink-0" />
          {isCollapsed ? null : (
            <>
              <span className="min-w-0 flex-1 truncate">Direct positions</span>
              {walletPositionStatus === "error" ? (
                <span className="rounded-full bg-destructive/10 px-1.5 py-0.5 text-[10px] font-semibold text-destructive">
                  !
                </span>
              ) : walletPositionCount != null ? (
                <span
                  className={cn(
                    "rounded-full px-1.5 py-0.5 text-[10px] font-semibold tabular-nums",
                    walletPositionCount > 0
                      ? "bg-primary text-primary-foreground"
                      : "bg-sidebar-accent text-sidebar-foreground/75",
                  )}
                >
                  {walletPositionCount}
                </span>
              ) : null}
            </>
          )}
        </button>

        <button
          type="button"
          onClick={() => toggleSection("deployed")}
          className={cn(
            "flex w-full items-center rounded-lg py-2 text-left text-sm font-medium transition-colors hover:bg-sidebar-accent",
            isCollapsed ? "justify-center px-0" : "gap-2.5 px-2.5",
          )}
          aria-expanded={openSection === "deployed"}
          aria-label="Deployed strategies"
          title={isCollapsed ? "Deployed strategies" : undefined}
        >
          <Wallet className="size-4 shrink-0" />
          {isCollapsed ? null : (
            <>
              <span className="min-w-0 flex-1 truncate">
                Deployed strategies
              </span>
              <ChevronDown
                className={cn(
                  "size-3.5 shrink-0 text-sidebar-foreground/70 transition-transform",
                  openSection === "deployed" && "rotate-180",
                )}
              />
            </>
          )}
        </button>

        {!isCollapsed && openSection === "deployed" ? (
          <div className="flex flex-col gap-0.5 pl-6">
            {deployedStrategies.length === 0 ? (
              <p className="px-2.5 py-2 text-sm text-sidebar-foreground/65">
                No deployed strategies yet
              </p>
            ) : null}

            {deployedStrategies.map((strategy) => {
              const isActive = strategy.mandate_id === activeVaultMandateId;
              const liveStatus = deployedStrategyStatuses[strategy.mandate_id];
              return (
                <button
                  key={strategy.mandate_id}
                  type="button"
                  onClick={() => onSelectDeployedStrategy(strategy)}
                  disabled={disabled}
                  title={strategy.vault_address}
                  className={cn(
                    "flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-50",
                    isActive
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
                      : "text-sidebar-foreground hover:bg-sidebar-accent/60",
                  )}
                >
                  <Wallet className="size-4 shrink-0 text-sidebar-foreground/75" />
                  <span className="min-w-0 flex-1 truncate">
                    {strategy.label}
                  </span>
                  <span
                    className={cn(
                      "rounded-full px-1.5 py-0.5 text-[10px] uppercase",
                      liveStatus?.tone === "live"
                        ? "bg-emerald-500/15 text-emerald-600 dark:text-emerald-300"
                        : liveStatus?.tone === "pending"
                          ? "bg-amber-500/15 text-amber-600 dark:text-amber-300"
                          : liveStatus?.tone === "error"
                            ? "bg-destructive/15 text-destructive"
                            : "bg-sidebar-accent text-sidebar-foreground/75",
                    )}
                  >
                    {liveStatus?.label ?? strategy.status}
                  </span>
                </button>
              );
            })}
          </div>
        ) : null}
      </nav>

      {isCollapsed ? null : (
        <div className="px-4 pb-1 pt-6">
          <p className="text-sm font-semibold">Recents</p>
        </div>
      )}

      {isCollapsed ? (
        <div className="flex-1" />
      ) : (
        <ScrollArea className="min-h-0 flex-1">
          <div className="flex flex-col gap-0.5 px-2 pb-3">
            {strategies.map((strategy) => {
              const isActive = strategy.strategy_id === activeStrategyId;

              return (
                <div
                  key={strategy.strategy_id}
                  className={cn(
                    "group flex items-center rounded-lg text-sm font-medium transition-colors",
                    isActive
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
                      : "text-sidebar-foreground hover:bg-sidebar-accent/60",
                  )}
                >
                  <button
                    type="button"
                    onClick={() => onSelectStrategy(strategy.strategy_id)}
                    disabled={disabled}
                    title={strategy.strategy_id}
                    className="flex min-w-0 flex-1 items-center gap-2.5 rounded-lg px-2.5 py-2 text-left disabled:pointer-events-none disabled:opacity-50"
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
                  <button
                    type="button"
                    onClick={() => onDeleteStrategy(strategy.strategy_id)}
                    disabled={deleteDisabled}
                    aria-label={`Delete ${strategy.label}`}
                    title="Delete chat"
                    className={cn(
                      "mr-1 flex size-7 shrink-0 items-center justify-center rounded-md text-sidebar-foreground/60 opacity-0 transition hover:bg-sidebar-accent hover:text-sidebar-foreground focus-visible:opacity-100 disabled:pointer-events-none disabled:opacity-30 group-hover:opacity-100",
                      isActive && "text-sidebar-accent-foreground/70",
                    )}
                  >
                    <Trash2 className="size-3.5" />
                  </button>
                </div>
              );
            })}
          </div>
        </ScrollArea>
      )}
    </aside>
  );
}
