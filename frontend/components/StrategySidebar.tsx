"use client";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
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
    <Card className="overflow-hidden border-border/70 bg-background shadow-sm lg:sticky lg:top-6 lg:h-[calc(100vh-3rem)]">
      <div className="flex items-center justify-between gap-3 border-b bg-muted/30 px-4 py-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-[0.2em] text-muted-foreground">
            Strategies
          </p>
          <p className="mt-1 text-sm text-foreground">
            Switch between locally known strategy threads.
          </p>
        </div>

        <Button
          variant="outline"
          size="sm"
          onClick={() => void onNewStrategy()}
          disabled={disabled}
        >
          New
        </Button>
      </div>

      <ScrollArea className="max-h-80 lg:h-[calc(100vh-9rem)] lg:max-h-none">
        <div className="flex flex-col gap-2 p-3">
          {strategies.length === 0 ? (
            <div className="rounded-xl border border-dashed border-border bg-muted/20 px-4 py-8 text-center text-sm text-muted-foreground">
              Known strategies will appear here.
            </div>
          ) : null}

          {strategies.map((strategy) => {
            const isActive = strategy.strategy_id === activeStrategyId;

            return (
              <button
                key={strategy.strategy_id}
                type="button"
                onClick={() => onSelectStrategy(strategy.strategy_id)}
                disabled={disabled}
                className={cn(
                  "rounded-xl border px-3 py-3 text-left transition-colors disabled:pointer-events-none disabled:opacity-50",
                  isActive
                    ? "border-primary/30 bg-primary/10 text-foreground shadow-sm"
                    : "border-transparent bg-background hover:bg-muted/60",
                )}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-medium text-foreground">
                      {strategy.label}
                    </p>
                    <p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">
                      {strategy.strategy_id}
                    </p>
                  </div>

                  {isActive ? (
                    <span className="rounded-full bg-primary/15 px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.18em] text-primary">
                      Active
                    </span>
                  ) : null}
                </div>
              </button>
            );
          })}
        </div>
      </ScrollArea>
    </Card>
  );
}
