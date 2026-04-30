"use client";

import { USER_ID } from "@/lib/constants";

import { Button } from "@/components/ui/button";

type IdentityBarProps = {
  strategyId: string;
  disabled?: boolean;
  onNewStrategy: () => void | Promise<void>;
};

export function IdentityBar({
  strategyId,
  disabled = false,
  onNewStrategy,
}: IdentityBarProps) {
  return (
    <div className="flex flex-col gap-4 border-b bg-muted/30 px-4 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-5">
      <div className="space-y-1">
        <p className="text-xs font-medium uppercase tracking-[0.2em] text-muted-foreground">
          Strategy tester
        </p>
        <div className="flex flex-col gap-1 text-sm sm:flex-row sm:items-center sm:gap-3">
          <span className="font-medium text-foreground">{USER_ID}</span>
          <span className="hidden text-muted-foreground sm:inline">/</span>
          <span className="font-mono text-xs text-muted-foreground sm:text-sm">
            {strategyId}
          </span>
        </div>
      </div>

      <Button
        variant="outline"
        onClick={() => void onNewStrategy()}
        disabled={disabled}
      >
        New strategy
      </Button>
    </div>
  );
}
