"use client";

import { Plus } from "lucide-react";

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
    <div className="flex h-12 shrink-0 items-center gap-3 border-b border-border/60 px-3 sm:px-4">
      <span className="truncate font-mono text-xs text-muted-foreground">
        {strategyId}
      </span>

      <Button
        variant="ghost"
        size="icon-sm"
        className="ml-auto md:hidden"
        onClick={() => void onNewStrategy()}
        disabled={disabled}
        aria-label="New strategy"
      >
        <Plus />
      </Button>
    </div>
  );
}
