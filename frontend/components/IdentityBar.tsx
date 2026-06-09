"use client";

import { Plus } from "lucide-react";

import { Button } from "@/components/ui/button";

type IdentityBarProps = {
  strategyId: string;
  disabled?: boolean;
  onNewStrategy: () => void | Promise<void>;
};

export function IdentityBar({
  disabled = false,
  onNewStrategy,
}: IdentityBarProps) {
  return (
    <div className="flex h-12 shrink-0 items-center justify-end border-b border-border/60 px-3 sm:px-4 md:hidden">
      <Button
        variant="ghost"
        size="icon-sm"
        onClick={() => void onNewStrategy()}
        disabled={disabled}
        aria-label="New strategy"
      >
        <Plus />
      </Button>
    </div>
  );
}
