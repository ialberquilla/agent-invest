"use client";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import type { TimelinePart, ToolStatus } from "@/lib/agent-events";
import { cn } from "@/lib/utils";

const TOOL_STATUS_LABEL: Record<ToolStatus, string> = {
  pending: "queued",
  running: "running",
  completed: "done",
  error: "error",
};

const TOOL_STATUS_TONE: Record<ToolStatus, string> = {
  pending: "bg-muted text-muted-foreground",
  running: "bg-blue-500/15 text-blue-700 dark:text-blue-300",
  completed: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300",
  error: "bg-destructive/15 text-destructive",
};

export type LiveActivityProps = {
  parts: TimelinePart[];
};

export function LiveActivity({ parts }: LiveActivityProps) {
  const visibleParts = parts.filter(isVisible);
  if (visibleParts.length === 0) return null;

  return (
    <div className="flex justify-start">
      <Card
        size="sm"
        className="max-w-[90%] border-dashed bg-muted/30 py-3 sm:max-w-[80%]"
      >
        <CardContent className="space-y-3">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            agent activity
          </p>
          <div className="space-y-2">
            {visibleParts.map((part) => (
              <PartView key={part.id} part={part} />
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function isVisible(part: TimelinePart): boolean {
  if (part.kind === "step-start" || part.kind === "step-finish") return false;
  if (part.kind === "reasoning" || part.kind === "text") {
    return part.text.trim().length > 0;
  }
  return true;
}

function PartView({ part }: { part: TimelinePart }) {
  if (part.kind === "reasoning") {
    return (
      <div className="rounded-md border-l-2 border-amber-400/50 bg-background/50 px-3 py-2">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-amber-700 dark:text-amber-400">
          reasoning {part.completed ? null : <span aria-hidden>…</span>}
        </p>
        <pre className="mt-1 whitespace-pre-wrap break-words font-sans text-xs italic leading-5 text-muted-foreground">
          {part.text}
        </pre>
      </div>
    );
  }

  if (part.kind === "tool") {
    const statusLabel = TOOL_STATUS_LABEL[part.status] ?? part.status;
    const tone = TOOL_STATUS_TONE[part.status] ?? "bg-muted text-muted-foreground";
    return (
      <div className="rounded-md border border-border bg-background/60 px-3 py-2">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline" className="font-mono text-[10px] uppercase">
            {part.name}
          </Badge>
          <Badge className={cn("text-[10px]", tone)}>{statusLabel}</Badge>
          {part.description ? (
            <span className="truncate text-xs text-muted-foreground">
              {part.description}
            </span>
          ) : null}
        </div>
        {part.command ? (
          <pre className="mt-2 max-h-32 overflow-auto rounded bg-muted/60 px-2 py-1 font-mono text-[11px] leading-5">
            {part.command}
          </pre>
        ) : null}
        {part.output ? (
          <pre className="mt-2 max-h-40 overflow-auto rounded bg-muted/40 px-2 py-1 font-mono text-[11px] leading-5 text-muted-foreground">
            {truncate(part.output, 1200)}
          </pre>
        ) : null}
        {part.errorMessage ? (
          <p className="mt-2 text-xs text-destructive">{part.errorMessage}</p>
        ) : null}
      </div>
    );
  }

  if (part.kind === "text") {
    return (
      <div className="rounded-md border-l-2 border-primary/40 bg-background/80 px-3 py-2">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-primary/80">
          drafting reply {part.completed ? null : <span aria-hidden>…</span>}
        </p>
        <pre className="mt-1 whitespace-pre-wrap break-words font-sans text-sm leading-6">
          {part.text}
        </pre>
      </div>
    );
  }

  return null;
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text;
  return `…${text.slice(-max)}`;
}
