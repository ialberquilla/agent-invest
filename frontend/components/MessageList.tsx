"use client";

import { type MouseEvent, useEffect, useRef, useState } from "react";

import { ArtifactGallery } from "@/components/ArtifactGallery";
import { ChatEmptyState } from "@/components/ChatEmptyState";
import { LiveActivity } from "@/components/LiveActivity";
import { StrategyResultReport } from "@/components/StrategyResultReport";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { TimelinePart } from "@/lib/agent-events";
import { ChatMessage } from "@/lib/local-store";
import type { StrategyResult } from "@/lib/types";
import { cn } from "@/lib/utils";

// A structured result is worth rendering as the full report card only
// when it carries real backtest output -- an equity curve or at least
// one finite KPI. A no_viable result (empty kpis/charts) falls back to
// the plain text bubble.
function reportWithData(
  result: StrategyResult | null | undefined,
): StrategyResult | null {
  if (!result) return null;
  const curve = result.charts?.equity_curve;
  const hasCurve = Array.isArray(curve) && curve.length > 0;
  const hasKpi = Object.values(result.kpis ?? {}).some(
    (value) => typeof value === "number" && Number.isFinite(value),
  );
  return hasCurve || hasKpi ? result : null;
}

type MessageListProps = {
  messages: ChatMessage[];
  isThinking: boolean;
  liveRunId?: string | null;
  liveParts?: TimelinePart[];
  onInspectRun?: (runId: string, trigger: HTMLButtonElement) => void;
  emptyStateDisabled?: boolean;
  onSelectPrompt?: (prompt: string) => void;
  onOpenWizard?: () => void;
};

export function MessageList({
  messages,
  isThinking,
  liveRunId = null,
  liveParts = [],
  onInspectRun,
  emptyStateDisabled = false,
  onSelectPrompt,
  onOpenWizard,
}: MessageListProps) {
  const endRef = useRef<HTMLDivElement | null>(null);
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [isThinking, liveParts, messages]);

  return (
    <ScrollArea className="h-full">
      <div className="mx-auto flex min-h-full w-full max-w-[100rem] flex-col gap-6 px-4 py-6 sm:px-8">
        {messages.length === 0 ? (
          onSelectPrompt && onOpenWizard ? (
            <ChatEmptyState
              disabled={emptyStateDisabled}
              onSelectPrompt={onSelectPrompt}
              onOpenWizard={onOpenWizard}
            />
          ) : (
            <div className="flex flex-1 items-center justify-center py-20 text-center text-sm text-muted-foreground">
              Ask the agent to build or refine a strategy.
            </div>
          )
        ) : null}

        {messages.map((message, index) => {
          const isUser = message.role === "user";
          const isInspectable =
            message.role === "agent" && typeof message.run_id === "string";
          const runId = isInspectable ? message.run_id : null;
          const isActivityExpanded = runId === expandedRunId;
          const metadata = [message.status, message.run_id]
            .filter(Boolean)
            .join(" · ");
          const hasArtifacts =
            message.role === "agent" &&
            !!message.artifacts &&
            message.artifacts.length > 0;
          const report =
            message.role === "agent" && !message.error
              ? reportWithData(message.structured_result)
              : null;

          // User turns read as a compact right-aligned bubble; assistant turns
          // render as plain flowing text (Open WebUI style). Rich report cards
          // and errors are the exceptions and get their own treatment.
          const body = report ? (
            <StrategyResultReport result={report} runId={runId ?? undefined} />
          ) : message.error ? (
            <div className="rounded-xl bg-destructive/10 px-4 py-3 text-sm leading-6 text-destructive ring-1 ring-destructive/20">
              {message.error}
            </div>
          ) : isUser ? (
            <div className="ml-auto max-w-[85%] rounded-2xl bg-secondary px-4 py-2.5 text-sm leading-6 text-secondary-foreground">
              <pre className="font-sans whitespace-pre-wrap break-words">
                {message.text}
              </pre>
            </div>
          ) : (
            <div
              className={cn(
                "max-w-6xl rounded-xl text-[0.95rem] leading-7 text-foreground",
                isInspectable &&
                  "-mx-2 cursor-pointer px-2 py-1 transition-colors hover:bg-accent/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
              )}
              {...(isInspectable
                ? {
                    role: "button" as const,
                    tabIndex: 0,
                    "aria-label": `Inspect run ${runId}`,
                    onClick: (event: MouseEvent<HTMLDivElement>) => {
                      if (runId) {
                        onInspectRun?.(
                          runId,
                          event.currentTarget as unknown as HTMLButtonElement,
                        );
                      }
                    },
                  }
                : {})}
            >
              <pre className="font-sans whitespace-pre-wrap break-words">
                {message.text}
              </pre>
              {metadata ? (
                <p className="mt-2 text-xs text-muted-foreground">{metadata}</p>
              ) : null}
            </div>
          );

          return (
            <div
              key={`${message.role}-${message.run_id ?? index}`}
              className="flex w-full flex-col gap-2"
            >
              {body}

              {hasArtifacts && message.artifacts ? (
                <ArtifactGallery artifacts={message.artifacts} />
              ) : null}

              {runId ? (
                <div className="space-y-2">
                  <button
                    type="button"
                    className="text-xs font-medium text-muted-foreground underline-offset-4 hover:text-foreground hover:underline"
                    onClick={() =>
                      setExpandedRunId((current) =>
                        current === runId ? null : runId,
                      )
                    }
                  >
                    {isActivityExpanded
                      ? "Hide run activity"
                      : "Show run activity"}
                  </button>
                  {isActivityExpanded ? (
                    <LiveActivity runId={runId} fullWidth />
                  ) : null}
                </div>
              ) : null}
            </div>
          );
        })}

        {isThinking ? (
          liveRunId || liveParts.length > 0 ? (
            <LiveActivity
              runId={liveRunId ?? undefined}
              parts={liveRunId ? [] : liveParts}
              fullWidth={Boolean(liveRunId)}
              includeText
            />
          ) : (
            <p className="text-sm italic text-muted-foreground">thinking...</p>
          )
        ) : null}

        <div ref={endRef} />
      </div>
    </ScrollArea>
  );
}
