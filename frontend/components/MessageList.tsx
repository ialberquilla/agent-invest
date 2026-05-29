"use client";

import { useEffect, useRef, useState } from "react";

import { ArtifactGallery } from "@/components/ArtifactGallery";
import { LiveActivity } from "@/components/LiveActivity";
import { Card, CardContent } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { TimelinePart } from "@/lib/agent-events";
import { ChatMessage } from "@/lib/local-store";
import { cn } from "@/lib/utils";

type MessageListProps = {
  messages: ChatMessage[];
  isThinking: boolean;
  liveRunId?: string | null;
  liveParts?: TimelinePart[];
  onInspectRun?: (runId: string, trigger: HTMLButtonElement) => void;
};

export function MessageList({
  messages,
  isThinking,
  liveRunId = null,
  liveParts = [],
  onInspectRun,
}: MessageListProps) {
  const endRef = useRef<HTMLDivElement | null>(null);
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [isThinking, liveParts, messages]);

  return (
    <ScrollArea className="h-full">
      <div className="flex min-h-full flex-col gap-4 px-4 py-4 sm:px-5">
        {messages.length === 0 ? (
          <div className="flex flex-1 items-center justify-center rounded-xl border border-dashed border-border bg-muted/20 px-6 py-10 text-center text-sm text-muted-foreground">
            Your chat history for this strategy will appear here.
          </div>
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
          const bubbleClassName = cn(
            "py-3 shadow-sm",
            isUser && "bg-primary text-primary-foreground ring-primary/15",
            !isUser && !message.error && "bg-card text-card-foreground",
            message.error &&
              "bg-destructive/10 text-destructive ring-destructive/20",
            isInspectable &&
              "cursor-pointer transition-colors hover:bg-accent hover:text-accent-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          );
          const bubbleBody = (
            <Card size="sm" className={bubbleClassName}>
              <CardContent className="space-y-2">
                {message.error ? (
                  <p className="text-sm leading-6">{message.error}</p>
                ) : (
                  <pre className="font-sans text-sm leading-6 whitespace-pre-wrap break-words">
                    {message.text}
                  </pre>
                )}

                {metadata ? (
                  <p className="text-xs opacity-70">{metadata}</p>
                ) : null}
              </CardContent>
            </Card>
          );

          return (
            <div
              key={`${message.role}-${message.run_id ?? index}`}
              className={cn("flex", isUser ? "justify-end" : "justify-start")}
            >
              <div className="flex max-w-[96%] flex-col gap-2 sm:max-w-[92%] xl:max-w-[88%]">
                {isInspectable ? (
                  <button
                    type="button"
                    className="rounded-xl text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                    aria-label={`Inspect run ${runId}`}
                    onClick={(event) => {
                      if (runId) {
                        onInspectRun?.(runId, event.currentTarget);
                      }
                    }}
                  >
                    {bubbleBody}
                  </button>
                ) : (
                  bubbleBody
                )}

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
            <div className="flex justify-start">
              <Card
                size="sm"
                className="max-w-[96%] bg-muted py-3 text-muted-foreground sm:max-w-[92%] xl:max-w-[88%]"
              >
                <CardContent>
                  <p className="text-sm italic">thinking...</p>
                </CardContent>
              </Card>
            </div>
          )
        ) : null}

        <div ref={endRef} />
      </div>
    </ScrollArea>
  );
}
