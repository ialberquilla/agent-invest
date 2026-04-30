"use client";

import { useEffect, useRef } from "react";

import { Card, CardContent } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { ChatMessage } from "@/lib/local-store";
import { cn } from "@/lib/utils";

type MessageListProps = {
  messages: ChatMessage[];
  isThinking: boolean;
  onInspectRun?: (runId: string, trigger: HTMLButtonElement) => void;
};

export function MessageList({
  messages,
  isThinking,
  onInspectRun,
}: MessageListProps) {
  const endRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [isThinking, messages]);

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
          const metadata = [message.status, message.run_id]
            .filter(Boolean)
            .join(" · ");
          const bubbleClassName = cn(
            "max-w-[90%] py-3 shadow-sm sm:max-w-[80%]",
            isUser && "bg-primary text-primary-foreground ring-primary/15",
            !isUser && !message.error && "bg-card text-card-foreground",
            message.error &&
              "bg-destructive/10 text-destructive ring-destructive/20",
            isInspectable &&
              "cursor-pointer transition-colors hover:bg-accent hover:text-accent-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          );

          return (
            <div
              key={`${message.role}-${message.run_id ?? index}`}
              className={cn("flex", isUser ? "justify-end" : "justify-start")}
            >
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
                </button>
              ) : (
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
              )}
            </div>
          );
        })}

        {isThinking ? (
          <div className="flex justify-start">
            <Card
              size="sm"
              className="max-w-[90%] bg-muted py-3 text-muted-foreground sm:max-w-[80%]"
            >
              <CardContent>
                <p className="text-sm italic">thinking...</p>
              </CardContent>
            </Card>
          </div>
        ) : null}

        <div ref={endRef} />
      </div>
    </ScrollArea>
  );
}
