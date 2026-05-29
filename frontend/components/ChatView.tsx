"use client";

import { useEffect, useEffectEvent, useRef, useState } from "react";

import { Composer } from "@/components/Composer";
import { IdentityBar } from "@/components/IdentityBar";
import { MessageList } from "@/components/MessageList";
import { RunInspector } from "@/components/RunInspector";
import { Card } from "@/components/ui/card";
import {
  initialTimeline,
  reduceTimeline,
  type TimelineState,
} from "@/lib/agent-events";
import {
  ChatMessage,
  deriveStrategyLabel,
  ensureKnownStrategy,
  getMessages,
  setMessages as persistMessages,
  setStrategyId as persistStrategyId,
  upsertKnownStrategy,
} from "@/lib/local-store";
import { readSse } from "@/lib/sse";

type ChatViewProps = {
  strategyId: string;
  disabled?: boolean;
  strategyError?: string | null;
  onBusyChange?: (isBusy: boolean) => void;
  onKnownStrategiesChange?: () => void;
  onNewStrategy: () => void | Promise<void>;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

async function readJson(response: Response) {
  try {
    return (await response.json()) as unknown;
  } catch {
    return null;
  }
}

function getErrorMessage(payload: unknown) {
  if (
    isRecord(payload) &&
    typeof payload.message === "string" &&
    payload.message.trim()
  ) {
    return payload.message.trim();
  }

  return "Request failed";
}

function getRunStartedId(data: string) {
  try {
    const payload = JSON.parse(data) as unknown;
    return isRecord(payload) && typeof payload.run_id === "string"
      ? payload.run_id
      : null;
  } catch {
    return null;
  }
}

export function ChatView({
  strategyId,
  disabled = false,
  strategyError = null,
  onBusyChange,
  onKnownStrategiesChange,
  onNewStrategy,
}: ChatViewProps) {
  const [messages, setMessages] = useState<ChatMessage[]>(() =>
    getMessages(strategyId),
  );
  const [isSending, setIsSending] = useState(false);
  const [liveRunId, setLiveRunId] = useState<string | null>(null);
  const [liveTimeline, setLiveTimeline] =
    useState<TimelineState>(initialTimeline);
  const [inspectedRunId, setInspectedRunId] = useState<string | null>(null);
  const [isInspectorOpen, setIsInspectorOpen] = useState(false);
  const inspectorTriggerRef = useRef<HTMLButtonElement | null>(null);

  const reportBusyChange = useEffectEvent((isBusy: boolean) => {
    onBusyChange?.(isBusy);
  });

  const notifyKnownStrategiesChange = useEffectEvent(() => {
    onKnownStrategiesChange?.();
  });

  useEffect(() => {
    reportBusyChange(isSending);
  }, [isSending]);

  useEffect(() => {
    persistStrategyId(strategyId);
    persistMessages(strategyId, messages);

    const firstUserMessage = messages.find(
      (message) => message.role === "user",
    );
    if (firstUserMessage) {
      upsertKnownStrategy({
        strategy_id: strategyId,
        label: deriveStrategyLabel(firstUserMessage.text),
      });
    } else {
      ensureKnownStrategy(strategyId);
    }

    notifyKnownStrategiesChange();
  }, [messages, strategyId]);

  async function handleSend(text: string) {
    if (disabled || isSending) {
      return;
    }

    setMessages((current) => [...current, { role: "user", text }]);
    setIsSending(true);
    setLiveRunId(null);
    setLiveTimeline(initialTimeline);

    try {
      const response = await fetch("/api/messages/stream", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        cache: "no-store",
        body: JSON.stringify({ strategy_id: strategyId, text }),
      });

      if (!response.ok) {
        const payload = await readJson(response);
        setMessages((current) => [
          ...current,
          {
            role: "agent",
            text: "",
            status: String(response.status),
            error: getErrorMessage(payload),
          },
        ]);
        return;
      }

      if (!response.body) {
        setMessages((current) => [
          ...current,
          {
            role: "agent",
            text: "",
            status: "error",
            error: "Stream returned no body",
          },
        ]);
        return;
      }

      let timeline: TimelineState = initialTimeline;
      for await (const sseMessage of readSse(response.body)) {
        if (sseMessage.event === "run.started") {
          const runId = getRunStartedId(sseMessage.data);
          if (runId) setLiveRunId(runId);
        }
        timeline = reduceTimeline(timeline, sseMessage);
        setLiveTimeline(timeline);
        if (timeline.done) break;
      }

      const run = timeline.finalRun;
      if (!run) {
        setMessages((current) => [
          ...current,
          {
            role: "agent",
            text: "",
            status: "error",
            error: "Stream ended without a completed run",
          },
        ]);
        return;
      }

      setMessages((current) => [
        ...current,
        {
          role: "agent",
          text: run.reply ?? "",
          run_id: run.run_id,
          status: run.status,
          error: run.error ?? undefined,
          artifacts: run.artifacts,
        },
      ]);
    } catch {
      setMessages((current) => [
        ...current,
        {
          role: "agent",
          text: "",
          status: "error",
          error: "Unable to reach the chat service",
        },
      ]);
    } finally {
      setIsSending(false);
      setLiveRunId(null);
      setLiveTimeline(initialTimeline);
    }
  }

  function handleInspectRun(runId: string, trigger: HTMLButtonElement) {
    inspectorTriggerRef.current = trigger;
    setInspectedRunId(runId);
    setIsInspectorOpen(true);
  }

  function handleInspectorOpenChange(open: boolean) {
    setIsInspectorOpen(open);

    if (!open) {
      inspectorTriggerRef.current?.focus();
    }
  }

  const isDisabled = disabled || isSending;

  return (
    <>
      <Card className="flex h-[calc(100vh-22rem)] min-h-[30rem] w-full flex-col overflow-hidden border-border/70 bg-background shadow-sm lg:h-[calc(100vh-3rem)]">
        <IdentityBar
          strategyId={strategyId}
          disabled={isDisabled}
          onNewStrategy={onNewStrategy}
        />

        {strategyError ? (
          <div className="border-b bg-destructive/10 px-4 py-3 text-sm text-destructive sm:px-5">
            {strategyError}
          </div>
        ) : null}

        <div className="min-h-0 flex-1 bg-muted/10">
          <MessageList
            messages={messages}
            isThinking={isSending}
            liveRunId={liveRunId}
            liveParts={liveTimeline.parts}
            onInspectRun={handleInspectRun}
          />
        </div>

        <Composer disabled={isDisabled} onSubmit={handleSend} />
      </Card>

      <RunInspector
        key={inspectedRunId ?? "empty"}
        open={isInspectorOpen}
        runId={inspectedRunId}
        onOpenChange={handleInspectorOpenChange}
      />
    </>
  );
}
