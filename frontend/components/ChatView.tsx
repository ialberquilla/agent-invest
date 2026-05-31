"use client";

import { useEffect, useEffectEvent, useRef, useState } from "react";

import { AllocationWizard } from "@/components/AllocationWizard";
import { Composer } from "@/components/Composer";
import { IdentityBar } from "@/components/IdentityBar";
import { MessageList } from "@/components/MessageList";
import { RunInspector } from "@/components/RunInspector";
import { Button } from "@/components/ui/button";
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
import type { AllocationWizardState } from "@/lib/wizard-prompt";

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

type RunSubmission =
  | { text: string }
  | { wizard_params: AllocationWizardState; displayText: string };

function wizardSubmissionText() {
  return "Allocation wizard submission";
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
  const [isWizardOpen, setIsWizardOpen] = useState(false);
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

  async function submitRun(submission: RunSubmission) {
    if (disabled || isSending) {
      return;
    }

    const userText =
      "text" in submission ? submission.text : submission.displayText;

    setMessages((current) => [...current, { role: "user", text: userText }]);
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
        body: JSON.stringify(
          "text" in submission
            ? { strategy_id: strategyId, text: submission.text }
            : {
                strategy_id: strategyId,
                wizard_params: submission.wizard_params,
              },
        ),
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
          structured_result: run.structured_result,
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

  async function handleSend(text: string) {
    await submitRun({ text });
  }

  async function handleWizardSubmit(wizardParams: AllocationWizardState) {
    setIsWizardOpen(false);
    await submitRun({
      wizard_params: wizardParams,
      displayText: wizardSubmissionText(),
    });
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
      <div className="flex h-dvh flex-col bg-background">
        <IdentityBar
          strategyId={strategyId}
          disabled={isDisabled}
          onNewStrategy={onNewStrategy}
        />

        {strategyError ? (
          <div className="border-b border-border/60 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            {strategyError}
          </div>
        ) : null}

        <div
          className={
            isWizardOpen
              ? "min-h-0 max-h-28 shrink-0"
              : "min-h-0 flex-1"
          }
        >
          <MessageList
            messages={messages}
            isThinking={isSending}
            liveRunId={liveRunId}
            liveParts={liveTimeline.parts}
            onInspectRun={handleInspectRun}
          />
        </div>

        {isWizardOpen ? (
          <section className="min-h-0 flex-1 overflow-y-auto border-t border-border/60">
            <div className="mx-auto w-full max-w-5xl">
              <div className="sticky top-0 z-20 flex items-center justify-between gap-4 border-b border-border/60 bg-background/95 px-4 py-2 backdrop-blur">
                <h2 className="font-heading text-sm font-semibold">
                  Allocation wizard
                </h2>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => setIsWizardOpen(false)}
                >
                  <span aria-hidden>×</span>
                  <span className="sr-only">Close wizard</span>
                </Button>
              </div>
              <div className="p-2 sm:p-3">
                <AllocationWizard embedded onSubmit={handleWizardSubmit} />
              </div>
            </div>
          </section>
        ) : null}

        <Composer
          disabled={isDisabled}
          action={
            <Button
              type="button"
              variant="ghost"
              size="sm"
              disabled={isDisabled}
              onClick={() => setIsWizardOpen(true)}
            >
              Wizard
            </Button>
          }
          onSubmit={handleSend}
        />
      </div>

      <RunInspector
        key={inspectedRunId ?? "empty"}
        open={isInspectorOpen}
        runId={inspectedRunId}
        onOpenChange={handleInspectorOpenChange}
      />
    </>
  );
}
