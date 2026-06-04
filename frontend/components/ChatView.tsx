"use client";

import { useEffect, useEffectEvent, useRef, useState } from "react";
import { Compass } from "lucide-react";

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
  getAnonymousUserId,
  setMessages as persistMessages,
  setStrategyId as persistStrategyId,
  upsertKnownStrategy,
} from "@/lib/local-store";
import { readSse } from "@/lib/sse";
import type { Run } from "@/lib/types";
import type { AllocationWizardState } from "@/lib/wizard-prompt";

type ChatViewProps = {
  strategyId: string;
  disabled?: boolean;
  strategyError?: string | null;
  onBusyChange?: (isBusy: boolean) => void;
  onKnownStrategiesChange?: () => void;
  onNewStrategy: () => void | Promise<void>;
  authenticated: boolean;
  getAccessToken: () => Promise<string | null>;
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

function getChatRunId(payload: unknown) {
  return isRecord(payload) && typeof payload.run_id === "string"
    ? payload.run_id
    : null;
}

function getChatContent(payload: unknown) {
  return isRecord(payload) && typeof payload.content === "string"
    ? payload.content
    : "";
}

function isRun(payload: unknown): payload is Run {
  return (
    isRecord(payload) &&
    typeof payload.run_id === "string" &&
    typeof payload.status === "string"
  );
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
  authenticated,
  getAccessToken,
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
      const accessToken = authenticated ? await getAccessToken() : null;
      if ("text" in submission) {
        const response = await fetch("/api/chat/messages", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
          },
          cache: "no-store",
          body: JSON.stringify({
            chat_session_id: strategyId,
            message: submission.text,
            user_id: getAnonymousUserId(),
          }),
        });

        const payload = await readJson(response);
        if (!response.ok) {
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

        const runId = getChatRunId(payload);
        const content = getChatContent(payload);
        if (!runId) {
          setMessages((current) => [
            ...current,
            { role: "agent", text: content || "I do not have a response." },
          ]);
          return;
        }

        const startedText =
          content.trim() || `Started strategy research run ${runId}.`;
        setLiveRunId(runId);
        setMessages((current) => [
          ...current,
          { role: "agent", text: startedText, run_id: runId, status: "running" },
        ]);

        const run = await waitForRunCompletion(runId, accessToken);
        setMessages((current) =>
          current.map((message) =>
            message.run_id === run.run_id
              ? {
                  role: "agent",
                  text: run.reply ?? startedText,
                  run_id: run.run_id,
                  status: run.status,
                  error: run.error ?? undefined,
                  artifacts: run.artifacts,
                  structured_result: run.structured_result,
                }
              : message,
          ),
        );
        return;
      }

      const response = await fetch("/api/strategy-pipeline/runs/stream", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(accessToken
            ? { authorization: `Bearer ${accessToken}` }
            : {}),
        },
        cache: "no-store",
        body: JSON.stringify({
          strategy_id: strategyId,
          wizard_params: submission.wizard_params,
          user_id: getAnonymousUserId(),
        }),
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

  async function waitForRunCompletion(runId: string, accessToken: string | null) {
    while (true) {
      const response = await fetch(`/api/runs/${encodeURIComponent(runId)}`, {
        headers: accessToken ? { authorization: `Bearer ${accessToken}` } : {},
        cache: "no-store",
      });
      const payload = await readJson(response);
      if (!response.ok) throw new Error(getErrorMessage(payload));
      if (!isRun(payload)) throw new Error("Run status returned invalid data");
      if (payload.status !== "running") return payload;
      await sleep(1000);
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
      <div className="flex min-h-0 flex-1 flex-col bg-background">
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
            emptyStateDisabled={isDisabled}
            onSelectPrompt={handleSend}
            onOpenWizard={() => setIsWizardOpen(true)}
          />
        </div>

        {isWizardOpen ? (
          <section className="min-h-0 flex-1 overflow-y-auto border-t border-border/60">
            <div className="mx-auto w-full max-w-5xl">
              <div className="sticky top-0 z-20 flex items-center justify-between gap-4 border-b border-border/60 bg-background/95 px-4 py-2 backdrop-blur">
                <h2 className="font-heading text-sm font-semibold">
                  Guided setup
                </h2>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => setIsWizardOpen(false)}
                >
                  <span aria-hidden>×</span>
                  <span className="sr-only">Close guided setup</span>
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
              className="gap-1.5"
            >
              <Compass className="size-4" />
              Guided setup
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
