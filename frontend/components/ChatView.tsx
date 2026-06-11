"use client";

import { useEffect, useEffectEvent, useRef, useState } from "react";
import { Compass } from "lucide-react";

import { StrategyTypePicker } from "@/components/StrategyTypePicker";
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
import type { Run, StructuredChatResult, SuggestedRerun } from "@/lib/types";
import type { AllocationWizardState } from "@/lib/wizard-prompt";

type ChatViewProps = {
  strategyId: string;
  disabled?: boolean;
  strategyError?: string | null;
  onBusyChange?: (isBusy: boolean) => void;
  onKnownStrategiesChange?: () => void;
  onPinnedScreenersChange?: () => void;
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
  if (!isRecord(payload)) return "";
  for (const key of ["content", "delta", "text"] as const) {
    const value = payload[key];
    if (typeof value === "string") return value;
  }
  return "";
}

function mergeChatDelta(current: string, next: string) {
  if (!next) return current;
  if (!current || next.startsWith(current)) return next;
  return `${current}${next}`;
}

function getChatStructuredResult(payload: unknown): StructuredChatResult | null {
  return isRecord(payload) && isRecord(payload.structured_result)
    ? (payload.structured_result as StructuredChatResult)
    : null;
}

function getChatStreamPayload(data: string) {
  try {
    const payload = JSON.parse(data) as unknown;
    return isRecord(payload) ? payload : null;
  } catch {
    return null;
  }
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
  | { text: string; displayText?: string }
  | {
      wizard_params: AllocationWizardState;
      displayText: string;
      // Deterministic thesis overrides for a non-basket strategy type chosen
      // in the picker. Applied after interpret_brief and re-validated.
      overrides?: Record<string, unknown>;
    };

function wizardSubmissionText() {
  return "Allocation wizard submission";
}

// A neutral base brief for the non-basket picker paths. interpret_brief turns
// it into a starting thesis; the strategy-type overrides then reshape it.
const BASE_WIZARD_PARAMS: AllocationWizardState = {
  universe: "top25",
  exclusions: ["stablecoins", "wrapped"],
  minimumMarketCap: "1b",
  concentrationLimit: "agent",
  maxDrawdown: "50",
  riskPreference: "balanced",
  horizon: "1y",
  rebalance: "monthly",
  initialCapitalUsd: "",
  cashAllocation: "none",
  targetAssets: "agent",
};

export function ChatView({
  strategyId,
  disabled = false,
  strategyError = null,
  onBusyChange,
  onKnownStrategiesChange,
  onPinnedScreenersChange,
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
    return () => {
      reportBusyChange(false);
    };
  }, []);

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
      "text" in submission
        ? (submission.displayText ?? submission.text)
        : submission.displayText;

    setMessages((current) => [...current, { role: "user", text: userText }]);
    setIsSending(true);
    setLiveRunId(null);
    setLiveTimeline(initialTimeline);

    try {
      const accessToken = authenticated ? await getAccessToken() : null;
      if ("text" in submission) {
        const placeholderIndex = messages.length + 1;
        setMessages((current) => [
          ...current,
          { role: "agent", text: "", status: "streaming" },
        ]);

        const response = await fetch("/api/chat/messages/stream", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            accept: "text/event-stream",
            ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
          },
          cache: "no-store",
          body: JSON.stringify({
            chat_session_id: strategyId,
            message: submission.text,
            user_id: getAnonymousUserId(),
          }),
        });

        if (!response.ok) {
          const payload = await readJson(response);
          setMessages((current) => [
            ...current.slice(0, placeholderIndex),
            { ...current[placeholderIndex], status: String(response.status), error: getErrorMessage(payload) },
          ]);
          return;
        }

        if (!response.body) {
          setMessages((current) => [
            ...current.slice(0, placeholderIndex),
            {
              ...current[placeholderIndex],
              status: "error",
              error: "Stream returned no body",
            },
          ]);
          return;
        }

        let finalRunId: string | null = null;
        let finalText = "";
        let finalStatus = "completed";
        let finalStructuredResult: StructuredChatResult | null = null;
        for await (const sseMessage of readSse(response.body)) {
          const payload = getChatStreamPayload(sseMessage.data);
          if (!payload) continue;

          if (sseMessage.event === "chat.delta") {
            finalText = mergeChatDelta(finalText, getChatContent(payload));
            setMessages((current) => [
              ...current.slice(0, placeholderIndex),
              { ...current[placeholderIndex], text: finalText, status: "streaming" },
            ]);
          } else if (sseMessage.event === "tool.updated") {
            const runId = getChatRunId(payload);
            if (runId) {
              finalRunId = runId;
              setLiveRunId(runId);
              setMessages((current) => [
                ...current.slice(0, placeholderIndex),
                { ...current[placeholderIndex], run_id: runId, status: "running" },
              ]);
            }
          } else if (sseMessage.event === "chat.completed") {
            finalText = getChatContent(payload) || finalText;
            finalRunId = getChatRunId(payload) ?? finalRunId;
            finalStructuredResult =
              getChatStructuredResult(payload) ?? finalStructuredResult;
            finalStatus = finalRunId ? "running" : "completed";
          } else if (sseMessage.event === "chat.error") {
            const message =
              typeof payload.message === "string"
                ? payload.message
                : "Unable to reach the chat service";
            setMessages((current) => [
              ...current.slice(0, placeholderIndex),
              { ...current[placeholderIndex], status: "error", error: message },
            ]);
            return;
          }
        }

        const completedText = finalText || "I do not have a response.";
        setMessages((current) => [
          ...current.slice(0, placeholderIndex),
          {
            ...current[placeholderIndex],
            text: completedText,
            run_id: finalRunId ?? undefined,
            status: finalStatus,
            structured_result: finalStructuredResult,
          },
        ]);

        if (!finalRunId) return;

        const run = await waitForRunCompletion(finalRunId, accessToken);
        setMessages((current) =>
          current.map((message) =>
            message.run_id === run.run_id
              ? {
                  role: "agent",
                  text: run.reply ?? completedText,
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
          overrides: submission.overrides,
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

  // Relaunch the pipeline from a finished run with one structured change.
  // The chat agent forwards based_on_run_id + the exact overrides to
  // run_strategy_pipeline (overrides are applied deterministically there),
  // so the instruction is explicit and leaves nothing to infer.
  async function handleRerun(
    suggestion: SuggestedRerun,
    basedOnRunId: string,
  ) {
    const instruction = [
      "Re-run the previous strategy as a new pipeline run.",
      `Call ${"run_strategy_pipeline"} with based_on_run_id="${basedOnRunId}"`,
      `and these exact overrides (do not modify them): ${JSON.stringify(
        suggestion.overrides,
      )}.`,
      "Reuse the original brief.",
    ].join(" ");
    await submitRun({ text: instruction, displayText: `Rerun: ${suggestion.label}` });
  }

  async function handleWizardSubmit(wizardParams: AllocationWizardState) {
    setIsWizardOpen(false);
    await submitRun({
      wizard_params: wizardParams,
      displayText: wizardSubmissionText(),
    });
  }

  // Non-basket strategy types from the picker: a base wizard brief gives
  // interpret_brief something to work from, and the overrides deterministically
  // reshape the thesis into the chosen single-asset / pair / long-short shape.
  async function handleWizardOverrides(
    overrides: Record<string, unknown>,
    label: string,
  ) {
    setIsWizardOpen(false);
    await submitRun({
      wizard_params: BASE_WIZARD_PARAMS,
      overrides,
      displayText: `Guided setup: ${label}`,
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
            onPinnedScreenersChange={onPinnedScreenersChange}
            onRerun={handleRerun}
            rerunDisabled={isDisabled}
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
                <StrategyTypePicker
                  onSubmitBasket={handleWizardSubmit}
                  onSubmitOverrides={handleWizardOverrides}
                  disabled={isDisabled}
                />
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
