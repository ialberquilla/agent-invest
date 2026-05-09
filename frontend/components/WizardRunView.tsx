"use client";

import { useEffect, useEffectEvent, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";

import { ArtifactGallery } from "@/components/ArtifactGallery";
import { LiveActivity } from "@/components/LiveActivity";
import { StrategyResultReport } from "@/components/StrategyResultReport";
import { Badge } from "@/components/ui/badge";
import { buttonVariants } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { initialTimeline, reduceTimeline } from "@/lib/agent-events";
import type { TimelineState } from "@/lib/agent-events";
import { trackEvent } from "@/lib/analytics";
import type { Run } from "@/lib/types";
import {
  deriveStrategyLabel,
  setMessages,
  setStrategyId,
  upsertKnownStrategy,
} from "@/lib/local-store";
import { readSse } from "@/lib/sse";
import type { AllocationWizardState } from "@/lib/wizard-prompt";

type StoredWizardRun = {
  state: AllocationWizardState;
  strategyId: string;
};

type StoredWizardRunResult = {
  strategyId: string;
  run: Run;
};

type RunState = {
  status: "loading" | "running" | "completed" | "error";
  strategyId: string | null;
  runId: string | null;
  error: string | null;
  timeline: TimelineState;
};

type FailureStage =
  | "missing_stored_run"
  | "strategy_create"
  | "stream_start"
  | "stream_processing"
  | "final_result"
  | "unknown";

class WizardRunError extends Error {
  constructor(
    message: string,
    readonly failureStage: FailureStage,
  ) {
    super(message);
    this.name = "WizardRunError";
  }
}

const initialRunState: RunState = {
  status: "loading",
  strategyId: null,
  runId: null,
  error: null,
  timeline: initialTimeline,
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isStoredWizardRun(value: unknown): value is StoredWizardRun {
  return (
    isRecord(value) &&
    isRecord(value.state) &&
    typeof value.strategyId === "string" &&
    value.strategyId.trim().length > 0
  );
}

function wizardSubmissionText() {
  return "Allocation wizard submission";
}

async function readJson(response: Response) {
  try {
    return (await response.json()) as unknown;
  } catch {
    return null;
  }
}

function getErrorMessage(payload: unknown, fallback: string) {
  if (
    isRecord(payload) &&
    typeof payload.message === "string" &&
    payload.message.trim()
  ) {
    return payload.message.trim();
  }

  return fallback;
}

function trackRunFailed(failureStage: FailureStage) {
  trackEvent("wizard_run_failed", {
    run_source: "allocation_wizard",
    failure_stage: failureStage,
  });
}

function readStoredRun(id: string | null): StoredWizardRun | null {
  if (!id) return null;

  const key = `wizard-run:${id}`;
  const raw = localStorage.getItem(key) ?? sessionStorage.getItem(key);
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as unknown;
    return isStoredWizardRun(parsed) ? parsed : null;
  } catch {
    return null;
  }
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

function isStoredWizardRunResult(
  value: unknown,
): value is StoredWizardRunResult {
  return (
    isRecord(value) &&
    typeof value.strategyId === "string" &&
    isRecord(value.run) &&
    typeof value.run.run_id === "string"
  );
}

function readStoredRunResult(id: string | null): StoredWizardRunResult | null {
  if (!id) return null;

  const raw = localStorage.getItem(`wizard-run-result:${id}`);
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as unknown;
    return isStoredWizardRunResult(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function writeStoredRunResult(id: string | null, strategyId: string, run: Run) {
  if (!id) return;
  localStorage.setItem(
    `wizard-run-result:${id}`,
    JSON.stringify({ strategyId, run }),
  );
}

export function WizardRunView() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id");
  const [storedRun, setStoredRun] = useState<StoredWizardRun | null>(null);
  const [runState, setRunState] = useState<RunState>(initialRunState);
  const hasStartedRef = useRef(false);
  const activityEndRef = useRef<HTMLDivElement | null>(null);

  const scrollToLatestActivity = useEffectEvent(() => {
    activityEndRef.current?.scrollIntoView({ block: "end" });
  });

  useEffect(() => {
    const previousResult = readStoredRunResult(id);
    if (previousResult) {
      setStoredRun(readStoredRun(id));
      setRunState({
        status: previousResult.run.error ? "error" : "completed",
        strategyId: previousResult.strategyId,
        runId: previousResult.run.run_id,
        error: previousResult.run.error,
        timeline: {
          ...initialTimeline,
          done: true,
          finalRun: previousResult.run,
        },
      });
      hasStartedRef.current = true;
      return;
    }

    const next = readStoredRun(id);
    setStoredRun(next);
    if (!next) {
      trackRunFailed("missing_stored_run");
      setRunState({
        ...initialRunState,
        status: "error",
        error:
          "Wizard run details were not found. Return to the wizard and run again.",
      });
    }
  }, [id]);

  useEffect(() => {
    scrollToLatestActivity();
  }, [runState.timeline.parts.length, runState.timeline]);

  useEffect(() => {
    if (!storedRun || hasStartedRef.current) return;
    hasStartedRef.current = true;
    const activeRun = storedRun;

    async function run() {
      setRunState({ ...initialRunState, status: "running" });
      const storageId = id;
      trackEvent("wizard_run_started", {
        run_source: "allocation_wizard",
      });

      try {
        const strategyId = activeRun.strategyId;
        const userMessageText = wizardSubmissionText();
        setStrategyId(strategyId);
        upsertKnownStrategy({
          strategy_id: strategyId,
          label: deriveStrategyLabel(userMessageText),
        });
        setMessages(strategyId, [{ role: "user", text: userMessageText }]);
        setRunState((current) => ({ ...current, strategyId }));

        let response: Response;
        try {
          response = await fetch("/api/messages/stream", {
            method: "POST",
            headers: { "content-type": "application/json" },
            cache: "no-store",
            body: JSON.stringify({
              strategy_id: strategyId,
              wizard_params: activeRun.state,
            }),
          });
        } catch (error) {
          throw new WizardRunError(
            error instanceof Error
              ? error.message
              : "Unable to run allocation agent",
            "stream_start",
          );
        }

        if (!response.ok) {
          const payload = await readJson(response);
          throw new WizardRunError(
            getErrorMessage(payload, "Unable to run allocation agent"),
            "stream_start",
          );
        }
        if (!response.body) {
          throw new WizardRunError("Stream returned no body", "stream_start");
        }

        let timeline: TimelineState = initialTimeline;
        try {
          for await (const sseMessage of readSse(response.body)) {
            if (sseMessage.event === "run.started") {
              const persistedRunId = getRunStartedId(sseMessage.data);
              if (persistedRunId) {
                setRunState((current) => ({
                  ...current,
                  runId: persistedRunId,
                }));
              }
            }
            timeline = reduceTimeline(timeline, sseMessage);
            setRunState((current) => ({ ...current, timeline }));
            if (timeline.done) break;
          }
        } catch (error) {
          throw new WizardRunError(
            error instanceof Error
              ? error.message
              : "Unable to process allocation agent stream",
            "stream_processing",
          );
        }

        const runResult = timeline.finalRun;
        if (!runResult) {
          throw new WizardRunError(
            "Stream ended without a completed run",
            "stream_processing",
          );
        }

        const artifacts = runResult.artifacts ?? [];
        writeStoredRunResult(storageId, strategyId, runResult);
        setMessages(strategyId, [
          { role: "user", text: userMessageText },
          {
            role: "agent",
            text: runResult.reply ?? "",
            run_id: runResult.run_id,
            status: runResult.status,
            error: runResult.error ?? undefined,
            artifacts,
          },
        ]);
        if (runResult.error) {
          trackRunFailed("final_result");
        } else {
          trackEvent("wizard_run_completed", {
            run_source: "allocation_wizard",
            has_artifacts: artifacts.length > 0,
            artifact_count: artifacts.length,
          });
        }
        setRunState({
          status: runResult.error ? "error" : "completed",
          strategyId,
          runId: runResult.run_id,
          error: runResult.error,
          timeline,
        });
      } catch (error) {
        trackRunFailed(
          error instanceof WizardRunError ? error.failureStage : "unknown",
        );
        setRunState((current) => ({
          ...current,
          status: "error",
          error:
            error instanceof Error
              ? error.message
              : "Unable to run allocation agent",
        }));
      }
    }

    void run();
  }, [id, storedRun]);

  const finalRun = runState.timeline.finalRun;
  const finalArtifacts = finalRun?.artifacts ?? [];
  const structuredResult = finalRun?.structured_result ?? null;
  const finalReply = finalRun?.reply?.trim() ?? "";
  const isFinished = !!finalRun;
  const failedBeforeFinal = runState.status === "error" && !isFinished;
  const isActiveRun = !isFinished && !failedBeforeFinal;
  const headerTitle = isActiveRun
    ? "The agent is building your strategy."
    : failedBeforeFinal
      ? "The allocation run failed."
      : "Your strategy report is ready.";
  const headerDescription = isActiveRun
    ? "Follow reasoning and tool calls live. The final answer and charts appear when the run finishes."
    : failedBeforeFinal
      ? "Review the error and any captured activity below."
      : "Review the final answer, allocation rationale, and charts below.";

  return (
    <main className="min-h-dvh bg-[radial-gradient(circle_at_top_left,color-mix(in_oklab,var(--primary)_16%,transparent),transparent_34%),linear-gradient(180deg,var(--background),color-mix(in_oklab,var(--accent)_38%,var(--background)))] px-3 py-4 sm:px-4 lg:px-6">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-4">
        <section className="flex flex-col gap-4 rounded-2xl border bg-background p-4 shadow-sm lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-3">
            <Badge variant="secondary" className="w-fit">
              Allocation run
            </Badge>
            <div className="max-w-3xl space-y-2">
              <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl lg:text-4xl">
                {headerTitle}
              </h1>
              <p className="text-sm leading-6 text-muted-foreground sm:text-base">
                {headerDescription}
              </p>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Link href="/" className={buttonVariants({ variant: "outline" })}>
              Back to wizard
            </Link>
          </div>
        </section>

        {isActiveRun ? (
          <section className="flex min-h-[calc(100dvh-14rem)] w-full">
            <Card className="min-h-[32rem] w-full overflow-hidden">
              <CardHeader className="border-b">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                  <div>
                    <CardTitle>Live activity</CardTitle>
                    <CardDescription>
                      Thinking, tool calls, and command output.
                    </CardDescription>
                  </div>
                  <Badge variant="outline">{runState.status}</Badge>
                </div>
              </CardHeader>
              <CardContent className="h-[calc(100dvh-22rem)] min-h-[26rem] p-0">
                <ScrollArea className="h-full">
                  <div className="space-y-4 p-4">
                    {runState.strategyId ? (
                      <div className="rounded-xl border bg-muted/40 p-3 text-xs text-muted-foreground">
                        Strategy: {runState.strategyId}
                        {runState.runId ? ` · Run: ${runState.runId}` : ""}
                      </div>
                    ) : null}

                    {runState.timeline.parts.length > 0 ? (
                      <LiveActivity
                        parts={runState.timeline.parts}
                        fullWidth
                        includeText={false}
                      />
                    ) : (
                      <div className="rounded-xl border bg-muted/40 p-4 text-sm italic text-muted-foreground">
                        Starting the allocation agent...
                      </div>
                    )}
                    <div ref={activityEndRef} />
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>
          </section>
        ) : failedBeforeFinal ? (
          <section className="flex min-h-[calc(100dvh-14rem)] w-full items-start">
            <Card className="w-full overflow-hidden">
              <CardHeader className="border-b">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                  <div>
                    <CardTitle>Run failed</CardTitle>
                    <CardDescription>
                      The allocation agent stopped before producing a final
                      response.
                    </CardDescription>
                  </div>
                  <Badge variant="outline">{runState.status}</Badge>
                </div>
              </CardHeader>
              <CardContent className="space-y-4 p-5 sm:p-6">
                <div className="rounded-xl border border-destructive/50 bg-destructive/10 p-4 text-sm leading-6 text-destructive">
                  {runState.error ?? "Unable to run allocation agent"}
                </div>

                {runState.timeline.parts.length > 0 ? (
                  <div className="rounded-2xl border bg-muted/20 p-4">
                    <LiveActivity
                      parts={runState.timeline.parts}
                      fullWidth
                      includeText={false}
                    />
                    <div ref={activityEndRef} />
                  </div>
                ) : null}

                <Link
                  href="/"
                  className={buttonVariants({ variant: "outline" })}
                >
                  Back to wizard
                </Link>
              </CardContent>
            </Card>
          </section>
        ) : structuredResult ? (
          <section className="w-full">
            <StrategyResultReport result={structuredResult} />
          </section>
        ) : (
          <section className="w-full">
            <Card className="overflow-hidden">
              <CardHeader className="border-b">
                <CardTitle>Final response</CardTitle>
                <CardDescription>
                  Raw response fallback for runs without a structured report.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4 p-4 sm:p-6">
                {finalRun?.error ? (
                  <div className="rounded-xl border border-destructive/50 bg-destructive/10 p-4 text-sm leading-6 text-destructive">
                    {finalRun.error}
                  </div>
                ) : null}

                {finalReply ? (
                  <pre className="whitespace-pre-wrap break-words rounded-xl border bg-background p-4 font-sans text-sm leading-6">
                    {finalReply}
                  </pre>
                ) : null}

                {finalArtifacts.length > 0 ? (
                  <ArtifactGallery artifacts={finalArtifacts} />
                ) : null}
              </CardContent>
            </Card>
          </section>
        )}
      </div>
    </main>
  );
}
