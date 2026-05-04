"use client";

import { useEffect, useEffectEvent, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";

import { ArtifactGallery } from "@/components/ArtifactGallery";
import { LiveActivity } from "@/components/LiveActivity";
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
import type { TimelinePart, TimelineState } from "@/lib/agent-events";
import {
  deriveStrategyLabel,
  setMessages,
  setStrategyId,
  upsertKnownStrategy,
} from "@/lib/local-store";
import { readSse } from "@/lib/sse";
import type { AllocationWizardState } from "@/lib/wizard-prompt";

type StoredWizardRun = {
  prompt: string;
  state: AllocationWizardState;
};

type RunState = {
  status: "loading" | "running" | "completed" | "error";
  strategyId: string | null;
  runId: string | null;
  error: string | null;
  timeline: TimelineState;
};

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
    isRecord(value) && typeof value.prompt === "string" && isRecord(value.state)
  );
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

function readStoredRun(id: string | null): StoredWizardRun | null {
  if (!id) return null;

  const raw = sessionStorage.getItem(`wizard-run:${id}`);
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as unknown;
    return isStoredWizardRun(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function latestCompletedText(parts: TimelinePart[]) {
  for (let index = parts.length - 1; index >= 0; index -= 1) {
    const part = parts[index];
    if (part.kind === "text" && part.completed && part.text.trim()) {
      return part.text.trim();
    }
  }

  return "";
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
    const next = readStoredRun(id);
    setStoredRun(next);
    if (!next) {
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

      try {
        const strategyResponse = await fetch("/api/strategies", {
          method: "POST",
          cache: "no-store",
        });
        const strategyPayload = await readJson(strategyResponse);

        if (!strategyResponse.ok) {
          throw new Error(
            getErrorMessage(strategyPayload, "Unable to create a strategy"),
          );
        }
        if (
          !isRecord(strategyPayload) ||
          typeof strategyPayload.strategy_id !== "string"
        ) {
          throw new Error("Strategy creation returned an invalid response");
        }

        const strategyId = strategyPayload.strategy_id;
        setStrategyId(strategyId);
        upsertKnownStrategy({
          strategy_id: strategyId,
          label: deriveStrategyLabel(activeRun.prompt),
        });
        setMessages(strategyId, [{ role: "user", text: activeRun.prompt }]);
        setRunState((current) => ({ ...current, strategyId }));

        const response = await fetch("/api/messages/stream", {
          method: "POST",
          headers: { "content-type": "application/json" },
          cache: "no-store",
          body: JSON.stringify({
            strategy_id: strategyId,
            text: activeRun.prompt,
          }),
        });

        if (!response.ok) {
          const payload = await readJson(response);
          throw new Error(
            getErrorMessage(payload, "Unable to run allocation agent"),
          );
        }
        if (!response.body) {
          throw new Error("Stream returned no body");
        }

        let timeline: TimelineState = initialTimeline;
        for await (const sseMessage of readSse(response.body)) {
          timeline = reduceTimeline(timeline, sseMessage);
          setRunState((current) => ({ ...current, timeline }));
          if (timeline.done) break;
        }

        const runResult = timeline.finalRun;
        if (!runResult) {
          throw new Error("Stream ended without a completed run");
        }

        const artifacts = runResult.artifacts ?? [];
        setMessages(strategyId, [
          { role: "user", text: activeRun.prompt },
          {
            role: "agent",
            text: runResult.reply ?? "",
            run_id: runResult.run_id,
            status: runResult.status,
            error: runResult.error ?? undefined,
            artifacts,
          },
        ]);
        setRunState({
          status: runResult.error ? "error" : "completed",
          strategyId,
          runId: runResult.run_id,
          error: runResult.error,
          timeline,
        });
      } catch (error) {
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
  }, [storedRun]);

  const finalRun = runState.timeline.finalRun;
  const finalArtifacts = finalRun?.artifacts ?? [];
  const completedDraft = latestCompletedText(runState.timeline.parts);
  const finalReply = finalRun?.reply?.trim() || completedDraft;
  const isFinished = !!finalRun || !!completedDraft;

  return (
    <main className="min-h-dvh bg-muted/30 px-4 py-6 sm:px-6 lg:px-8">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-6">
        <section className="flex flex-col gap-4 rounded-3xl border bg-background p-5 shadow-sm sm:p-6 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-3">
            <Badge variant="secondary" className="w-fit">
              Allocation run
            </Badge>
            <div className="max-w-3xl space-y-2">
              <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl">
                The agent is building your strategy.
              </h1>
              <p className="text-base text-muted-foreground">
                Follow reasoning and tool calls live. The final answer and
                charts appear on the right when the run finishes.
              </p>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Link href="/" className={buttonVariants({ variant: "outline" })}>
              Back to wizard
            </Link>
            {runState.strategyId ? (
              <Link
                href="/chat"
                className={buttonVariants({ variant: "outline" })}
              >
                Continue in chat
              </Link>
            ) : null}
          </div>
        </section>

        <section className="grid min-h-[calc(100dvh-14rem)] gap-6 lg:grid-cols-[minmax(0,1.05fr)_minmax(24rem,0.95fr)]">
          <Card className="min-h-[32rem] overflow-hidden">
            <CardHeader className="border-b">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div>
                  <CardTitle>Live activity</CardTitle>
                  <CardDescription>
                    Thinking, tool calls, command output, and draft text.
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

                  {runState.error && !finalRun ? (
                    <div className="rounded-xl border border-destructive/50 bg-destructive/10 p-4 text-sm text-destructive">
                      {runState.error}
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

          <Card className="min-h-[32rem] overflow-hidden">
            <CardHeader className="border-b">
              <CardTitle>Final response</CardTitle>
              <CardDescription>
                The answer and charts stay empty until the agent completes.
              </CardDescription>
            </CardHeader>
            <CardContent className="h-[calc(100dvh-22rem)] min-h-[26rem] p-0">
              <ScrollArea className="h-full">
                <div className="space-y-4 p-4">
                  {!isFinished ? (
                    <div className="flex min-h-[18rem] items-center justify-center rounded-2xl border border-dashed bg-muted/30 p-8 text-center text-sm leading-6 text-muted-foreground">
                      Waiting for the final allocation, metrics, and generated
                      artifacts.
                    </div>
                  ) : null}

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
                </div>
              </ScrollArea>
            </CardContent>
          </Card>
        </section>
      </div>
    </main>
  );
}
