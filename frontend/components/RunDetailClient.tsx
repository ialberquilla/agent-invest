"use client";

import { useEffect, useMemo, useState } from "react";

import { LiveActivity } from "@/components/LiveActivity";
import { PipelineTimeline } from "@/components/PipelineTimeline";
import { Badge } from "@/components/ui/badge";
import { Run, StageRunDelta, StageRunSummary } from "@/lib/types";

type StreamStatus = "connecting" | "open" | "closed" | "error";

type RunDetailClientProps = {
  run: Run;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isStageRunSummary(value: unknown): value is StageRunSummary {
  if (!isRecord(value)) return false;

  return (
    typeof value.stage_run_id === "string" &&
    typeof value.stage === "string" &&
    typeof value.round === "number" &&
    typeof value.status === "string" &&
    typeof value.started_at === "string" &&
    (typeof value.ended_at === "string" || value.ended_at === null) &&
    typeof value.model === "string" &&
    isRecord(value.tokens) &&
    (typeof value.tokens.input === "number" || value.tokens.input === null) &&
    (typeof value.tokens.output === "number" || value.tokens.output === null)
  );
}

function isStageRunDelta(value: unknown): value is StageRunDelta {
  if (!isRecord(value)) return false;

  return (
    typeof value.run_id === "string" &&
    typeof value.stage_run_id === "string" &&
    typeof value.stage === "string" &&
    typeof value.round === "number" &&
    typeof value.status === "string"
  );
}

function parseJson(data: string) {
  try {
    return JSON.parse(data) as unknown;
  } catch {
    return null;
  }
}

function reduceSnapshot(stages: StageRunSummary[]) {
  return new Map(stages.map((stage) => [stage.stage_run_id, stage]));
}

function reduceDelta(
  stages: Map<string, StageRunSummary>,
  delta: StageRunDelta,
) {
  const next = new Map(stages);
  const existing = next.get(delta.stage_run_id);

  next.set(delta.stage_run_id, {
    stage_run_id: delta.stage_run_id,
    stage: delta.stage,
    round: delta.round,
    status: delta.status,
    started_at: existing?.started_at ?? new Date().toISOString(),
    ended_at: existing?.ended_at ?? null,
    model: existing?.model ?? "",
    tokens: existing?.tokens ?? { input: null, output: null },
  });

  return next;
}

function sortStages(stages: StageRunSummary[]) {
  return [...stages].sort((left, right) => {
    const stage = left.stage.localeCompare(right.stage);
    if (stage !== 0) return stage;
    if (left.round !== right.round) return left.round - right.round;
    return left.started_at.localeCompare(right.started_at);
  });
}

export function RunDetailClient({ run }: RunDetailClientProps) {
  const [streamStatus, setStreamStatus] = useState<StreamStatus>("connecting");
  const [stagesById, setStagesById] = useState(() =>
    reduceSnapshot(run.stages ?? []),
  );

  useEffect(() => {
    const source = new EventSource(
      `/api/runs/${encodeURIComponent(run.run_id)}/stream`,
    );

    source.addEventListener("open", () => {
      setStreamStatus("open");
    });

    source.addEventListener("snapshot", (event) => {
      const payload = parseJson(event.data);
      if (!Array.isArray(payload) || !payload.every(isStageRunSummary)) return;
      setStagesById(reduceSnapshot(payload));
    });

    source.addEventListener("delta", (event) => {
      const payload = parseJson(event.data);
      if (!isStageRunDelta(payload) || payload.run_id !== run.run_id) return;
      setStagesById((current) => reduceDelta(current, payload));
    });

    source.addEventListener("error", () => {
      setStreamStatus(source.readyState === EventSource.CLOSED ? "closed" : "error");
    });

    return () => {
      setStreamStatus("closed");
      source.close();
    };
  }, [run.run_id]);

  const stages = useMemo(() => sortStages([...stagesById.values()]), [stagesById]);

  return (
    <main className="min-h-screen bg-background px-4 py-8 text-foreground sm:px-6 lg:px-8">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-6">
        <header className="rounded-3xl border bg-card p-6 shadow-sm">
          <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
            <div className="space-y-2">
              <p className="text-sm font-medium text-muted-foreground">Run detail</p>
              <h1 className="font-[family-name:var(--font-manrope)] text-2xl font-semibold tracking-tight md:text-3xl">
                {run.run_id}
              </h1>
              <p className="max-w-2xl text-sm text-muted-foreground">
                Developer inspection view for stage state, timeline, panels, and refinement chain.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Badge variant="outline">run {run.status}</Badge>
              <Badge variant={streamStatus === "open" ? "secondary" : "outline"}>
                stream {streamStatus}
              </Badge>
            </div>
          </div>
        </header>

        <section className="grid gap-4 md:grid-cols-3">
          <div className="rounded-2xl border bg-card p-4">
            <p className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
              Started
            </p>
            <p className="mt-2 font-mono text-sm">{run.started_at}</p>
          </div>
          <div className="rounded-2xl border bg-card p-4">
            <p className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
              Ended
            </p>
            <p className="mt-2 font-mono text-sm">{run.ended_at ?? "running"}</p>
          </div>
          <div className="rounded-2xl border bg-card p-4">
            <p className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
              Stages
            </p>
            <p className="mt-2 font-mono text-sm">{stages.length}</p>
          </div>
        </section>

        <section className="rounded-3xl border bg-card p-6">
          <div className="mb-4 flex items-center justify-between gap-4">
            <div>
              <h2 className="font-[family-name:var(--font-manrope)] text-lg font-semibold">
                Pipeline timeline
              </h2>
              <p className="text-sm text-muted-foreground">
                Live stage status across the investment pipeline.
              </p>
            </div>
          </div>
          <PipelineTimeline stages={stages} />
        </section>

        <section className="rounded-3xl border bg-card p-6">
          <div className="mb-4">
            <h2 className="font-[family-name:var(--font-manrope)] text-lg font-semibold">
              Live activity
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Stage events, tool calls, and failures persisted for this run.
            </p>
          </div>
          <LiveActivity runId={run.run_id} fullWidth includeText={false} />
        </section>

        <section className="grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(280px,1fr)]">
          <div className="rounded-3xl border bg-card p-6">
            <h2 className="font-[family-name:var(--font-manrope)] text-lg font-semibold">
              Stage panels
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">Placeholder for task 017.</p>
            <div className="mt-4 space-y-3">
              {stages.map((stage) => (
                <div
                  key={stage.stage_run_id}
                  id={`stage-${stage.stage.toLowerCase().replace(/[_\s-]+/g, "")}-${stage.round}`}
                  className="rounded-2xl border p-4 scroll-mt-6"
                >
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <p className="font-medium">{stage.stage} round {stage.round}</p>
                      <p className="font-mono text-xs text-muted-foreground">{stage.stage_run_id}</p>
                    </div>
                    <Badge variant="outline">{stage.status}</Badge>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <aside className="rounded-3xl border bg-card p-6">
            <h2 className="font-[family-name:var(--font-manrope)] text-lg font-semibold">
              Refinement chain
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">Placeholder for task 018.</p>
          </aside>
        </section>
      </div>
    </main>
  );
}
