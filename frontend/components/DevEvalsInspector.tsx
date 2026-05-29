"use client";

import { useState, useTransition } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

export type EvalRunSummary = {
  eval_run_id: string;
  stage: string;
  fixture_id: string;
  model: string;
  passed: boolean;
  score: number | null;
  duration_ms: number;
  created_at: string;
};

type EvalRule = {
  rule?: string;
  passed?: boolean;
  message?: string;
  score?: number;
};

type EvalRunDetail = EvalRunSummary & {
  diagnostics: { rules?: EvalRule[] } | unknown;
  expectations: unknown;
  output: unknown;
};

type DevEvalsInspectorProps = {
  initialRuns: EvalRunSummary[];
  initialStage?: string;
  initialFixtureId?: string;
};

const STAGES = ["", "thesis", "designer", "adjudicator", "reporter"];

function formatDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatScore(value: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "No score";
  return value.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function rulesFromDiagnostics(value: unknown): EvalRule[] {
  if (!isRecord(value) || !Array.isArray(value.rules)) return [];
  return value.rules.filter(isRecord).map((rule) => ({
    rule: typeof rule.rule === "string" ? rule.rule : undefined,
    passed: typeof rule.passed === "boolean" ? rule.passed : undefined,
    message: typeof rule.message === "string" ? rule.message : undefined,
    score: typeof rule.score === "number" ? rule.score : undefined,
  }));
}

function JsonBlock({ title, value }: { title: string; value: unknown }) {
  return (
    <section className="min-w-0 rounded-2xl border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">{title}</h2>
      </div>
      <pre className="max-h-[34rem] overflow-auto p-4 font-mono text-xs leading-relaxed">
        {JSON.stringify(value, null, 2)}
      </pre>
    </section>
  );
}

function RuleBadges({ diagnostics }: { diagnostics: unknown }) {
  const rules = rulesFromDiagnostics(diagnostics);
  if (rules.length === 0) {
    return <p className="text-sm text-muted-foreground">No rule diagnostics found.</p>;
  }

  return (
    <div className="flex flex-wrap gap-2">
      {rules.map((rule, index) => (
        <Badge
          key={`${rule.rule ?? "rule"}-${index}`}
          variant="outline"
          className={cn(
            rule.passed
              ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
              : "border-destructive/30 bg-destructive/10 text-destructive",
          )}
          title={rule.message}
        >
          {rule.passed ? "PASS" : "FAIL"} {rule.rule ?? "unnamed"}
        </Badge>
      ))}
    </div>
  );
}

export function DevEvalsInspector({
  initialRuns,
  initialStage = "",
  initialFixtureId = "",
}: DevEvalsInspectorProps) {
  const [runs, setRuns] = useState(initialRuns);
  const [stage, setStage] = useState(initialStage);
  const [fixtureId, setFixtureId] = useState(initialFixtureId);
  const [selected, setSelected] = useState<EvalRunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  async function loadRuns(nextStage = stage, nextFixtureId = fixtureId) {
    const params = new URLSearchParams({ dev: "1" });
    if (nextStage) params.set("stage", nextStage);
    if (nextFixtureId) params.set("fixture_id", nextFixtureId);

    const response = await fetch(`/api/dev/evals?${params.toString()}`);
    if (!response.ok) throw new Error("Failed to load eval runs");
    const nextRuns = (await response.json()) as EvalRunSummary[];
    setRuns(nextRuns);
    window.history.replaceState(null, "", `/dev/evals?${params.toString()}`);
  }

  function applyFilters() {
    startTransition(async () => {
      setError(null);
      try {
        await loadRuns();
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : "Failed to load eval runs");
      }
    });
  }

  async function selectRun(run: EvalRunSummary) {
    setError(null);
    const response = await fetch(`/api/dev/evals/${encodeURIComponent(run.eval_run_id)}`);
    if (!response.ok) {
      setError("Failed to load eval run detail");
      return;
    }
    setSelected((await response.json()) as EvalRunDetail);
  }

  return (
    <main className="min-h-screen bg-background px-4 py-6 text-foreground sm:px-6 lg:px-8">
      <div className="mx-auto grid max-w-7xl gap-6">
        <header className="rounded-3xl border bg-card p-5 shadow-sm">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">Dev inspector</p>
          <div className="mt-2 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <h1 className="text-3xl font-semibold tracking-tight">Stage eval runs</h1>
              <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
                Recent persisted eval runs with fixture expectations, stage output, and scorer diagnostics.
              </p>
            </div>
            <div className="flex flex-col gap-2 sm:flex-row sm:items-end">
              <label className="grid gap-1 text-sm">
                <span className="font-medium">Stage</span>
                <select
                  value={stage}
                  onChange={(event) => setStage(event.target.value)}
                  className="h-10 rounded-md border bg-background px-3 text-sm"
                >
                  {STAGES.map((option) => (
                    <option key={option || "all"} value={option}>
                      {option || "All stages"}
                    </option>
                  ))}
                </select>
              </label>
              <label className="grid gap-1 text-sm">
                <span className="font-medium">Fixture id</span>
                <Input value={fixtureId} onChange={(event) => setFixtureId(event.target.value)} placeholder="fixture_id" />
              </label>
              <Button type="button" onClick={applyFilters} disabled={isPending}>
                {isPending ? "Loading..." : "Apply"}
              </Button>
            </div>
          </div>
          {error ? <p className="mt-3 text-sm text-destructive">{error}</p> : null}
        </header>

        <div className="grid gap-6 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.35fr)]">
          <section className="overflow-hidden rounded-2xl border bg-card">
            <div className="border-b px-4 py-3">
              <h2 className="text-sm font-semibold">Recent runs</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[48rem] text-left text-sm">
                <thead className="bg-muted/40 text-xs uppercase text-muted-foreground">
                  <tr>
                    <th className="px-4 py-3 font-medium">Created</th>
                    <th className="px-4 py-3 font-medium">Stage</th>
                    <th className="px-4 py-3 font-medium">Fixture</th>
                    <th className="px-4 py-3 font-medium">Result</th>
                    <th className="px-4 py-3 font-medium">Score</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <tr
                      key={run.eval_run_id}
                      className={cn(
                        "cursor-pointer border-t transition-colors hover:bg-muted/30",
                        selected?.eval_run_id === run.eval_run_id ? "bg-muted/40" : null,
                      )}
                      onClick={() => void selectRun(run)}
                    >
                      <td className="px-4 py-3 text-xs text-muted-foreground">{formatDate(run.created_at)}</td>
                      <td className="px-4 py-3 capitalize">{run.stage}</td>
                      <td className="max-w-64 truncate px-4 py-3 font-mono text-xs" title={run.fixture_id}>{run.fixture_id}</td>
                      <td className="px-4 py-3">
                        <Badge variant="outline" className={run.passed ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700" : "border-destructive/30 bg-destructive/10 text-destructive"}>
                          {run.passed ? "PASS" : "FAIL"}
                        </Badge>
                      </td>
                      <td className="px-4 py-3 font-mono text-xs">{formatScore(run.score)}</td>
                    </tr>
                  ))}
                  {runs.length === 0 ? (
                    <tr>
                      <td colSpan={5} className="px-4 py-8 text-center text-sm text-muted-foreground">
                        No eval runs found. Run a stage eval with `--save-to-db` first.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>

          <aside className="grid content-start gap-4">
            {selected ? (
              <>
                <section className="rounded-2xl border bg-card p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <h2 className="text-sm font-semibold">{selected.fixture_id}</h2>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {selected.stage} · {selected.model} · {selected.duration_ms}ms
                      </p>
                    </div>
                    <Badge variant="outline" className={selected.passed ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700" : "border-destructive/30 bg-destructive/10 text-destructive"}>
                      {selected.passed ? "PASS" : "FAIL"}
                    </Badge>
                  </div>
                  <div className="mt-4">
                    <RuleBadges diagnostics={selected.diagnostics} />
                  </div>
                </section>
                <div className="grid gap-4 lg:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
                  <JsonBlock title="Fixture expectations" value={selected.expectations} />
                  <JsonBlock title="Stage output" value={selected.output} />
                </div>
              </>
            ) : (
              <section className="rounded-2xl border border-dashed bg-card p-8 text-center text-sm text-muted-foreground">
                Select a row to inspect expectations, output, and per-rule diagnostics.
              </section>
            )}
          </aside>
        </div>
      </div>
    </main>
  );
}
