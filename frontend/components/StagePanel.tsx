"use client";

import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import type { StageRunDetail, StageRunSummary, StrategyKpis } from "@/lib/types";
import { cn } from "@/lib/utils";

type StagePanelProps = {
  stage: StageRunSummary;
  fetchStage: (stageRunId: string) => Promise<StageRunDetail>;
};

const statusClasses: Record<string, string> = {
  running: "border-blue-500/30 bg-blue-500/10 text-blue-700 dark:text-blue-300",
  succeeded: "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
  success: "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
  completed: "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
  failed: "border-destructive/30 bg-destructive/10 text-destructive",
  error: "border-destructive/30 bg-destructive/10 text-destructive",
};

const KPI_ITEMS: Array<{
  key: keyof StrategyKpis;
  label: string;
  format: (value: number | null | undefined) => string;
}> = [
  { key: "cagr", label: "CAGR", format: formatPercent },
  { key: "sharpe_ratio", label: "Sharpe", format: formatNumber },
  { key: "sortino_ratio", label: "Sortino", format: formatNumber },
  { key: "max_drawdown", label: "Max drawdown", format: formatPercent },
  { key: "monthly_hit_rate", label: "Monthly hit rate", format: formatPercent },
  { key: "final_equity_usd", label: "Final equity", format: formatCurrency },
  { key: "final_equity_multiple", label: "Final multiple", format: formatMultiple },
  { key: "total_trading_cost_usd", label: "Trading cost", format: formatCurrency },
  { key: "total_num_swaps", label: "Swaps", format: formatNumber },
];

function hasValue(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function formatPercent(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatCurrency(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatNumber(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 2,
  }).format(value);
}

function formatMultiple(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value)}x`;
}

function formatDuration(startedAt: string, endedAt: string | null) {
  if (!endedAt) return "In progress";
  const started = new Date(startedAt).getTime();
  const ended = new Date(endedAt).getTime();
  if (Number.isNaN(started) || Number.isNaN(ended) || ended < started) return "Unknown duration";
  const seconds = Math.round((ended - started) / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return remainingSeconds ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`;
}

function formatTokens(stage: StageRunSummary) {
  const input = stage.tokens.input ?? 0;
  const output = stage.tokens.output ?? 0;
  const total = input + output;
  if (!total) return "No tokens";
  return `${new Intl.NumberFormat("en-US").format(total)} tokens`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function outputKpis(output: unknown): StrategyKpis | null {
  if (!isRecord(output) || !isRecord(output.kpis)) return null;
  return output.kpis as StrategyKpis;
}

function JsonBlock({ label, value }: { label: string; value: unknown }) {
  return (
    <section className="min-w-0">
      <h4 className="text-sm font-semibold">{label}</h4>
      <pre className="mt-2 max-h-80 overflow-auto rounded-xl border bg-muted/30 p-3 text-xs leading-relaxed">
        {JSON.stringify(value, null, 2)}
      </pre>
    </section>
  );
}

function BacktestKpiTable({ kpis }: { kpis: StrategyKpis }) {
  return (
    <div className="overflow-x-auto rounded-xl border">
      <table className="w-full min-w-96 text-left text-sm">
        <thead className="bg-muted/40 text-xs uppercase text-muted-foreground">
          <tr>
            <th className="px-3 py-2 font-medium">Metric</th>
            <th className="px-3 py-2 font-medium">Value</th>
          </tr>
        </thead>
        <tbody>
          {KPI_ITEMS.map((item) => (
            <tr key={item.key} className="border-t">
              <td className="px-3 py-2 text-muted-foreground">{item.label}</td>
              <td className="px-3 py-2 font-medium">{item.format(kpis[item.key])}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function StagePanel({ stage, fetchStage }: StagePanelProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [detail, setDetail] = useState<StageRunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  async function togglePanel() {
    const nextOpen = !isOpen;
    setIsOpen(nextOpen);
    if (!nextOpen || detail || isLoading) return;

    setIsLoading(true);
    setError(null);
    try {
      setDetail(await fetchStage(stage.stage_run_id));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Failed to load stage details");
    } finally {
      setIsLoading(false);
    }
  }

  const kpis = outputKpis(detail?.output);

  return (
    <section id={`stage-${stage.stage}-${stage.round}`} className="rounded-2xl border bg-card">
      <button
        type="button"
        className="flex w-full flex-col gap-3 p-4 text-left outline-none transition-colors hover:bg-muted/20 focus-visible:ring-3 focus-visible:ring-ring/50 sm:flex-row sm:items-center sm:justify-between"
        aria-expanded={isOpen}
        onClick={togglePanel}
      >
        <span className="min-w-0">
          <span className="block text-sm font-semibold capitalize">
            {stage.stage} round {stage.round}
          </span>
          <span className="mt-1 block truncate text-xs text-muted-foreground">{stage.model}</span>
        </span>
        <span className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          <Badge
            variant="outline"
            className={cn("capitalize", statusClasses[stage.status] ?? "border-border bg-muted/40 text-muted-foreground")}
          >
            {stage.status}
          </Badge>
          <span>{formatDuration(stage.started_at, stage.ended_at)}</span>
          <span>{formatTokens(stage)}</span>
        </span>
      </button>

      {isOpen ? (
        <div className="grid gap-4 border-t p-4">
          {isLoading ? <p className="text-sm text-muted-foreground">Loading stage details...</p> : null}
          {error ? <p className="text-sm text-destructive">{error}</p> : null}
          {detail ? (
            <>
              {detail.error ? <p className="rounded-xl border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{detail.error}</p> : null}
              {kpis ? <BacktestKpiTable kpis={kpis} /> : null}
              <div className="grid gap-4 lg:grid-cols-2">
                <JsonBlock label="Input" value={detail.input} />
                <JsonBlock label="Output" value={detail.output} />
              </div>
            </>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
