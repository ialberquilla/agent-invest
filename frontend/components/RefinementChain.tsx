"use client";

import { useEffect, useMemo, useState } from "react";

import { Badge } from "@/components/ui/badge";
import type {
  StageRunDetail,
  StageRunSummary,
  StrategyKpis,
} from "@/lib/types";

type RefinementChainProps = {
  stages: StageRunSummary[];
  fetchStage: (stageRunId: string) => Promise<StageRunDetail>;
};

type CandidateSummary = {
  candidate_id: string;
  template_id: string;
  kpis: StrategyKpis | null;
};

type RefinementReason = {
  candidate_id?: string;
  reason: string;
  detail: string;
  suggested_fix?: string;
};

type RoundSummary = {
  round: number;
  designer?: StageRunSummary;
  adjudicator?: StageRunSummary;
  candidates: CandidateSummary[];
  winnerCandidateId: string | null;
  winnerJustification: string | null;
  reasons: RefinementReason[];
};

const KPI_ITEMS: Array<{
  key: keyof StrategyKpis;
  label: string;
  format: (value: number | null | undefined) => string;
}> = [
  { key: "cagr", label: "CAGR", format: formatPercent },
  { key: "sharpe_ratio", label: "Sharpe", format: formatNumber },
  { key: "max_drawdown", label: "Max DD", format: formatPercent },
  { key: "final_equity_multiple", label: "Multiple", format: formatMultiple },
];

function hasValue(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function formatPercent(value: number | null | undefined) {
  if (!hasValue(value)) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatNumber(value: number | null | undefined) {
  if (!hasValue(value)) return "-";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(
    value,
  );
}

function formatMultiple(value: number | null | undefined) {
  if (!hasValue(value)) return "-";
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value)}x`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeStageName(stage: string) {
  return stage.toLowerCase().replace(/[\s_-]+/g, "");
}

function stageForRound(stages: StageRunSummary[], name: string, round: number) {
  return stages.find(
    (stage) =>
      normalizeStageName(stage.stage) === name && stage.round === round,
  );
}

function readString(value: unknown) {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readKpis(value: unknown): StrategyKpis | null {
  if (!isRecord(value)) return null;

  const source = isRecord(value.kpis)
    ? value.kpis
    : isRecord(value.allocation_metrics)
      ? value.allocation_metrics
      : isRecord(value.tactical_metrics)
        ? value.tactical_metrics
        : value;

  return source as StrategyKpis;
}

function candidateKpis(
  candidate: Record<string, unknown>,
  output: Record<string, unknown>,
) {
  const direct = readKpis(candidate);
  if (direct) return direct;

  const kpis = output.kpis;
  const candidateId = readString(candidate.candidate_id);
  if (!candidateId || !isRecord(kpis)) return null;

  return readKpis(kpis[candidateId]);
}

function readCandidates(output: unknown): CandidateSummary[] {
  if (!isRecord(output) || !Array.isArray(output.candidates)) return [];

  return output.candidates.filter(isRecord).map((candidate) => ({
    candidate_id: readString(candidate.candidate_id) ?? "unknown-candidate",
    template_id: readString(candidate.template_id) ?? "unknown-template",
    kpis: candidateKpis(candidate, output),
  }));
}

function readReasons(output: unknown): RefinementReason[] {
  if (!isRecord(output) || !Array.isArray(output.reasons)) return [];

  return output.reasons.filter(isRecord).map((reason) => ({
    candidate_id: readString(reason.candidate_id) ?? undefined,
    reason: readString(reason.reason) ?? "unknown_reason",
    detail: readString(reason.detail) ?? "No detail provided",
    suggested_fix: readString(reason.suggested_fix) ?? undefined,
  }));
}

function readWinner(output: unknown) {
  if (!isRecord(output) || output.kind !== "winner") {
    return { winnerCandidateId: null, winnerJustification: null };
  }

  return {
    winnerCandidateId: readString(output.candidate_id),
    winnerJustification: readString(output.justification),
  };
}

function DetailState({ message }: { message: string }) {
  return (
    <div className="rounded-xl border border-dashed bg-muted/20 p-4 text-sm text-muted-foreground">
      {message}
    </div>
  );
}

function CandidateCard({ candidate }: { candidate: CandidateSummary }) {
  return (
    <article className="rounded-xl border bg-card p-3 shadow-xs">
      <div className="flex min-w-0 items-start justify-between gap-3">
        <div className="min-w-0">
          <h4 className="truncate font-mono text-sm font-semibold">
            {candidate.candidate_id}
          </h4>
          <p className="truncate text-xs text-muted-foreground">
            {candidate.template_id}
          </p>
        </div>
      </div>
      <dl className="mt-3 grid grid-cols-2 gap-2">
        {KPI_ITEMS.map((item) => (
          <div key={item.key} className="rounded-lg bg-muted/40 px-2 py-1.5">
            <dt className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
              {item.label}
            </dt>
            <dd className="mt-0.5 font-mono text-xs font-semibold">
              {item.format(candidate.kpis?.[item.key])}
            </dd>
          </div>
        ))}
      </dl>
    </article>
  );
}

function Verdict({ round }: { round: RoundSummary }) {
  if (round.winnerCandidateId) {
    return (
      <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-3">
        <Badge
          variant="outline"
          className="border-emerald-500/40 text-emerald-700 dark:text-emerald-300"
        >
          Winner
        </Badge>
        <p className="mt-2 font-mono text-sm font-semibold">
          {round.winnerCandidateId}
        </p>
        {round.winnerJustification ? (
          <p className="mt-2 text-sm leading-6 text-muted-foreground">
            {round.winnerJustification}
          </p>
        ) : null}
      </div>
    );
  }

  if (round.reasons.length === 0)
    return (
      <DetailState message="No adjudicator verdict loaded for this round." />
    );

  return (
    <div className="space-y-2">
      {round.reasons.map((reason, index) => (
        <article
          key={`${reason.reason}-${index}`}
          className="rounded-xl border bg-muted/30 p-3"
        >
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="secondary" className="font-mono text-[11px]">
              {reason.reason}
            </Badge>
            {reason.candidate_id ? (
              <span className="font-mono text-xs text-muted-foreground">
                {reason.candidate_id}
              </span>
            ) : null}
          </div>
          <p className="mt-2 text-sm leading-6">{reason.detail}</p>
          {reason.suggested_fix ? (
            <p className="mt-2 text-xs text-muted-foreground">
              Fix: {reason.suggested_fix}
            </p>
          ) : null}
        </article>
      ))}
    </div>
  );
}

export function RefinementChain({ stages, fetchStage }: RefinementChainProps) {
  const [details, setDetails] = useState<Record<string, StageRunDetail>>({});
  const [error, setError] = useState<string | null>(null);

  const roundStages = useMemo(
    () =>
      [1, 2, 3]
        .map((round) => ({
          round,
          designer: stageForRound(stages, "designer", round),
          adjudicator: stageForRound(stages, "adjudicator", round),
        }))
        .filter((round) => round.designer || round.adjudicator),
    [stages],
  );

  useEffect(() => {
    const missing = roundStages
      .flatMap((round) => [round.designer, round.adjudicator])
      .filter(
        (stage): stage is StageRunSummary =>
          stage !== undefined && !details[stage.stage_run_id],
      );
    if (missing.length === 0) return;

    let cancelled = false;
    Promise.all(missing.map((stage) => fetchStage(stage.stage_run_id)))
      .then((loaded) => {
        if (cancelled) return;
        setDetails((current) => ({
          ...current,
          ...Object.fromEntries(
            loaded.map((detail) => [detail.stage_run_id, detail]),
          ),
        }));
      })
      .catch((caught) => {
        if (!cancelled)
          setError(
            caught instanceof Error
              ? caught.message
              : "Failed to load refinement chain",
          );
      });

    return () => {
      cancelled = true;
    };
  }, [details, fetchStage, roundStages]);

  const rounds = roundStages.map((round): RoundSummary => {
    const designerDetail = round.designer
      ? details[round.designer.stage_run_id]
      : undefined;
    const adjudicatorDetail = round.adjudicator
      ? details[round.adjudicator.stage_run_id]
      : undefined;
    const winner = readWinner(adjudicatorDetail?.output);

    return {
      ...round,
      candidates: readCandidates(designerDetail?.output),
      reasons: readReasons(adjudicatorDetail?.output),
      ...winner,
    };
  });

  if (roundStages.length === 0) {
    return (
      <DetailState message="No Designer or Adjudicator stages are available yet." />
    );
  }

  return (
    <div className="space-y-4">
      {error ? (
        <p className="rounded-xl border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
          {error}
        </p>
      ) : null}
      {rounds.map((round, index) => {
        const nextReasonCodes = round.reasons.map((reason) => reason.reason);
        return (
          <section key={round.round} className="rounded-2xl border bg-card p-4">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h3 className="font-[family-name:var(--font-manrope)] text-base font-semibold">
                  Round {round.round}
                </h3>
                <p className="text-sm text-muted-foreground">
                  Designer batch and Adjudicator decision.
                </p>
              </div>
              <Badge variant="outline">
                {round.candidates.length} candidates
              </Badge>
            </div>

            <div className="grid gap-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(260px,0.85fr)]">
              <div className="min-w-0">
                <h4 className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Candidate batch
                </h4>
                {round.candidates.length > 0 ? (
                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
                    {round.candidates.map((candidate) => (
                      <CandidateCard
                        key={candidate.candidate_id}
                        candidate={candidate}
                      />
                    ))}
                  </div>
                ) : (
                  <DetailState message="Loading candidate batch..." />
                )}
              </div>

              <div className="min-w-0">
                <h4 className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Adjudicator verdict
                </h4>
                <Verdict round={round} />
              </div>
            </div>

            {index < rounds.length - 1 ? (
              <div className="mt-4 rounded-xl border border-dashed bg-muted/20 p-3 text-sm">
                <span className="font-medium">
                  Round {round.round} to {rounds[index + 1].round}
                </span>
                <span className="mx-2 text-muted-foreground">via</span>
                {nextReasonCodes.length > 0 ? (
                  <span className="inline-flex flex-wrap gap-1 align-middle">
                    {nextReasonCodes.map((code) => (
                      <Badge
                        key={code}
                        variant="secondary"
                        className="font-mono text-[11px]"
                      >
                        {code}
                      </Badge>
                    ))}
                  </span>
                ) : (
                  <span className="text-muted-foreground">
                    no refinement codes
                  </span>
                )}
              </div>
            ) : null}
          </section>
        );
      })}
    </div>
  );
}
