import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { RefinementChain } from "./RefinementChain";
import type { StageRunDetail, StageRunSummary } from "@/lib/types";

function stage(stageName: string, round: number): StageRunSummary {
  return {
    stage_run_id: `${stageName}-${round}`,
    stage: stageName,
    round,
    status: "succeeded",
    started_at: "2026-01-01T00:00:00Z",
    ended_at: "2026-01-01T00:01:00Z",
    model: "test-model",
    tokens: { input: 100, output: 50 },
  };
}

function detail(summary: StageRunSummary, output: unknown): StageRunDetail {
  return {
    ...summary,
    run_id: "run-1",
    opencode_session_id: null,
    input: {},
    output,
    error: null,
  };
}

function designerOutput(round: number) {
  return {
    batch_id: `batch-${round}`,
    candidates: [
      {
        candidate_id: `r${round}_c1`,
        template_id: "periodic_rebalance",
        kpis: {
          cagr: 0.12,
          sharpe_ratio: 1.4,
          max_drawdown: -0.18,
          final_equity_multiple: 1.32,
        },
      },
      {
        candidate_id: `r${round}_c2`,
        template_id: "momentum",
        kpis: {
          cagr: 0.08,
          sharpe_ratio: 0.9,
          max_drawdown: -0.28,
          final_equity_multiple: 1.14,
        },
      },
    ],
  };
}

describe("RefinementChain", () => {
  it("renders a single round and gracefully omits connectors", async () => {
    const designer = stage("designer", 1);
    const adjudicator = stage("adjudicator", 1);
    const fetchStage = vi.fn<(stageRunId: string) => Promise<StageRunDetail>>(
      (stageRunId) => {
        if (stageRunId === designer.stage_run_id)
          return Promise.resolve(detail(designer, designerOutput(1)));
        return Promise.resolve(
          detail(adjudicator, {
            kind: "winner",
            candidate_id: "r1_c1",
            justification: "Best risk-adjusted return.",
          }),
        );
      },
    );

    render(
      <RefinementChain
        stages={[designer, adjudicator]}
        fetchStage={fetchStage}
      />,
    );

    expect(await screen.findAllByText("r1_c1")).toHaveLength(2);
    expect(screen.getByText("periodic_rebalance")).toBeInTheDocument();
    expect(screen.getByText("Best risk-adjusted return.")).toBeInTheDocument();
    expect(screen.queryByText(/Round 1 to 2/i)).not.toBeInTheDocument();
    await waitFor(() => expect(fetchStage).toHaveBeenCalledTimes(2));
  });

  it("renders three rounds with refinement reason codes and details", async () => {
    const stages = [1, 2, 3].flatMap((round) => [
      stage("designer", round),
      stage("adjudicator", round),
    ]);
    const details = new Map(
      stages.map((summary) => {
        if (summary.stage === "designer") {
          return [
            summary.stage_run_id,
            detail(summary, designerOutput(summary.round)),
          ] as const;
        }

        if (summary.round === 3) {
          return [
            summary.stage_run_id,
            detail(summary, {
              kind: "winner",
              candidate_id: "r3_c1",
              justification: "Round three satisfies the thesis.",
            }),
          ] as const;
        }

        return [
          summary.stage_run_id,
          detail(summary, {
            kind: "refine",
            reasons: [
              {
                candidate_id: `r${summary.round}_c2`,
                reason:
                  summary.round === 1
                    ? "constraint_violation"
                    : "underperformed_benchmark",
                detail:
                  summary.round === 1
                    ? "Drawdown breached the mandate."
                    : "Sharpe lagged bitcoin.",
                suggested_fix: "Reduce risk concentration.",
              },
            ],
          }),
        ] as const;
      }),
    );
    const fetchStage = vi.fn<(stageRunId: string) => Promise<StageRunDetail>>(
      (stageRunId) => Promise.resolve(details.get(stageRunId)!),
    );

    render(<RefinementChain stages={stages} fetchStage={fetchStage} />);

    expect(await screen.findAllByText("constraint_violation")).toHaveLength(2);
    expect(
      screen.getByText("Drawdown breached the mandate."),
    ).toBeInTheDocument();
    expect(screen.getAllByText("underperformed_benchmark")).toHaveLength(2);
    expect(screen.getByText("Sharpe lagged bitcoin.")).toBeInTheDocument();
    expect(screen.getByText("Round 1 to 2")).toBeInTheDocument();
    expect(screen.getByText("Round 2 to 3")).toBeInTheDocument();
    expect(
      screen.getByText("Round three satisfies the thesis."),
    ).toBeInTheDocument();
    await waitFor(() => expect(fetchStage).toHaveBeenCalledTimes(6));
  });
});
