import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { StagePanel } from "./StagePanel";
import type { StageRunDetail, StageRunSummary } from "@/lib/types";

const stage: StageRunSummary = {
  stage_run_id: "stage-1",
  stage: "reporter",
  round: 1,
  status: "succeeded",
  started_at: "2026-01-01T00:00:00Z",
  ended_at: "2026-01-01T00:01:05Z",
  model: "test-model",
  tokens: { input: 1000, output: 250 },
};

const detail: StageRunDetail = {
  ...stage,
  run_id: "run-1",
  opencode_session_id: null,
  input: { prompt: "Build a strategy" },
  output: {
    summary: "Strategy complete",
    kpis: {
      cagr: 0.1234,
      sharpe_ratio: 1.456,
      max_drawdown: -0.2345,
      final_equity_usd: 123456,
      final_equity_multiple: 1.23,
    },
  },
  error: null,
};

describe("StagePanel", () => {
  it("renders collapsed summary without fetching details", () => {
    const fetchStage = vi.fn<() => Promise<StageRunDetail>>();

    render(<StagePanel stage={stage} fetchStage={fetchStage} />);

    expect(screen.getByRole("button", { name: /reporter round 1/i })).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByText("succeeded")).toBeInTheDocument();
    expect(screen.getByText("1m 5s")).toBeInTheDocument();
    expect(screen.getByText("1,250 tokens")).toBeInTheDocument();
    expect(screen.queryByText("Input")).not.toBeInTheDocument();
    expect(fetchStage).not.toHaveBeenCalled();
  });

  it("fetches and renders details when expanded", async () => {
    const fetchStage = vi.fn<() => Promise<StageRunDetail>>().mockResolvedValue(detail);

    render(<StagePanel stage={stage} fetchStage={fetchStage} />);

    fireEvent.click(screen.getByRole("button", { name: /reporter round 1/i }));

    await waitFor(() => expect(fetchStage).toHaveBeenCalledWith("stage-1"));
    expect(await screen.findByText("Input")).toBeInTheDocument();
    expect(screen.getByText(/Build a strategy/)).toBeInTheDocument();
    expect(screen.getByText("CAGR")).toBeInTheDocument();
    expect(screen.getByText("12.34%")).toBeInTheDocument();
    expect(screen.getByText("Final equity")).toBeInTheDocument();
    expect(screen.getByText("$123,456")).toBeInTheDocument();
  });
});
