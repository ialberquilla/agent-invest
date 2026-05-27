import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { PipelineTimeline } from "./PipelineTimeline";
import { StageRunSummary } from "@/lib/types";

function stage(
  stageName: string,
  round: number,
  status: string,
  startedAt = `2026-01-01T00:00:0${round}Z`,
): StageRunSummary {
  return {
    stage_run_id: `${stageName}-${round}-${status}`,
    stage: stageName,
    round,
    status,
    started_at: startedAt,
    ended_at: status === "running" ? null : startedAt,
    model: "test-model",
    tokens: { input: null, output: null },
  };
}

describe("PipelineTimeline", () => {
  it("renders all four fixed stages with status pills", () => {
    render(
      <PipelineTimeline
        stages={[
          stage("thesis", 1, "succeeded"),
          stage("designer", 1, "running"),
          stage("adjudicator", 1, "failed"),
        ]}
      />,
    );

    expect(screen.getByLabelText("Thesis stage")).toHaveTextContent("succeeded");
    expect(screen.getByLabelText("Designer stage")).toHaveTextContent("running");
    expect(screen.getByLabelText("Adjudicator stage")).toHaveTextContent("failed");
    expect(screen.getByLabelText("Reporter stage")).toHaveTextContent("pending");
  });

  it("renders keyboard-focusable round links for designer and adjudicator", () => {
    render(
      <PipelineTimeline
        stages={[
          stage("designer", 1, "succeeded"),
          stage("designer", 2, "running"),
          stage("adjudicator", 1, "pending"),
        ]}
      />,
    );

    const designer = screen.getByLabelText("Designer rounds");
    const adjudicator = screen.getByLabelText("Adjudicator rounds");

    expect(within(designer).getByRole("link", { name: "Designer round 1 succeeded" })).toHaveAttribute("href", "#stage-designer-1");
    expect(within(designer).getByRole("link", { name: "Designer round 2 running" })).toHaveAttribute("href", "#stage-designer-2");
    expect(within(designer).getByRole("link", { name: "Designer round 3 pending" })).toHaveAttribute("href", "#stage-designer-3");
    expect(within(adjudicator).getByRole("link", { name: "Adjudicator round 1 pending" })).toHaveAttribute("href", "#stage-adjudicator-1");
  });
});
