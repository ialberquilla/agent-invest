import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { StrategyResultReport } from "@/components/StrategyResultReport";
import type { StrategyResult } from "@/lib/types";

function baseResult(
  overrides: Partial<StrategyResult> = {},
): StrategyResult {
  return {
    title: "Test strategy",
    summary: "",
    reasoning: "",
    allocation: [],
    kpis: {},
    assumptions: [],
    risks: [],
    next_steps: [],
    backtest: {},
    charts: {},
    ...overrides,
  } as StrategyResult;
}

describe("StrategyResultReport iterate buttons", () => {
  it("renders a button per suggested rerun and fires onRerun on click", () => {
    const onRerun = vi.fn();
    const suggestion = {
      label: "Fewer assets",
      rationale: "Concentrate into 3-4 assets.",
      overrides: { asset_count_min: 3, asset_count_max: 4 },
    };
    render(
      <StrategyResultReport
        result={baseResult({ suggested_reruns: [suggestion] })}
        onRerun={onRerun}
      />,
    );

    const button = screen.getByRole("button", { name: "Fewer assets" });
    fireEvent.click(button);
    expect(onRerun).toHaveBeenCalledWith(suggestion);
  });

  it("hides the iterate section when onRerun is absent", () => {
    render(
      <StrategyResultReport
        result={baseResult({
          suggested_reruns: [
            { label: "Fewer assets", rationale: "x", overrides: {} },
          ],
        })}
      />,
    );

    expect(
      screen.queryByText("Iterate on this strategy"),
    ).not.toBeInTheDocument();
  });

  it("disables the buttons while a rerun is in flight", () => {
    render(
      <StrategyResultReport
        result={baseResult({
          suggested_reruns: [
            { label: "More assets", rationale: "x", overrides: {} },
          ],
        })}
        onRerun={vi.fn()}
        rerunDisabled
      />,
    );

    expect(screen.getByRole("button", { name: "More assets" })).toBeDisabled();
  });
});
