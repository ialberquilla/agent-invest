import assert from "node:assert/strict";
import test from "node:test";

import { runPipeline, type PipelineRunners } from "../src/agent/pipeline";
import type { AdjudicatorStageOutput } from "../src/agent/stages/adjudicator";
import type { DesignerStageOutput } from "../src/agent/stages/designer";
import type { ThesisJson } from "../src/agent/stages/thesis";

const runId = "run-pipeline-test";

const thesis: ThesisJson = {
  run_id: runId,
  objective: "balanced",
  primary_factors: [{ factor: "sharpe", direction: "high" }],
  constraints: { max_drawdown: -0.35 },
  horizon_days: 365,
  interpretation_notes: "Balanced risk-adjusted growth.",
};

function batch(round: 1 | 2 | 3): DesignerStageOutput {
  return {
    batch_id: `batch-${round}`,
    candidates: [
      {
        candidate_id: `candidate-${round}`,
        label: `Candidate ${round}`,
        template_id: "periodic_rebalance",
        select_top: 5,
        ranking: [{ factor: "sharpe", direction: "high", weight: 1 }],
        config: { weighting: "equal" },
      },
    ],
    kpis: { round },
  };
}

function refine(round: 1 | 2 | 3): AdjudicatorStageOutput {
  return {
    kind: "refine",
    reasons: [
      {
        candidate_id: `candidate-${round}`,
        reason: "weak_performance",
        detail: `Round ${round} did not clear the benchmark.`,
      },
    ],
  };
}

function winner(round: 1 | 2 | 3): AdjudicatorStageOutput {
  return {
    kind: "winner",
    candidate_id: `candidate-${round}`,
    justification: `Candidate ${round} wins.`,
  };
}

function createRunners(adjudications: AdjudicatorStageOutput[]) {
  const calls: Array<{ stage: string; input: unknown; round: number }> = [];

  const runners: PipelineRunners = {
    thesis: {
      async run(input, _runId, round) {
        calls.push({ stage: "thesis", input, round });
        return { thesis_id: "thesis-1", thesis };
      },
    },
    designer: {
      async run(input, _runId, round) {
        calls.push({ stage: "designer", input, round });
        return batch(input.round);
      },
    },
    adjudicator: {
      async run(input, _runId, round) {
        calls.push({ stage: "adjudicator", input, round });
        const output = adjudications.shift();
        if (!output) throw new Error("missing adjudication fixture");
        return output;
      },
    },
    reporter: {
      async run(input, _runId, round) {
        calls.push({ stage: "reporter", input, round });
        return { result_id: "result-1" };
      },
    },
  };

  return { calls, runners };
}

test("pipeline exits with a winner in round 1", async () => {
  const { calls, runners } = createRunners([winner(1)]);

  const result = await runPipeline(runId, "balanced brief", runners);

  assert.deepEqual(result, { result_id: "result-1" });
  assert.deepEqual(
    calls.map((call) => [call.stage, call.round]),
    [
      ["thesis", 1],
      ["designer", 1],
      ["adjudicator", 1],
      ["reporter", 1],
    ],
  );
  assert.equal(reporterInput(calls).winner_candidate_id, "candidate-1");
  assert.equal(reporterInput(calls).candidate_batch_id, "batch-1");
});

test("pipeline refines once then exits with a round 2 winner", async () => {
  const round1Refinement = refine(1);
  const { calls, runners } = createRunners([round1Refinement, winner(2)]);

  await runPipeline(runId, "balanced brief", runners);

  const designerRound2 = calls.find(
    (call) => call.stage === "designer" && call.round === 2,
  );
  assert.equal(designerInput(designerRound2).prior_batch_id, "batch-1");
  assert.equal(round1Refinement.kind, "refine");
  assert.deepEqual(
    designerInput(designerRound2).refinement_reasons,
    round1Refinement.reasons,
  );
  assert.equal(reporterInput(calls).winner_candidate_id, "candidate-2");
});

test("pipeline refines twice then exits with a round 3 winner", async () => {
  const { calls, runners } = createRunners([refine(1), refine(2), winner(3)]);

  await runPipeline(runId, "balanced brief", runners);

  assert.equal(
    designerInput(
      calls.find((call) => call.stage === "designer" && call.round === 3),
    ).prior_batch_id,
    "batch-2",
  );
  assert.equal(reporterInput(calls).winner_candidate_id, "candidate-3");
  assert.equal(reporterInput(calls).round_history.length, 3);
});

test("pipeline refines three times then reports no viable strategy", async () => {
  const { calls, runners } = createRunners([refine(1), refine(2), refine(3)]);

  await runPipeline(runId, "balanced brief", runners);

  assert.equal(reporterInput(calls).winner_candidate_id, null);
  assert.equal(reporterInput(calls).candidate_batch_id, "batch-3");
  assert.equal(reporterInput(calls).round_history.length, 3);
});

test("pipeline short-circuits stage failures to reporter no viable strategy", async () => {
  const { calls, runners } = createRunners([winner(1)]);
  runners.designer = {
    async run(input, _runId, round) {
      calls.push({ stage: "designer", input, round });
      throw new Error("designer failed");
    },
  };

  await runPipeline(runId, "balanced brief", runners);

  assert.deepEqual(
    calls.map((call) => [call.stage, call.round]),
    [
      ["thesis", 1],
      ["designer", 1],
      ["reporter", 1],
    ],
  );
  assert.equal(reporterInput(calls).winner_candidate_id, null);
  assert.equal(reporterInput(calls).candidate_batch_id, "no_viable_strategy");
});

function reporterInput(calls: Array<{ stage: string; input: unknown }>) {
  const call = calls.find((entry) => entry.stage === "reporter");
  assert.ok(call);
  return call.input as Parameters<PipelineRunners["reporter"]["run"]>[0];
}

function designerInput(call?: { input: unknown }) {
  assert.ok(call);
  return call.input as Parameters<PipelineRunners["designer"]["run"]>[0];
}
