import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

test("stage_eval_runs repository inserts and lists a row", async (t) => {
  if (!process.env.DATABASE_URL || !process.env.RUN_STAGE_EVAL_RUNS_SMOKE_TEST) {
    t.skip(
      "Set DATABASE_URL and RUN_STAGE_EVAL_RUNS_SMOKE_TEST=1 to run the stage_eval_runs smoke test.",
    );
    return;
  }

  const { pgPool } = await import("../src/db/client");
  const { createEvalRun, listEvalRuns } = await import(
    "../src/db/repositories/stage-eval-runs"
  );

  const evalRunId = `eval-run-${randomUUID()}`;
  const fixtureId = `fixture-${randomUUID()}`;

  try {
    await createEvalRun({
      diagnostics: { rules: [{ passed: true, rule: "shape" }] },
      durationMs: 42,
      evalRunId,
      fixtureId,
      model: "test-model",
      output: { result: "ok" },
      passed: true,
      score: 1,
      stage: "thesis",
    });

    const evalRuns = await listEvalRuns({ fixtureId, stage: "thesis" });

    assert.equal(evalRuns[0]?.evalRunId, evalRunId);
    assert.equal(evalRuns[0]?.fixtureId, fixtureId);
    assert.equal(evalRuns[0]?.stage, "thesis");
  } finally {
    await pgPool.query("DELETE FROM stage_eval_runs WHERE eval_run_id = $1", [
      evalRunId,
    ]);
    await pgPool.end();
  }
});
