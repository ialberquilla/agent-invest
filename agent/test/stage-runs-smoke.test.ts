import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

test("stage_runs repository inserts and reads a row", async (t) => {
  if (!process.env.DATABASE_URL || !process.env.RUN_STAGE_RUNS_SMOKE_TEST) {
    t.skip(
      "Set DATABASE_URL and RUN_STAGE_RUNS_SMOKE_TEST=1 to run the stage_runs smoke test.",
    );
    return;
  }

  const { db, pgPool } = await import("../src/db/client");
  const { createStageRun, getStageRun } =
    await import("../src/db/repositories/stage-runs");
  const { runs } = await import("../src/db/schema");

  const runId = `run-${randomUUID()}`;
  const stageRunId = `stage-run-${randomUUID()}`;

  try {
    await db.insert(runs).values({ runId, status: "running" });
    await createStageRun({
      input: { objective: "smoke" },
      model: "test-model",
      round: 1,
      runId,
      stage: "thesis",
      stageRunId,
      status: "running",
    });

    const stageRun = await getStageRun(stageRunId);

    assert.equal(stageRun?.stageRunId, stageRunId);
    assert.equal(stageRun?.runId, runId);
    assert.equal(stageRun?.stage, "thesis");
  } finally {
    await pgPool.query("DELETE FROM runs WHERE run_id = $1", [runId]);
    await pgPool.end();
  }
});
