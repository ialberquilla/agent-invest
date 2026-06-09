import assert from "node:assert/strict";
import test from "node:test";

import { eq } from "drizzle-orm";

import { investmentTheses, runs } from "../src/db/schema";
import {
  recordInvestmentThesis,
  type RecordThesisInput,
} from "../src/tools/record-investment-thesis";

const validInput: RecordThesisInput = {
  horizon_days: 365,
  interpretation_notes: "Favor durable growth without ignoring downside risk.",
  objective: "balanced",
  primary_factors: [{ direction: "high", factor: "market_cap" }],
  run_id: "run-thesis-1",
};

function createDbDouble(options: { existing?: unknown } = {}) {
  const inserted: unknown[] = [];
  let conflictIgnored = false;

  const db = {
    insert() {
      return {
        values(value: unknown) {
          inserted.push(value);
          return {
            onConflictDoNothing() {
              conflictIgnored = true;
              return {
                async returning() {
                  if (options.existing) return [];
                  return [{ thesisId: "11111111-1111-4111-8111-111111111111" }];
                },
              };
            },
          };
        },
      };
    },
    select() {
      return {
        from() {
          return {
            async where() {
              return options.existing ? [options.existing] : [];
            },
          };
        },
      };
    },
  };

  return {
    db: db as never,
    get conflictIgnored() {
      return conflictIgnored;
    },
    inserted,
  };
}

test("recordInvestmentThesis validates and persists a thesis", async () => {
  const dbDouble = createDbDouble();

  const output = await recordInvestmentThesis(validInput, dbDouble.db);

  assert.equal(output.thesis_id, "11111111-1111-4111-8111-111111111111");
  assert.equal(dbDouble.conflictIgnored, true);
  assert.deepEqual(dbDouble.inserted, [
    {
      objective: "balanced",
      payload: validInput,
      runId: "run-thesis-1",
    },
  ]);
});

test("recordInvestmentThesis returns the existing thesis for a duplicate run", async () => {
  const { db } = createDbDouble({
    existing: { thesisId: "22222222-2222-4222-8222-222222222222" },
  });

  const output = await recordInvestmentThesis(validInput, db);

  assert.equal(output.thesis_id, "22222222-2222-4222-8222-222222222222");
});

test("recordInvestmentThesis rejects an invalid objective before writing", async () => {
  const { db, inserted } = createDbDouble();

  await assert.rejects(
    () =>
      recordInvestmentThesis(
        {
          ...validInput,
          objective: "speculation" as RecordThesisInput["objective"],
        },
        db,
      ),
    /objective must be one of/,
  );
  assert.deepEqual(inserted, []);
});

test("recordInvestmentThesis rejects missing interpretation notes before writing", async () => {
  const { db, inserted } = createDbDouble();

  await assert.rejects(
    () =>
      recordInvestmentThesis({ ...validInput, interpretation_notes: " " }, db),
    /interpretation_notes must be a non-empty string/,
  );
  assert.deepEqual(inserted, []);
});

test("recordInvestmentThesis integration persists and reads back a thesis", async (t) => {
  if (!process.env.DATABASE_URL || !process.env.RUN_DB_INTEGRATION_TESTS) {
    t.skip(
      "Set DATABASE_URL and RUN_DB_INTEGRATION_TESTS=1 to run the Postgres thesis integration test.",
    );
    return;
  }

  const { db, pgPool } = await import("../src/db/client");
  const runId = `run-thesis-${Date.now()}`;

  try {
    await db.insert(runs).values({ runId, status: "running" });

    const output = await recordInvestmentThesis(
      { ...validInput, run_id: runId },
      db,
    );
    const [row] = await db
      .select()
      .from(investmentTheses)
      .where(eq(investmentTheses.thesisId, output.thesis_id));

    assert.ok(output.thesis_id);
    assert.equal(row?.runId, runId);
    assert.equal(row?.objective, "balanced");
    assert.deepEqual(row?.payload, { ...validInput, run_id: runId });
  } finally {
    await db.delete(runs).where(eq(runs.runId, runId));
    await pgPool.end();
  }
});
