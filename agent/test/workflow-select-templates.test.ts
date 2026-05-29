import assert from "node:assert/strict";
import test from "node:test";

import {
  selectTemplates,
  type SelectTemplatesDeps,
} from "../src/agent/workflow/steps/select_templates.ts";
import type {
  LLMClient,
  LLMResponse,
} from "../src/agent/workflow/llm.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import type {
  TemplateSelection,
  Thesis,
} from "../src/agent/workflow/state.ts";

const THESIS: Thesis = {
  objective: "balanced_growth",
  horizon_days: 365,
  weight_mode: "percentage",
  universe_hints: {
    top_n: 25,
    exclude_stablecoins: true,
    exclude_wrapped: true,
  },
  constraints: {
    max_weight_per_asset: 0.2,
    max_cash_weight: 0.1,
    max_drawdown: 0.35,
    asset_count_min: 5,
    asset_count_max: 10,
  },
  rebalance_frequency: "monthly",
  interpretation_notes: "Balanced large-cap basket, long-only.",
};

const SAMPLE_SELECTION: TemplateSelection = {
  rationale:
    "Balanced-growth, monthly-rebalanced long basket maps to rebalanced allocation families.",
  selected: [
    {
      family: "periodic_rebalanced_allocation",
      rank: 1,
      rationale: "Thesis specifies a monthly rebalance cadence.",
    },
    {
      family: "core_satellite_allocation",
      rank: 2,
      rationale: "Balanced growth with controlled alt upside.",
    },
  ],
};

type RecordingLLM = LLMClient & {
  calls: Array<{ system: string; user: string }>;
};

function fakeLLM(responses: string[]): RecordingLLM {
  const calls: Array<{ system: string; user: string }> = [];
  let i = 0;
  return {
    calls,
    async complete(request) {
      calls.push({ system: request.system, user: request.user });
      const text = responses[i] ?? responses[responses.length - 1] ?? "";
      i += 1;
      return {
        text,
        tokens_in: 100,
        tokens_out: 200,
      } satisfies LLMResponse;
    },
  };
}

function deps(llm: LLMClient): SelectTemplatesDeps {
  return { llm, logger: createSilentStepLogger() };
}

test("selectTemplates returns a valid selection and routes to select_universe", async () => {
  const llm = fakeLLM([JSON.stringify(SAMPLE_SELECTION)]);

  const result = await selectTemplates(
    { run_id: "test-1", thesis: THESIS },
    deps(llm),
  );

  assert.equal(result.next, "select_universe");
  assert.deepEqual(result.delta.template_selection, SAMPLE_SELECTION);
  assert.equal(llm.calls.length, 1);
});

test("selectTemplates sends the catalog and thesis digest in the user message", async () => {
  const llm = fakeLLM([JSON.stringify(SAMPLE_SELECTION)]);

  await selectTemplates({ run_id: "test-msg", thesis: THESIS }, deps(llm));

  const parsed = JSON.parse(llm.calls[0]!.user) as {
    catalog: string[];
    thesis: { objective: string; rebalance_frequency: string };
  };
  assert.ok(parsed.catalog.includes("periodic_rebalanced_allocation"));
  assert.equal(parsed.thesis.objective, "balanced_growth");
  assert.equal(parsed.thesis.rebalance_frequency, "monthly");
});

test("selectTemplates recovers from a fenced JSON response", async () => {
  const llm = fakeLLM([
    "```json\n" + JSON.stringify(SAMPLE_SELECTION) + "\n```",
  ]);

  const result = await selectTemplates(
    { run_id: "test-fenced", thesis: THESIS },
    deps(llm),
  );

  assert.equal(result.delta.template_selection.selected[0]?.family, "periodic_rebalanced_allocation");
  assert.equal(llm.calls.length, 1);
});

test("selectTemplates recovers when JSON is embedded in prose", async () => {
  const llm = fakeLLM([
    `Here is the selection: ${JSON.stringify(SAMPLE_SELECTION)} -- done.`,
  ]);

  const result = await selectTemplates(
    { run_id: "test-embedded", thesis: THESIS },
    deps(llm),
  );

  assert.equal(result.delta.template_selection.selected.length, 2);
});

test("selectTemplates retries once on parse failure then succeeds", async () => {
  const llm = fakeLLM([
    "I'd suggest a rebalanced allocation -- no JSON here.",
    JSON.stringify(SAMPLE_SELECTION),
  ]);

  const result = await selectTemplates(
    { run_id: "test-retry-parse", thesis: THESIS },
    deps(llm),
  );

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /could not be parsed/);
  assert.equal(result.delta.template_selection.selected.length, 2);
});

test("selectTemplates retries with a schema note on validation failure", async () => {
  const bad = {
    rationale: "x",
    selected: [{ family: "not_a_family", rank: 1, rationale: "y" }],
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(SAMPLE_SELECTION)]);

  const result = await selectTemplates(
    { run_id: "test-retry-schema", thesis: THESIS },
    deps(llm),
  );

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /failed validation/);
  assert.match(llm.calls[1]!.system, /family must be one of/);
  assert.equal(result.delta.template_selection.selected[0]?.family, "periodic_rebalanced_allocation");
});

test("selectTemplates throws when both attempts fail validation", async () => {
  const bad = {
    rationale: "x",
    selected: [{ family: "not_a_family", rank: 1, rationale: "y" }],
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () => selectTemplates({ run_id: "test-bad", thesis: THESIS }, deps(llm)),
    /family must be one of/,
  );
  assert.equal(llm.calls.length, 2);
});

test("selectTemplates rejects duplicate ranks", async () => {
  const dup = {
    rationale: "x",
    selected: [
      { family: "synthetic_long_allocation", rank: 1, rationale: "a" },
      { family: "core_satellite_allocation", rank: 1, rationale: "b" },
    ],
  };
  const llm = fakeLLM([JSON.stringify(dup), JSON.stringify(dup)]);

  await assert.rejects(
    () => selectTemplates({ run_id: "test-dup-rank", thesis: THESIS }, deps(llm)),
    /rank 1 is duplicated/,
  );
});

test("selectTemplates rejects non-contiguous ranks", async () => {
  const gap = {
    rationale: "x",
    selected: [
      { family: "synthetic_long_allocation", rank: 1, rationale: "a" },
      { family: "core_satellite_allocation", rank: 3, rationale: "b" },
    ],
  };
  const llm = fakeLLM([JSON.stringify(gap), JSON.stringify(gap)]);

  await assert.rejects(
    () => selectTemplates({ run_id: "test-gap", thesis: THESIS }, deps(llm)),
    /ranks must be contiguous/,
  );
});

test("selectTemplates rejects an empty shortlist", async () => {
  const empty = { rationale: "x", selected: [] };
  const llm = fakeLLM([JSON.stringify(empty), JSON.stringify(empty)]);

  await assert.rejects(
    () => selectTemplates({ run_id: "test-empty", thesis: THESIS }, deps(llm)),
    /between 1 and 3/,
  );
});

test("selectTemplates rejects more than 3 families", async () => {
  const tooMany = {
    rationale: "x",
    selected: [
      { family: "synthetic_long_allocation", rank: 1, rationale: "a" },
      { family: "core_satellite_allocation", rank: 2, rationale: "b" },
      { family: "barbell_allocation", rank: 3, rationale: "c" },
      { family: "periodic_rebalanced_allocation", rank: 4, rationale: "d" },
    ],
  };
  const llm = fakeLLM([JSON.stringify(tooMany), JSON.stringify(tooMany)]);

  await assert.rejects(
    () => selectTemplates({ run_id: "test-too-many", thesis: THESIS }, deps(llm)),
    /between 1 and 3/,
  );
});
