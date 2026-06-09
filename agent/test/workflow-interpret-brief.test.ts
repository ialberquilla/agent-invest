import assert from "node:assert/strict";
import test from "node:test";

import {
  interpretBrief,
  type InterpretBriefDeps,
} from "../src/agent/workflow/steps/interpret_brief.ts";
import type {
  LLMClient,
  LLMResponse,
} from "../src/agent/workflow/llm.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import type { Thesis } from "../src/agent/workflow/state.ts";

const SAMPLE_THESIS: Thesis = {
  objective: "balanced_growth",
  horizon_days: 365,
  weight_mode: "percentage",
  universe_hints: {
    top_n: 25,
    market_cap_min_usd: 1_000_000_000,
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
  interpretation_notes:
    "Balanced large-cap basket. Defaults applied: percentage weights, monthly rebalance.",
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

function deps(llm: LLMClient): InterpretBriefDeps {
  return { llm, logger: createSilentStepLogger() };
}

test("interpretBrief returns valid Thesis and routes to select_templates", async () => {
  const llm = fakeLLM([JSON.stringify(SAMPLE_THESIS)]);

  const result = await interpretBrief(
    { run_id: "test-1", brief: "balanced growth basket over 1 year" },
    deps(llm),
  );

  assert.equal(result.next, "select_templates");
  assert.deepEqual(result.delta.thesis, SAMPLE_THESIS);
  assert.equal(llm.calls.length, 1);
});

test("interpretBrief recovers from a fenced JSON response", async () => {
  const llm = fakeLLM([
    "```json\n" + JSON.stringify(SAMPLE_THESIS) + "\n```",
  ]);

  const result = await interpretBrief(
    { run_id: "test-fenced", brief: "balanced growth" },
    deps(llm),
  );

  assert.equal(result.delta.thesis.objective, "balanced_growth");
  assert.equal(llm.calls.length, 1);
});

test("interpretBrief recovers when JSON is embedded in prose", async () => {
  const llm = fakeLLM([
    `Here is the thesis: ${JSON.stringify(SAMPLE_THESIS)} -- hope this helps.`,
  ]);

  const result = await interpretBrief(
    { run_id: "test-embedded", brief: "balanced" },
    deps(llm),
  );

  assert.equal(result.delta.thesis.objective, "balanced_growth");
});

test("interpretBrief retries once on parse failure then succeeds", async () => {
  const llm = fakeLLM([
    "I'd recommend balanced growth -- no JSON in this response.",
    JSON.stringify(SAMPLE_THESIS),
  ]);

  const result = await interpretBrief(
    { run_id: "test-retry-parse", brief: "balanced" },
    deps(llm),
  );

  assert.equal(llm.calls.length, 2);
  assert.equal(result.delta.thesis.objective, "balanced_growth");
  assert.match(llm.calls[1]!.system, /could not be parsed/);
});

test("interpretBrief retries with schema-violation note on schema failure", async () => {
  const bad = { ...SAMPLE_THESIS, objective: "max_return" };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(SAMPLE_THESIS)]);

  const result = await interpretBrief(
    { run_id: "test-retry-schema", brief: "balanced" },
    deps(llm),
  );

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /failed validation/);
  assert.match(llm.calls[1]!.system, /objective must be one of/);
  assert.equal(result.delta.thesis.objective, "balanced_growth");
});

test("interpretBrief throws when both attempts fail schema validation", async () => {
  const bad = { ...SAMPLE_THESIS, objective: "max_return" };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () =>
      interpretBrief(
        { run_id: "test-bad-objective", brief: "x" },
        deps(llm),
      ),
    /objective must be one of/,
  );
  assert.equal(llm.calls.length, 2);
});

test("interpretBrief surfaces feasibility violation", async () => {
  // 3 assets * 0.2 cap = 0.6 max coverage; with 0 cash we need 1.0 -> infeasible.
  const infeasible: Thesis = {
    ...SAMPLE_THESIS,
    constraints: {
      ...SAMPLE_THESIS.constraints,
      asset_count_min: 3,
      max_cash_weight: 0,
    },
  };
  const llm = fakeLLM([
    JSON.stringify(infeasible),
    JSON.stringify(infeasible),
  ]);

  await assert.rejects(
    () =>
      interpretBrief(
        { run_id: "test-infeasible", brief: "x" },
        deps(llm),
      ),
    /infeasible/,
  );
});

test("interpretBrief accepts a valid top_skip", async () => {
  const withSkip: Thesis = {
    ...SAMPLE_THESIS,
    universe_hints: { ...SAMPLE_THESIS.universe_hints, top_skip: 5 },
  };
  const llm = fakeLLM([JSON.stringify(withSkip)]);

  const result = await interpretBrief(
    { run_id: "test-top-skip", brief: "outside top 5" },
    deps(llm),
  );

  assert.equal(result.delta.thesis.universe_hints.top_skip, 5);
});

test("interpretBrief rejects top_skip >= top_n", async () => {
  const bad: Thesis = {
    ...SAMPLE_THESIS,
    universe_hints: {
      ...SAMPLE_THESIS.universe_hints,
      top_n: 5,
      top_skip: 5,
    },
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () =>
      interpretBrief(
        { run_id: "test-top-skip-overflow", brief: "x" },
        deps(llm),
      ),
    /top_skip.*must be < top_n/,
  );
});

test("interpretBrief surfaces hand-picked count mismatch", async () => {
  const mismatch: Thesis = {
    ...SAMPLE_THESIS,
    universe_hints: {
      ...SAMPLE_THESIS.universe_hints,
      hand_picked_coin_ids: ["bitcoin", "ethereum"],
    },
  };
  const llm = fakeLLM([JSON.stringify(mismatch), JSON.stringify(mismatch)]);

  await assert.rejects(
    () =>
      interpretBrief(
        { run_id: "test-handpicked", brief: "x" },
        deps(llm),
      ),
    /hand_picked_coin_ids length/,
  );
});

test("interpretBrief includes ReinterpretHint in the user message when present", async () => {
  const llm = fakeLLM([JSON.stringify(SAMPLE_THESIS)]);

  await interpretBrief(
    {
      run_id: "test-hint",
      brief: "x",
      hint: {
        reason: "constraints_infeasible",
        fields_to_revisit: ["constraints"],
        rationale: "Previous thesis caps were too tight.",
      },
    },
    deps(llm),
  );

  const parsed = JSON.parse(llm.calls[0]!.user) as {
    brief: unknown;
    reinterpret_hint?: { reason: string };
  };
  assert.equal(parsed.reinterpret_hint?.reason, "constraints_infeasible");
});

test("interpretBrief omits reinterpret_hint when not supplied", async () => {
  const llm = fakeLLM([JSON.stringify(SAMPLE_THESIS)]);

  await interpretBrief(
    { run_id: "test-no-hint", brief: "balanced" },
    deps(llm),
  );

  const parsed = JSON.parse(llm.calls[0]!.user) as Record<string, unknown>;
  assert.equal(parsed.reinterpret_hint, undefined);
  assert.equal(parsed.brief, "balanced");
});
