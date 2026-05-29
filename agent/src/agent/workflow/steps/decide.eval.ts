// Eval runner for the decide step. Each fixture mirrors a realistic
// post-validate state and asserts the LLM picks the right action.
//
// Run with:  pnpm eval:decide

import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import { decide } from "./decide.ts";
import type {
  Attempt,
  Counters,
  DecideInput,
  Decision,
  Proposal,
  Thesis,
} from "../state.ts";

type Expect = {
  action: Decision["action"];
  // For refine_candidates: at least one of these constraints should appear in failed_constraints.
  refine_must_address_constraint?: string;
  refine_must_change_key?: string;
  broaden_must_loosen_key?: string;
  reinterpret_must_revisit_field?: string;
  winner_candidate_id?: string;
};

type EvalCase = {
  name: string;
  input: DecideInput;
  expect: Expect;
};

function thesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "balanced_growth",
    horizon_days: 365,
    weight_mode: "percentage",
    universe_hints: {
      top_n: 10,
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
    interpretation_notes: "eval fixture",
    ...overrides,
  };
}

const PROPOSAL: Proposal = {
  iteration_hypothesis: "Equal-weight 5-asset basket.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "buy_and_hold",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalance",
      select_top: 7,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "rebalanced cap-weighted",
    },
  ],
};

const ZERO: Counters = { reinterpret_brief: 0, broaden_universe: 0 };

const CASES: EvalCase[] = [
  {
    name: "latest_passes_should_stop_winner",
    input: {
      run_id: "eval-decide-stop-winner",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        {
          attempt_n: 1,
          proposal: PROPOSAL,
          batch_id: "batch_winner",
          validation_summary: {
            passing_candidate_ids: ["c2"],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  { constraint: "max_weight_per_asset", observed: 0.54, target: 0.2 },
                ],
              },
            ],
          },
        },
      ],
    },
    expect: { action: "stop_winner", winner_candidate_id: "c2" },
  },
  {
    name: "drawdown_failure_should_refine",
    input: {
      run_id: "eval-decide-refine-drawdown",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        {
          attempt_n: 1,
          proposal: PROPOSAL,
          batch_id: "batch_dd",
          validation_summary: {
            passing_candidate_ids: [],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  { constraint: "max_drawdown", observed: -0.46, target: -0.35 },
                ],
              },
              {
                candidate_id: "c2",
                violations: [
                  { constraint: "max_drawdown", observed: -0.45, target: -0.35 },
                ],
              },
            ],
          },
        },
      ],
    },
    expect: {
      action: "refine_candidates",
      refine_must_address_constraint: "max_drawdown",
    },
  },
  {
    name: "two_consecutive_no_improvement_should_stop_no_viable",
    input: {
      run_id: "eval-decide-no-viable",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        {
          attempt_n: 1,
          proposal: PROPOSAL,
          batch_id: "batch1",
          validation_summary: {
            passing_candidate_ids: [],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  { constraint: "max_drawdown", observed: -0.55, target: -0.35 },
                ],
              },
            ],
          },
          refinement_hint: {
            failed_constraints: [
              {
                constraint: "max_drawdown",
                observed: -0.55,
                target: -0.35,
                candidate_id: "c1",
              },
            ],
            suggested_changes: { tighten_weight_cap_to: 0.15 },
            rationale: "Tighten concentration to limit drawdown.",
          },
        },
        {
          attempt_n: 2,
          proposal: PROPOSAL,
          batch_id: "batch2",
          validation_summary: {
            passing_candidate_ids: [],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  { constraint: "max_drawdown", observed: -0.54, target: -0.35 },
                ],
              },
            ],
          },
          refinement_hint: {
            failed_constraints: [
              {
                constraint: "max_drawdown",
                observed: -0.54,
                target: -0.35,
                candidate_id: "c1",
              },
            ],
            suggested_changes: { increase_cash_to: 0.2 },
            rationale: "Increase cash to dampen drawdown further.",
          },
        },
        {
          attempt_n: 3,
          proposal: PROPOSAL,
          batch_id: "batch3",
          validation_summary: {
            passing_candidate_ids: [],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  { constraint: "max_drawdown", observed: -0.53, target: -0.35 },
                ],
              },
            ],
          },
        },
      ],
    },
    expect: { action: "stop_no_viable" },
  },
  {
    name: "narrow_universe_should_broaden",
    input: {
      run_id: "eval-decide-broaden",
      thesis: thesis({
        constraints: {
          max_weight_per_asset: 0.2,
          max_cash_weight: 0.1,
          max_drawdown: 0.35,
          asset_count_min: 5,
          asset_count_max: 10,
        },
      }),
      counters: ZERO,
      attempts: [
        {
          attempt_n: 1,
          proposal: {
            iteration_hypothesis: "Try the small viable basket.",
            candidates: [
              {
                candidate_id: "c1",
                template_id: "buy_and_hold",
                select_top: 4,
                weighting: "equal",
                rationale: "best we can do at universe size 4",
              },
              {
                candidate_id: "c2",
                template_id: "buy_and_hold",
                select_top: 4,
                weighting: "cap",
                rationale: "variant",
              },
              {
                candidate_id: "c3",
                template_id: "periodic_rebalance",
                select_top: 4,
                weighting: "equal",
                rebalance_trigger: "periodic_30d",
                rationale: "rebalanced variant",
              },
            ],
          },
          batch_id: "batch_narrow",
          validation_summary: {
            passing_candidate_ids: [],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  { constraint: "asset_count_min", observed: 4, target: 5 },
                ],
              },
              {
                candidate_id: "c2",
                violations: [
                  { constraint: "asset_count_min", observed: 4, target: 5 },
                ],
              },
              {
                candidate_id: "c3",
                violations: [
                  { constraint: "asset_count_min", observed: 4, target: 5 },
                ],
              },
            ],
          },
        },
      ],
    },
    expect: {
      action: "broaden_universe",
      broaden_must_loosen_key: "raise_top_n_to",
    },
  },
];

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  decision?: Decision;
};

async function main(): Promise<number> {
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-decide" });
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await decide(c.input, { llm });
      const violations = checkExpect(c.expect, result.delta.decision);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  decision: ${JSON.stringify(result.delta.decision)}\n`,
      );
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        decision: result.delta.decision,
      });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind = error instanceof Error ? error.constructor.name : "Error";
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(
        `[${c.name}] FAIL (${duration_ms} ms, ${kind})\n  ${message}\n`,
      );
      outcomes.push({
        name: c.name,
        status: "fail",
        duration_ms,
        violations: [`${kind}: ${message}`],
      });
    }
  }

  process.stdout.write("\n=== summary ===\n");
  for (const row of outcomes) {
    const reason =
      row.violations.length > 0 ? ` -- ${row.violations.join("; ")}` : "";
    process.stdout.write(
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(46)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(
    `\n${outcomes.length - failed}/${outcomes.length} passed\n`,
  );
  return failed > 0 ? 1 : 0;
}

function checkExpect(expect: Expect, decision: Decision): string[] {
  const out: string[] = [];
  if (decision.action !== expect.action) {
    out.push(`expected action=${expect.action}, got ${decision.action}`);
    return out;
  }
  if (decision.action === "stop_winner" && expect.winner_candidate_id) {
    if (decision.winner_candidate_id !== expect.winner_candidate_id) {
      out.push(
        `expected winner_candidate_id=${expect.winner_candidate_id}, got ${decision.winner_candidate_id}`,
      );
    }
  }
  if (decision.action === "refine_candidates") {
    if (expect.refine_must_address_constraint) {
      const matched = decision.hint.failed_constraints.some(
        (fc) => fc.constraint === expect.refine_must_address_constraint,
      );
      if (!matched) {
        out.push(
          `expected failed_constraints to include ${expect.refine_must_address_constraint}`,
        );
      }
    }
    if (expect.refine_must_change_key) {
      const keys = Object.keys(decision.hint.suggested_changes);
      if (!keys.includes(expect.refine_must_change_key)) {
        out.push(
          `expected suggested_changes to include key ${expect.refine_must_change_key} (got ${keys.join(", ")})`,
        );
      }
    }
  }
  if (decision.action === "broaden_universe" && expect.broaden_must_loosen_key) {
    const keys = Object.keys(decision.hint.loosen);
    if (!keys.includes(expect.broaden_must_loosen_key)) {
      out.push(
        `expected loosen to include key ${expect.broaden_must_loosen_key} (got ${keys.join(", ")})`,
      );
    }
  }
  if (decision.action === "reinterpret_brief" && expect.reinterpret_must_revisit_field) {
    if (!decision.hint.fields_to_revisit.includes(expect.reinterpret_must_revisit_field as keyof Thesis)) {
      out.push(
        `expected fields_to_revisit to include ${expect.reinterpret_must_revisit_field}`,
      );
    }
  }
  return out;
}

const entry = process.argv[1] ? `file://${process.argv[1]}` : undefined;
if (entry === import.meta.url) {
  main()
    .then((code) => {
      process.exit(code);
    })
    .catch((error: unknown) => {
      process.stderr.write(
        `fatal: ${error instanceof Error ? error.message : String(error)}\n`,
      );
      process.exit(2);
    });
}

// satisfy unused-import lint by referencing Attempt at module scope
export type _UnusedAttempt = Attempt;

export { CASES, checkExpect, main };
