// Eval runner for the decide step.
//
// decide is an LLM step: it reads the iteration history (thesis + attempts +
// their validation summaries + counters/caps) and emits exactly ONE Decision
// routing the run to a next step. Its deterministic plumbing -- routing per
// action, the cap/legality validator, and the stop_best_effort fallback when the
// LLM can't emit valid JSON -- is already covered hermetically by
// test/workflow-decide.test.ts with a fake LLM. This eval's distinct job is the
// LIVE JUDGEMENT: given a realistic post-validate state, does the real model
// pick the action that matches the failure pattern, and does its hint address
// the constraint that actually failed? Those are exactly the calls the schema
// validator can't make for us (it only rejects illegal/cap-violating actions).
//
// Groups (named by the decision driver under test):
//   winner       latest attempt has >=1 passing candidate -> stop_winner, and
//                the winner is drawn from the passing set (not a failed one)
//   refine       a single fixable failure with budget left -> refine_candidates
//                whose hint addresses the constraint that actually failed
//   broaden      every candidate fails asset_count_min (universe collapsed
//                below the floor) -> broaden_universe loosening breadth
//   reinterpret  a thesis-level contradiction no candidate/universe edit can
//                fix, with refine futile + broaden exhausted -> reinterpret_brief
//   caps         every backward edge is at its cap and the latest failed ->
//                only stop_best_effort is legal, so it MUST be chosen (STEP 0)
//   persona      a full multi-attempt history failing the same constraint with
//                no improvement -> stop_best_effort on judgement (refinement memory)
//
// Run all:        pnpm eval:decide
// Run one group:  pnpm eval:decide refine reinterpret

import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import { decide } from "./decide.ts";
import type {
  Attempt,
  AttemptValidationSummary,
  Counters,
  DecideInput,
  Decision,
  Proposal,
  Thesis,
} from "../state.ts";

type Expect = {
  action: Decision["action"];
  // stop_winner: the chosen winner must be one of these (a passing candidate).
  winner_in?: string[];
  // refine_candidates: this constraint must appear in hint.failed_constraints.
  refine_addresses?: string;
  // refine_candidates: hint.suggested_changes must include at least one of these
  // keys (a coherent fix for the failure -- any of them is legitimate).
  refine_change_any_of?: string[];
  // broaden_universe: hint.loosen must include at least one of these keys.
  broaden_loosen_any_of?: string[];
  // reinterpret_brief: hint.fields_to_revisit must include this field.
  reinterpret_field?: string;
  // reinterpret_brief: hint.reason must be one of these.
  reinterpret_reason_any_of?: string[];
};

type EvalCase = {
  group: string;
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
  iteration_hypothesis: "Equal-weight 5-asset basket plus a rebalanced control.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "static equal-weight baseline",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalanced_allocation",
      select_top: 7,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "monthly rebalanced cap-weighted",
    },
  ],
};

// A 3-candidate concentrated proposal: select_top too small, so the per-asset
// weight necessarily exceeds the cap -- a failure fixable by refining (widen
// the basket / tighten the cap), not by broadening the universe.
const CONCENTRATED: Proposal = {
  iteration_hypothesis: "Tight 3-name basket.",
  candidates: [
    { candidate_id: "c1", template_id: "synthetic_long_allocation", select_top: 3, weighting: "equal", rationale: "3-name equal" },
    { candidate_id: "c2", template_id: "synthetic_long_allocation", select_top: 3, weighting: "cap", rationale: "3-name cap" },
    { candidate_id: "c3", template_id: "periodic_rebalanced_allocation", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_30d", rationale: "3-name rebalanced" },
  ],
};

// A proposal forced down to 4 names because the filtered universe only yielded
// 4 -- every candidate then trips asset_count_min=5.
const NARROW: Proposal = {
  iteration_hypothesis: "Best we can do at universe size 4.",
  candidates: [
    { candidate_id: "c1", template_id: "synthetic_long_allocation", select_top: 4, weighting: "equal", rationale: "size-4 equal" },
    { candidate_id: "c2", template_id: "synthetic_long_allocation", select_top: 4, weighting: "cap", rationale: "size-4 cap" },
    { candidate_id: "c3", template_id: "periodic_rebalanced_allocation", select_top: 4, weighting: "equal", rebalance_trigger: "periodic_30d", rationale: "size-4 rebalanced" },
  ],
};

const ZERO: Counters = { reinterpret_brief: 0, broaden_universe: 0 };

// --- attempt builders --------------------------------------------------------

function passAttempt(
  attempt_n: number,
  passing: string[],
  failing: AttemptValidationSummary["failing"] = [],
  proposal: Proposal = PROPOSAL,
): Attempt {
  return {
    attempt_n,
    proposal,
    batch_id: `batch_${attempt_n}`,
    validation_summary: {
      passing_candidate_ids: passing,
      failing,
      candidates: [
        ...passing.map((id) => ({
          candidate_id: id,
          passed: true,
          constraint_distance: 0,
        })),
        ...failing.map((f) => ({
          candidate_id: f.candidate_id,
          passed: false,
          constraint_distance: 1,
        })),
      ],
    },
  };
}

function failAttempt(
  attempt_n: number,
  failing: Array<{ candidate_id: string; constraint: string; observed: number; target: number }>,
  proposal: Proposal = PROPOSAL,
  refinement_hint?: Attempt["refinement_hint"],
): Attempt {
  return {
    attempt_n,
    proposal,
    batch_id: `batch_${attempt_n}`,
    validation_summary: {
      passing_candidate_ids: [],
      failing: failing.map((f) => ({
        candidate_id: f.candidate_id,
        violations: [{ constraint: f.constraint, observed: f.observed, target: f.target }],
      })),
      candidates: failing.map((f) => ({
        candidate_id: f.candidate_id,
        passed: false,
        constraint_distance:
          Math.abs(f.observed - f.target) / Math.max(Math.abs(f.target), 1),
      })),
    },
    refinement_hint,
  };
}

const CASES: EvalCase[] = [
  // --- winner: a passing candidate exists -> stop, pick from the passing set --
  {
    group: "winner",
    name: "winner=single_pass_despite_a_failure",
    // c2 passes, c1 fails its weight cap. The right move is to STOP on the
    // winner, not get distracted into another refine round.
    input: {
      run_id: "eval-decide-winner-single",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        passAttempt(1, ["c2"], [
          { candidate_id: "c1", violations: [{ constraint: "max_weight_per_asset", observed: 0.54, target: 0.2 }] },
        ]),
      ],
    },
    expect: { action: "stop_winner", winner_in: ["c2"] },
  },
  {
    group: "winner",
    name: "winner=multi_pass_picks_a_passing_id",
    // Both pass: any winner from the passing set is correct (no metrics in the
    // summary let the model rank them, so we assert membership, not identity).
    input: {
      run_id: "eval-decide-winner-multi",
      thesis: thesis(),
      counters: ZERO,
      attempts: [passAttempt(1, ["c1", "c2"])],
    },
    expect: { action: "stop_winner", winner_in: ["c1", "c2"] },
  },

  // --- refine: a single fixable failure with budget remaining ----------------
  {
    group: "refine",
    name: "refine=drawdown_too_high",
    input: {
      run_id: "eval-decide-refine-dd",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        failAttempt(1, [
          { candidate_id: "c1", constraint: "max_drawdown", observed: -0.46, target: -0.35 },
          { candidate_id: "c2", constraint: "max_drawdown", observed: -0.45, target: -0.35 },
        ]),
      ],
    },
    expect: {
      action: "refine_candidates",
      refine_addresses: "max_drawdown",
      // drawdown is fixable by tightening concentration, adding cash, or moving
      // to a drawdown-aware template -- any of these is a coherent hint.
      refine_change_any_of: ["tighten_weight_cap_to", "increase_cash_to", "change_template_to", "change_rebalance_to"],
    },
  },
  {
    group: "refine",
    name: "refine=too_concentrated",
    // Universe is fine; the basket is just too small, so per-asset weight blows
    // the cap. Fixable by a different candidate config (wider select_top /
    // tighter cap), NOT by broadening the universe or rewriting the brief.
    input: {
      run_id: "eval-decide-refine-conc",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        failAttempt(
          1,
          [
            { candidate_id: "c1", constraint: "max_weight_per_asset", observed: 0.33, target: 0.2 },
            { candidate_id: "c2", constraint: "max_weight_per_asset", observed: 0.33, target: 0.2 },
            { candidate_id: "c3", constraint: "max_weight_per_asset", observed: 0.33, target: 0.2 },
          ],
          CONCENTRATED,
        ),
      ],
    },
    expect: {
      action: "refine_candidates",
      refine_addresses: "max_weight_per_asset",
      refine_change_any_of: ["tighten_weight_cap_to", "change_template_to", "swap_assets"],
    },
  },

  // --- broaden: the universe collapsed below the floor -----------------------
  {
    group: "broaden",
    name: "broaden=universe_too_small",
    input: {
      run_id: "eval-decide-broaden",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        failAttempt(
          1,
          [
            { candidate_id: "c1", constraint: "asset_count_min", observed: 4, target: 5 },
            { candidate_id: "c2", constraint: "asset_count_min", observed: 4, target: 5 },
            { candidate_id: "c3", constraint: "asset_count_min", observed: 4, target: 5 },
          ],
          NARROW,
        ),
      ],
    },
    expect: {
      action: "broaden_universe",
      broaden_loosen_any_of: ["raise_top_n_to", "lower_market_cap_floor_to", "drop_filter", "add_sectors"],
    },
  },

  // --- reinterpret: a thesis-level contradiction no edit downstream can fix ---
  {
    // max_cash_weight=0 demands a fully-invested book, but asset_count_max=5 at
    // max_weight_per_asset=0.1 caps deployable capital at 5*0.1=0.5 < 1.0. No
    // candidate can satisfy this (refine is futile) and broadening the universe
    // can't help because asset_count_max is a THESIS cap, not a universe limit
    // (and broaden is already exhausted). The only real fix is to revisit the
    // brief. This is the tightest judgement case in the suite: refine is a trap.
    group: "reinterpret",
    name: "reinterpret=constraints_infeasible",
    input: {
      run_id: "eval-decide-reinterpret",
      thesis: thesis({
        interpretation_notes:
          "Fully invested (no cash) but at most 5 names, each capped at 10%.",
        constraints: {
          max_weight_per_asset: 0.1,
          max_cash_weight: 0.0,
          max_drawdown: 0.5,
          asset_count_min: 5,
          asset_count_max: 5,
        },
      }),
      // broaden already spent, so it's off the table; refine still has budget
      // (attempts < max) but cannot resolve a thesis-level contradiction.
      counters: { reinterpret_brief: 0, broaden_universe: 1 },
      attempts: [
        failAttempt(1, [
          { candidate_id: "c1", constraint: "max_weight_per_asset", observed: 0.2, target: 0.1 },
          { candidate_id: "c2", constraint: "max_weight_per_asset", observed: 0.2, target: 0.1 },
        ]),
        failAttempt(
          2,
          [
            { candidate_id: "c1", constraint: "max_weight_per_asset", observed: 0.2, target: 0.1 },
            { candidate_id: "c2", constraint: "max_weight_per_asset", observed: 0.2, target: 0.1 },
          ],
          PROPOSAL,
          {
            failed_constraints: [
              { constraint: "max_weight_per_asset", observed: 0.2, target: 0.1, candidate_id: "c1" },
            ],
            suggested_changes: { tighten_weight_cap_to: 0.1 },
            rationale: "Tried to honour the 10% cap; impossible with only 5 fully-invested names.",
          },
        ),
      ],
    },
    expect: {
      action: "reinterpret_brief",
      reinterpret_field: "constraints",
      reinterpret_reason_any_of: ["constraints_infeasible", "objective_mismatch"],
    },
  },

  // --- caps: every backward edge exhausted -> only stop_best_effort is legal -
  {
    // STEP 0 hard gate: attempts at max_attempts, both counters at their caps,
    // latest attempt failed. Every action but stop_best_effort is FORBIDDEN, so
    // the model must pick it regardless of how "fixable" the failure looks.
    group: "caps",
    name: "caps=all_exhausted_forces_best_effort",
    input: {
      run_id: "eval-decide-caps",
      thesis: thesis(),
      counters: { reinterpret_brief: 1, broaden_universe: 1 },
      attempts: [
        failAttempt(1, [{ candidate_id: "c1", constraint: "max_drawdown", observed: -0.5, target: -0.35 }]),
        failAttempt(2, [{ candidate_id: "c1", constraint: "max_drawdown", observed: -0.49, target: -0.35 }]),
        failAttempt(3, [{ candidate_id: "c1", constraint: "max_drawdown", observed: -0.48, target: -0.35 }]),
      ],
    },
    expect: { action: "stop_best_effort" },
  },

  // --- persona: full history, same constraint, no improvement ----------------
  {
    // Three attempts, each refining against the same max_drawdown failure with
    // no material improvement (-0.55 -> -0.54 -> -0.53). attempts==max_attempts
    // so refine is also forbidden, but the judgement signal is the flat
    // no-improvement trajectory across the carried refinement_hints.
    group: "persona",
    name: "persona=repeated_no_improvement",
    input: {
      run_id: "eval-decide-persona",
      thesis: thesis(),
      counters: ZERO,
      attempts: [
        failAttempt(1, [{ candidate_id: "c1", constraint: "max_drawdown", observed: -0.55, target: -0.35 }], PROPOSAL, {
          failed_constraints: [{ constraint: "max_drawdown", observed: -0.55, target: -0.35, candidate_id: "c1" }],
          suggested_changes: { tighten_weight_cap_to: 0.15 },
          rationale: "Tighten concentration to limit drawdown.",
        }),
        failAttempt(2, [{ candidate_id: "c1", constraint: "max_drawdown", observed: -0.54, target: -0.35 }], PROPOSAL, {
          failed_constraints: [{ constraint: "max_drawdown", observed: -0.54, target: -0.35, candidate_id: "c1" }],
          suggested_changes: { increase_cash_to: 0.2 },
          rationale: "Add cash to dampen drawdown further.",
        }),
        failAttempt(3, [{ candidate_id: "c1", constraint: "max_drawdown", observed: -0.53, target: -0.35 }]),
      ],
    },
    expect: { action: "stop_best_effort" },
  },
];

// --- assertions --------------------------------------------------------------

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

function evaluate(expect: Expect, decision: Decision): Assertion[] {
  const a: Assertion[] = [];
  const push = (label: string, expected: unknown, actual: unknown, ok: boolean) =>
    a.push({ label, expected: String(expected), actual: String(actual), ok });

  push("action", expect.action, decision.action, decision.action === expect.action);

  // Sub-assertions only apply when the action matched (the discriminated union
  // narrows here); a wrong action is already captured by the assertion above.
  if (decision.action === "stop_winner" && expect.winner_in) {
    const ok = expect.winner_in.includes(decision.winner_candidate_id);
    push("winner_in", `one of [${expect.winner_in.join(",")}]`, decision.winner_candidate_id, ok);
  }

  if (decision.action === "refine_candidates") {
    if (expect.refine_addresses) {
      const seen = decision.hint.failed_constraints.map((fc) => String(fc.constraint));
      push("refine_addresses", expect.refine_addresses, `[${seen.join(",")}]`, seen.includes(expect.refine_addresses));
    }
    if (expect.refine_change_any_of) {
      const keys = Object.keys(decision.hint.suggested_changes);
      const ok = keys.some((k) => expect.refine_change_any_of!.includes(k));
      push("refine_change_any_of", `any of [${expect.refine_change_any_of.join(",")}]`, `[${keys.join(",")}]`, ok);
    }
  }

  if (decision.action === "broaden_universe" && expect.broaden_loosen_any_of) {
    const keys = Object.keys(decision.hint.loosen);
    const ok = keys.some((k) => expect.broaden_loosen_any_of!.includes(k));
    push("broaden_loosen_any_of", `any of [${expect.broaden_loosen_any_of.join(",")}]`, `[${keys.join(",")}]`, ok);
  }

  if (decision.action === "reinterpret_brief") {
    if (expect.reinterpret_field) {
      const fields = decision.hint.fields_to_revisit.map(String);
      push("reinterpret_field", expect.reinterpret_field, `[${fields.join(",")}]`, fields.includes(expect.reinterpret_field));
    }
    if (expect.reinterpret_reason_any_of) {
      const ok = expect.reinterpret_reason_any_of.includes(decision.hint.reason);
      push("reinterpret_reason_any_of", `any of [${expect.reinterpret_reason_any_of.join(",")}]`, decision.hint.reason, ok);
    }
  }

  return a;
}

// --- a compact, uniform view of the input state per case ---------------------

function describeInput(input: DecideInput): string {
  const latest = input.attempts.at(-1)!;
  const summary = latest.validation_summary!;
  const failed = [
    ...new Set(summary.failing.flatMap((f) => f.violations.map((v) => v.constraint))),
  ];
  return (
    `attempts=${input.attempts.length} latest.passing=${JSON.stringify(summary.passing_candidate_ids)} ` +
    `latest.failed_constraints=[${failed.join(",")}]\n` +
    `counters=${JSON.stringify(input.counters)} constraints=${JSON.stringify(input.thesis.constraints)}`
  );
}

type CaseOutcome = {
  name: string;
  group: string;
  status: "pass" | "fail";
  duration_ms: number;
  failed: string[];
};

async function main(): Promise<number> {
  const groupFilter = new Set(process.argv.slice(2));
  const cases = CASES.filter((c) => groupFilter.size === 0 || groupFilter.has(c.group));
  if (groupFilter.size > 0) {
    process.stdout.write(`group filter: ${[...groupFilter].join(", ")}\n`);
  }
  process.stdout.write(`running ${cases.length} cases\n`);

  const llm = createOpencodeLLMClient({ sessionTitle: "eval-decide" });
  const outcomes: CaseOutcome[] = [];

  for (const c of cases) {
    process.stdout.write(`\n${"=".repeat(72)}\n[${c.name}]\n`);
    process.stdout.write(`--- input ---\n${describeInput(c.input)}\n`);

    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/llm/exit lines alongside the eval's assertions.
      const result = await decide(c.input, { llm });
      const duration_ms = Date.now() - start;
      const decision = result.delta.decision;

      process.stdout.write(`--- output ---\nnext=${result.next} decision=${JSON.stringify(decision)}\n`);

      const assertions = evaluate(c.expect, decision);
      process.stdout.write(`--- assertions ---\n`);
      for (const x of assertions) {
        process.stdout.write(`  ${x.ok ? "PASS" : "FAIL"} ${x.label}: expected ${x.expected}, got ${x.actual}\n`);
      }
      const failed = assertions.filter((x) => !x.ok).map((x) => x.label);
      const status: "pass" | "fail" = failed.length === 0 ? "pass" : "fail";
      process.stdout.write(`[${c.name}] ${status.toUpperCase()} (${duration_ms} ms)\n`);
      outcomes.push({ name: c.name, group: c.group, status, duration_ms, failed });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind = error instanceof Error ? error.constructor.name : "Error";
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms, ${kind})\n  ${message}\n`);
      outcomes.push({ name: c.name, group: c.group, status: "fail", duration_ms, failed: [`${kind}: ${message}`] });
    }
  }

  process.stdout.write(`\n${"=".repeat(72)}\n=== summary ===\n`);
  for (const row of outcomes) {
    const reason = row.failed.length > 0 ? ` -- ${row.failed.join("; ")}` : "";
    process.stdout.write(
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(40)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(`\n${outcomes.length - failed}/${outcomes.length} passed\n`);
  return failed > 0 ? 1 : 0;
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

export { CASES, evaluate, main };
