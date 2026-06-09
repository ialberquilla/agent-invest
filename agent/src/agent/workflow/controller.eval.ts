// End-to-end eval for the workflow controller. Runs the full pipeline
// against the real LLM + real Python CLIs.
//
// Run with:  pnpm eval:workflow
//
// Single fixture by design -- this is the integration check that all
// 7 steps work together. The per-step evals cover variation.

import "../../env.ts";

import { runWorkflow } from "./controller.ts";
import { createOpencodeLLMClient } from "./llm.ts";
import type { Final, FinalWinner } from "./state.ts";

// growth objective -> high_growth benchmark -> BTC HODL (no USDC
// dependency). Using balanced_growth would map to balanced_5050, whose
// USDC price series only starts 2023-09-25 and so collides with the
// recommended 2022-05-25 window. Real workflow runs with
// balanced_growth will hit that same dataset gap until USDC is
// backfilled.
const BRIEF =
  "Build a 3-asset basket using bitcoin, ethereum, and binancecoin only. Use equal weights, monthly rebalance, growth objective over 1 year, treat 70% max drawdown as the hard risk limit, allow 0% cash, with exactly 3 assets.";

type Outcome = {
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  final?: Final;
};

async function main(): Promise<number> {
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-workflow" });
  process.stdout.write(`\n[workflow_e2e] running...\n`);
  const start = Date.now();
  let outcome: Outcome;
  try {
    const state = await runWorkflow("eval-workflow-e2e", BRIEF, { llm });
    const final = state.final;
    if (!final) {
      outcome = {
        status: "fail",
        duration_ms: Date.now() - start,
        violations: ["workflow ended with no final"],
      };
    } else {
      const violations = checkFinal(final);
      outcome = {
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms: Date.now() - start,
        violations,
        final,
      };
      printFinal(final);
    }
  } catch (error) {
    outcome = {
      status: "fail",
      duration_ms: Date.now() - start,
      violations: [
        error instanceof Error
          ? `${error.constructor.name}: ${error.message}`
          : String(error),
      ],
    };
  }

  process.stdout.write(
    `\n[workflow_e2e] ${outcome.status.toUpperCase()} (${outcome.duration_ms} ms)\n`,
  );
  for (const v of outcome.violations) process.stdout.write(`  - ${v}\n`);

  return outcome.status === "pass" ? 0 : 1;
}

function checkFinal(final: Final): string[] {
  const out: string[] = [];
  if (final.kind === "winner") {
    if (!final.winner_candidate_id) out.push("missing winner_candidate_id");
    if (!final.candidate_batch_id) out.push("missing candidate_batch_id");
    if (final.attempts_summary.length === 0) out.push("attempts_summary empty");
    const n = final.narrative;
    if (!n.title) out.push("narrative.title empty");
    if (!n.summary) out.push("narrative.summary empty");
    if (n.risks.length === 0) out.push("narrative.risks empty");
    if (n.next_steps.length === 0) out.push("narrative.next_steps empty");
    const winnerInLatest = final.attempts_summary
      .at(-1)
      ?.validation_summary?.passing_candidate_ids?.includes(
        final.winner_candidate_id,
      );
    if (!winnerInLatest) {
      out.push("winner_candidate_id is not in latest passing_candidate_ids");
    }
  } else {
    if (final.reasons.length === 0) out.push("no_viable reasons empty");
  }
  return out;
}

function printFinal(final: Final): void {
  process.stdout.write(`  kind: ${final.kind}\n`);
  if (final.kind === "winner") {
    const w = final as FinalWinner;
    process.stdout.write(`  winner_candidate_id: ${w.winner_candidate_id}\n`);
    process.stdout.write(`  candidate_batch_id: ${w.candidate_batch_id}\n`);
    process.stdout.write(`  attempts: ${w.attempts_summary.length}\n`);
    process.stdout.write(`  title: ${w.narrative.title}\n`);
    process.stdout.write(`  summary: ${w.narrative.summary}\n`);
  } else {
    process.stdout.write(
      `  reasons:\n${final.reasons.map((r) => `    - ${r}`).join("\n")}\n`,
    );
    process.stdout.write(`  attempts: ${final.attempts_summary.length}\n`);
  }
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

export { main };
