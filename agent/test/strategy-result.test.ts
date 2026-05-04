import assert from "node:assert/strict";
import test from "node:test";

import { parseStrategyResultBlock } from "../src/agent/strategy-result";

const validResult = {
  allocation: [{ asset: "Bitcoin", weight: 1 }],
  assumptions: ["Daily rebalancing is feasible"],
  extra: { preserved: true },
  kpis: { sharpe_ratio: 1.2 },
  next_steps: ["Run a longer backtest"],
  reasoning: "Momentum is persistent in this universe.",
  risks: ["Momentum crash"],
  summary: "A simple momentum strategy.",
  title: "Crypto Momentum",
};

test("parseStrategyResultBlock parses a valid strategy_result JSON fence", () => {
  const parsed = parseStrategyResultBlock(
    `Final answer\n\n\`\`\`strategy_result\n${JSON.stringify(validResult)}\n\`\`\``,
  );

  assert.deepEqual(parsed, validResult);
});

test("parseStrategyResultBlock returns null when the block is missing", () => {
  assert.equal(parseStrategyResultBlock("No structured result here."), null);
});

test("parseStrategyResultBlock returns null for invalid JSON", () => {
  assert.equal(
    parseStrategyResultBlock("```strategy_result\n{not valid json}\n```"),
    null,
  );
});

test("parseStrategyResultBlock returns null for invalid field types", () => {
  assert.equal(
    parseStrategyResultBlock(
      `\`\`\`strategy_result\n${JSON.stringify({
        ...validResult,
        allocation: { asset: "Bitcoin", weight: 1 },
      })}\n\`\`\``,
    ),
    null,
  );
});
