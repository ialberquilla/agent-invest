import assert from "node:assert/strict";
import test from "node:test";

import { parseRecommendWindowOutput } from "../src/tools/recommend-backtest-window.js";

test("parseRecommendWindowOutput returns a valid RecommendWindowOutput", () => {
  const output = parseRecommendWindowOutput(
    JSON.stringify({
      start: "2020-07-01",
      end: "2024-01-01",
      rationale: "Selected a deterministic window.",
      covered_drawdowns: [
        {
          asset: "bitcoin",
          peak_date: "2021-11-01",
          trough_date: "2022-11-01",
          drawdown_pct: -0.4,
        },
      ],
      history_constraints: {
        intersection_start: "2020-07-01",
        intersection_end: "2024-01-01",
        target_window_length_days: 2190,
        window_length_days: 1280,
        limiting_coin: "solana",
        coins: {
          solana: {
            first_price_date: "2020-07-01",
            last_price_date: "2024-01-01",
            price_days: 1281,
          },
        },
      },
    }),
  );

  assert.equal(output.history_constraints.limiting_coin, "solana");
  assert.equal(output.covered_drawdowns[0]?.drawdown_pct, -0.4);
});
