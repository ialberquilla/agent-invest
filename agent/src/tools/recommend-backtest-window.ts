import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { promisify } from "node:util";

import { pythonEnv, pythonExecutable } from "../python-runner.ts";

const execFileAsync = promisify(execFile);

export type RecommendWindowInput = {
  coinIds: string[];
  horizonDays: number;
  requireDrawdownPct?: number;
};

export type RecommendWindowOutput = {
  start: string;
  end: string;
  rationale: string;
  covered_drawdowns: Array<{
    asset: string;
    peak_date: string;
    trough_date: string;
    drawdown_pct: number;
  }>;
  history_constraints: {
    intersection_start: string;
    intersection_end: string;
    target_window_length_days: number;
    window_length_days: number;
    limiting_coin: string;
    coins: Record<
      string,
      {
        first_price_date: string;
        last_price_date: string;
        price_days: number;
      }
    >;
  };
};

export async function recommendBacktestWindow(
  input: RecommendWindowInput,
): Promise<RecommendWindowOutput> {
  if (input.coinIds.length === 0) {
    throw new Error("coinIds must include at least one coin ID");
  }

  const args = [
    "-m",
    "agent_invest_scripts.recommend_backtest_window",
    "--coin-ids",
    input.coinIds.join(","),
    "--horizon-days",
    String(input.horizonDays),
  ];

  if (input.requireDrawdownPct !== undefined) {
    args.push("--require-drawdown-pct", String(input.requireDrawdownPct));
  }

  const scriptsDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../scripts");
  const { stdout } = await execFileAsync(pythonExecutable(scriptsDir), args, {
    cwd: scriptsDir,
    env: pythonEnv(scriptsDir),
  });

  return parseRecommendWindowOutput(stdout);
}

export function parseRecommendWindowOutput(raw: string): RecommendWindowOutput {
  const parsed: unknown = JSON.parse(raw);
  assertRecommendWindowOutput(parsed);
  return parsed;
}

function assertRecommendWindowOutput(value: unknown): asserts value is RecommendWindowOutput {
  if (!isRecord(value)) throw new Error("recommend_backtest_window returned invalid JSON");
  assertString(value.start, "start");
  assertString(value.end, "end");
  assertString(value.rationale, "rationale");
  if (!Array.isArray(value.covered_drawdowns)) {
    throw new Error("covered_drawdowns must be an array");
  }
  for (const [index, drawdown] of value.covered_drawdowns.entries()) {
    if (!isRecord(drawdown)) throw new Error(`covered_drawdowns[${index}] must be an object`);
    assertString(drawdown.asset, `covered_drawdowns[${index}].asset`);
    assertString(drawdown.peak_date, `covered_drawdowns[${index}].peak_date`);
    assertString(drawdown.trough_date, `covered_drawdowns[${index}].trough_date`);
    assertNumber(drawdown.drawdown_pct, `covered_drawdowns[${index}].drawdown_pct`);
  }

  const constraints = value.history_constraints;
  if (!isRecord(constraints)) throw new Error("history_constraints must be an object");
  assertString(constraints.intersection_start, "history_constraints.intersection_start");
  assertString(constraints.intersection_end, "history_constraints.intersection_end");
  assertNumber(
    constraints.target_window_length_days,
    "history_constraints.target_window_length_days",
  );
  assertNumber(constraints.window_length_days, "history_constraints.window_length_days");
  assertString(constraints.limiting_coin, "history_constraints.limiting_coin");
  if (!isRecord(constraints.coins)) throw new Error("history_constraints.coins must be an object");
  for (const [coinId, coin] of Object.entries(constraints.coins)) {
    if (!isRecord(coin)) throw new Error(`history_constraints.coins.${coinId} must be an object`);
    assertString(coin.first_price_date, `history_constraints.coins.${coinId}.first_price_date`);
    assertString(coin.last_price_date, `history_constraints.coins.${coinId}.last_price_date`);
    assertNumber(coin.price_days, `history_constraints.coins.${coinId}.price_days`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string") throw new Error(`${name} must be a string`);
}

function assertNumber(value: unknown, name: string): asserts value is number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${name} must be a finite number`);
  }
}
