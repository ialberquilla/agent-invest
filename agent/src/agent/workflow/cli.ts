// Thin wrappers around agent_invest_scripts CLIs. Each wrapper is
// injectable (accepts an `execFile` option) so steps can be unit-tested
// with a fake subprocess runner. Mirrors agent/src/agent/gate.ts.

import {
  execFile,
  spawn,
  type ExecFileException,
} from "node:child_process";

import { scriptEnv, scriptsDirectory, uvCommand } from "../../scripts-runtime.ts";

type ExecFileCallback = (
  error: ExecFileException | null,
  stdout: string,
  stderr: string,
) => void;

export type ExecFileLike = (
  file: string,
  args: string[],
  options: {
    cwd: string;
    env: NodeJS.ProcessEnv;
    maxBuffer?: number;
  },
  callback: ExecFileCallback,
) => void;

// run_candidate_batch backtest output (per-day equity curves, weights,
// trades) can easily exceed Node's default 1 MB execFile buffer.
const SCRIPT_STDOUT_BUFFER_BYTES = 64 * 1024 * 1024;

export type RankUniverseRequest = {
  top_n: number;
  exclude_stablecoins?: boolean;
  exclude_wrapped?: boolean;
  min_market_cap?: number;
  // Filter coins to those with at least this many days of price
  // history. Used so the universe stage prunes short-history coins
  // BEFORE select_window has to clamp the common-history window
  // around them.
  min_history_days?: number;
  risk_profile?:
    | "preserve"
    | "balanced"
    | "aggressive"
    | "max_upside"
    | "high_growth"
    | "preserve_capital"
    | "income";
  as_of?: string;
};

// One ranked asset row from rank_universe. `coin_id` is the field we
// care about; everything else is opaque to callers that just want the
// set of selected coins.
export type RankUniverseRow = {
  coin_id: string;
  rank: number;
  market_cap_rank: number | null;
  symbol?: string;
  name?: string;
  [key: string]: unknown;
};

const scriptsDir = scriptsDirectory(import.meta.url, "../../../scripts");

export type RecommendWindowRequest = {
  coin_ids: string[];
  horizon_days: number;
  require_drawdown_pct?: number;
  // Keep the window inside this objective's benchmark data (the recommender
  // folds the benchmark coins into the common-history intersection).
  benchmark_objective?: "high_growth" | "balanced" | "preserve_capital" | "income";
};

export type RecommendWindowResponse = {
  start: string;
  end: string;
  rationale: string;
  covered_drawdowns?: unknown[];
  history_constraints?: {
    intersection_start: string;
    intersection_end: string;
    target_window_length_days: number;
    window_length_days: number;
    limiting_coin?: string;
    coins?: Record<string, unknown>;
  };
};

export async function runRecommendBacktestWindow(
  request: RecommendWindowRequest,
  options: { execFile?: ExecFileLike } = {},
): Promise<RecommendWindowResponse> {
  const args: string[] = [
    "--coin-ids",
    request.coin_ids.join(","),
    "--horizon-days",
    String(request.horizon_days),
  ];
  if (request.require_drawdown_pct !== undefined) {
    args.push("--require-drawdown-pct", String(request.require_drawdown_pct));
  }
  if (request.benchmark_objective !== undefined) {
    args.push("--benchmark-objective", request.benchmark_objective);
  }
  const stdout = await runScript(
    "recommend_backtest_window",
    args,
    options.execFile ?? execFile,
  );
  const parsed = JSON.parse(stdout);
  if (parsed && typeof parsed === "object" && "error" in parsed) {
    const err = (parsed as { error: { message?: string; type?: string } }).error;
    throw new Error(
      `recommend_backtest_window failed: ${err.type ?? "Error"} ${err.message ?? ""}`.trim(),
    );
  }
  return parsed as RecommendWindowResponse;
}

// run_candidate_batch payload. We send a hand-picked universe and an
// explicit start/end window because the workflow's prior steps have
// already resolved those -- the Python script's selector machinery
// would otherwise pick a different set.
export type RunCandidateBatchRequest = {
  run_id: string;
  round: number;
  iteration_hypothesis: string;
  universe_override: {
    id: "hand_picked";
    params: { coin_ids: string[] };
  };
  window_override: { start: string; end: string };
  candidates: Array<{
    candidate_id: string;
    template_id: string;
    select_top: number;
    config: Record<string, unknown>;
  }>;
};

export type RunCandidateBatchResponse = {
  batch_id: string;
  run_id: string;
  round: number;
  results?: unknown[];
};

export async function runCandidateBatch(
  request: RunCandidateBatchRequest,
  options: { execFile?: ExecFileLike; timeoutSeconds?: number } = {},
): Promise<RunCandidateBatchResponse> {
  const args: string[] = ["--input", JSON.stringify(request)];
  if (options.timeoutSeconds !== undefined) {
    args.push("--timeout-seconds", String(options.timeoutSeconds));
  }
  const stdout = await runScript(
    "run_candidate_batch",
    args,
    options.execFile ?? execFile,
  );
  const parsed = JSON.parse(stdout) as Record<string, unknown>;
  if (parsed && typeof parsed === "object" && "error" in parsed) {
    const err = parsed.error as { message?: string; type?: string };
    throw new Error(
      `run_candidate_batch failed: ${err.type ?? "Error"} ${err.message ?? ""}`.trim(),
    );
  }
  if (typeof parsed.batch_id !== "string") {
    throw new Error("run_candidate_batch did not return a batch_id");
  }
  return parsed as unknown as RunCandidateBatchResponse;
}

// Phase 3 of plans/integrate_contracts.md: a StrategyMandate projection the
// live allocator runs to get the strategy's current target weights.
export type LiveAllocationRequest = {
  template_id: string;
  select_top: number;
  config: Record<string, unknown>;
  coin_ids: string[];
  objective: "high_growth" | "balanced" | "preserve_capital" | "income";
  horizon_days?: number;
  // Null/omitted => latest available data ("today"). Set for backfill/parity.
  as_of?: string;
  window?: { start: string; end: string };
};

export type LiveAllocationLeg = {
  coin_id: string;
  weight: number;
  side: "long" | "short";
};

export type LiveAllocationResponse = {
  as_of: string | null;
  rebalance_date: string;
  weights: LiveAllocationLeg[];
  net_weight: number;
  gross_weight: number;
  cash_weight: number;
  template_id: string;
  coin_ids: string[];
};

export async function computeLiveAllocation(
  request: LiveAllocationRequest,
  options: { execFile?: ExecFileLike; timeoutSeconds?: number } = {},
): Promise<LiveAllocationResponse> {
  const args: string[] = ["--input", JSON.stringify(request)];
  if (options.timeoutSeconds !== undefined) {
    args.push("--timeout-seconds", String(options.timeoutSeconds));
  }
  const stdout = await runScript(
    "compute_live_allocation",
    args,
    options.execFile ?? execFile,
  );
  const parsed = JSON.parse(stdout) as Record<string, unknown>;
  if (parsed && typeof parsed === "object" && "error" in parsed) {
    const err = parsed.error as { message?: string; type?: string };
    throw new Error(
      `compute_live_allocation failed: ${err.type ?? "Error"} ${err.message ?? ""}`.trim(),
    );
  }
  if (!Array.isArray(parsed.weights)) {
    throw new Error("compute_live_allocation did not return weights");
  }
  return parsed as unknown as LiveAllocationResponse;
}

// validate_against_thesis input. The validator now takes the full
// candidate-batch payload inline via --input (no filesystem coupling).
// The thesis here is the *trimmed* shape the Python validator expects
// (objective, horizon_days, constraints) -- not the full workflow
// Thesis.
export type ValidateAgainstThesisRequest = {
  batch: RunCandidateBatchResponse;
  thesis: Record<string, unknown>;
};

export type ValidationViolation = {
  constraint: string;
  expected: number | string | null;
  actual: number | string | null;
};

export type ValidationResultRow = {
  candidate_id: string;
  passed: boolean;
  violations?: ValidationViolation[];
};

export type ValidateAgainstThesisResponse = {
  batch_id: string;
  run_id?: string;
  round?: number;
  results: ValidationResultRow[];
  passing_candidate_ids: string[];
};

export async function runValidateAgainstThesis(
  request: ValidateAgainstThesisRequest,
  options: {
    runScriptStdin?: typeof runScriptStdin;
    timeoutSeconds?: number;
  } = {},
): Promise<ValidateAgainstThesisResponse> {
  // The batch payload is ~1MB+ for typical backtests, well past the
  // argv limit. Stream it via stdin (--input -).
  const args: string[] = [
    "--input",
    "-",
    "--thesis",
    JSON.stringify(request.thesis),
  ];
  if (options.timeoutSeconds !== undefined) {
    args.push("--timeout-seconds", String(options.timeoutSeconds));
  }
  const runner = options.runScriptStdin ?? runScriptStdin;
  const stdout = await runner(
    "validate_against_thesis",
    args,
    JSON.stringify(request.batch),
  );
  const parsed = JSON.parse(stdout) as Record<string, unknown>;
  if (parsed && typeof parsed === "object" && "error" in parsed) {
    const err = parsed.error as { message?: string; type?: string };
    throw new Error(
      `validate_against_thesis failed: ${err.type ?? "Error"} ${err.message ?? ""}`.trim(),
    );
  }
  return parsed as unknown as ValidateAgainstThesisResponse;
}

export async function runRankUniverse(
  request: RankUniverseRequest,
  options: { execFile?: ExecFileLike } = {},
): Promise<RankUniverseRow[]> {
  const args = buildRankUniverseArgs(request);
  const stdout = await runScript("rank_universe", args, options.execFile ?? execFile);
  const parsed = JSON.parse(stdout);
  if (!Array.isArray(parsed)) {
    throw new Error("rank_universe returned a non-array payload");
  }
  return parsed as RankUniverseRow[];
}

export function buildRankUniverseArgs(request: RankUniverseRequest): string[] {
  const args: string[] = ["--top-n", String(request.top_n)];
  if (request.exclude_stablecoins) args.push("--exclude-stablecoins");
  if (request.exclude_wrapped) args.push("--exclude-wrapped");
  if (request.min_market_cap !== undefined) {
    args.push("--min-market-cap", String(request.min_market_cap));
  }
  if (request.min_history_days !== undefined) {
    args.push("--min-history-days", String(request.min_history_days));
  }
  if (request.risk_profile) {
    args.push("--risk-profile", request.risk_profile);
  }
  if (request.as_of) {
    args.push("--as-of", request.as_of);
  }
  return args;
}

async function runScript(
  moduleName: string,
  args: string[],
  execFileImpl: ExecFileLike,
): Promise<string> {
  return new Promise((resolveOutput, reject) => {
    execFileImpl(
      uvCommand(),
      [
        "run",
        "--project",
        ".",
        "python",
        "-m",
        `agent_invest_scripts.${moduleName}`,
        ...args,
      ],
      {
        cwd: scriptsDir,
        env: scriptEnv(scriptsDir),
        maxBuffer: SCRIPT_STDOUT_BUFFER_BYTES,
      },
      (error, stdoutValue, stderrValue) => {
        if (error) {
          // Several scripts (run_candidate_batch, validate_against_thesis)
          // emit their error payload on stdout via fail_json/print_json and
          // exit non-zero. Surface that structured message rather than the
          // opaque "Command failed: ...".
          const structured = extractStructuredError(stdoutValue);
          const stderr = stderrValue.trim();
          const detail =
            structured ??
            (stderr.length > 0 ? stderr : undefined) ??
            error.message;
          reject(new Error(`${moduleName}: ${detail}`));
          return;
        }
        resolveOutput(stdoutValue);
      },
    );
  });
}

// Variant of runScript that streams a large payload to the script via
// stdin instead of argv (whose Linux limit is ~128 KB).
export async function runScriptStdin(
  moduleName: string,
  args: string[],
  stdin: string,
): Promise<string> {
  return new Promise((resolveOutput, reject) => {
    const child = spawn(
      uvCommand(),
      [
        "run",
        "--project",
        ".",
        "python",
        "-m",
        `agent_invest_scripts.${moduleName}`,
        ...args,
      ],
      {
        cwd: scriptsDir,
        env: scriptEnv(scriptsDir),
      },
    );

    let stdoutBuf = "";
    let stderrBuf = "";
    child.stdout.setEncoding("utf-8");
    child.stderr.setEncoding("utf-8");
    child.stdout.on("data", (chunk: string) => {
      stdoutBuf += chunk;
    });
    child.stderr.on("data", (chunk: string) => {
      stderrBuf += chunk;
    });
    child.on("error", (error) => {
      reject(new Error(`${moduleName}: ${error.message}`));
    });
    child.on("close", (code) => {
      if (code === 0) {
        resolveOutput(stdoutBuf);
        return;
      }
      const structured = extractStructuredError(stdoutBuf);
      const stderrTrimmed = stderrBuf.trim();
      const detail =
        structured ??
        (stderrTrimmed.length > 0 ? stderrTrimmed : undefined) ??
        `exited with code ${code}`;
      reject(new Error(`${moduleName}: ${detail}`));
    });

    child.stdin.end(stdin, "utf-8");
  });
}

function extractStructuredError(stdout: string): string | undefined {
  const trimmed = stdout.trim();
  if (!trimmed) return undefined;
  try {
    const parsed = JSON.parse(trimmed) as { error?: { type?: string; message?: string } };
    if (parsed && typeof parsed === "object" && parsed.error) {
      const type = parsed.error.type ?? "Error";
      const message = parsed.error.message ?? "";
      return `${type}: ${message}`.trim();
    }
  } catch {
    /* not JSON; fall back to caller's default */
  }
  return undefined;
}
