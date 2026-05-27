import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

type RecoveryEpisode = {
  peak_date: string;
  peak_price: number;
  trough_date: string;
  trough_price: number;
  drawdown_pct: number;
  recovery_date: string | null;
  peak_to_trough_days: number;
  trough_to_recovery_days: number | null;
};

type RecoveryReport = {
  coin_id?: string;
  coverage: {
    first_price_date: string;
    last_price_date: string;
    history_days: number;
  };
  current_state: {
    current_drawdown_from_ath: number;
    days_since_ath: number;
    currently_above_sma_200d: boolean | null;
  };
  drawdown_episodes: RecoveryEpisode[];
  recovery_stats: {
    n_episodes: number;
    n_recovered: number;
    n_unrecovered: number;
    n_in_progress: number;
    recovery_rate: number | null;
    median_recovery_days: number | null;
    p90_recovery_days: number | null;
    max_recovery_days: number | null;
  };
  rolling_horizon_returns: {
    n_windows: number;
    median: number | null;
    p10: number | null;
    p90: number | null;
    pct_negative: number | null;
    worst_window: { start: string; end: string; return: number } | null;
  };
  horizon_verdict: {
    pct_drawdowns_recovered_within_horizon: number | null;
    pct_horizon_windows_positive: number | null;
    worst_horizon_loss: number | null;
  };
  survivorship_warning: boolean;
};

export type AnalyzeRecoveryInput = {
  coinIds?: string[];
  allocation?: Record<string, unknown>;
  horizonDays: number;
  minDrawdownPct?: number;
  asOf?: string;
  timeoutSeconds?: number;
};

export type AnalyzeRecoveryOutput = {
  as_of: string;
  horizon_days: number;
  min_drawdown_pct: number;
  coins?: RecoveryReport[];
  portfolio?: RecoveryReport;
};

export async function analyzeRecovery(
  input: AnalyzeRecoveryInput,
): Promise<AnalyzeRecoveryOutput> {
  const args = buildArgs(input);
  const output = await runScript(args);

  return JSON.parse(output) as AnalyzeRecoveryOutput;
}

function buildArgs(input: AnalyzeRecoveryInput) {
  if (input.horizonDays <= 0) {
    throw new Error("horizonDays must be positive");
  }
  if (input.coinIds?.length && input.allocation) {
    throw new Error("Provide either coinIds or allocation, not both");
  }
  if (!input.coinIds?.length && !input.allocation) {
    throw new Error("Provide coinIds or allocation");
  }

  const args = [
    "-m",
    "agent_invest_scripts.analyze_recovery",
    "--horizon-days",
    String(input.horizonDays),
  ];
  if (input.coinIds?.length) {
    args.push("--coin-ids", input.coinIds.join(","));
  } else {
    args.push("--allocation", JSON.stringify(input.allocation));
  }
  if (input.minDrawdownPct !== undefined) {
    args.push("--min-drawdown-pct", String(input.minDrawdownPct));
  }
  if (input.asOf) {
    args.push("--as-of", input.asOf);
  }
  if (input.timeoutSeconds !== undefined) {
    args.push("--timeout-seconds", String(input.timeoutSeconds));
  }

  return args;
}

function runScript(args: string[]) {
  return new Promise<string>((resolve, reject) => {
    const child = spawn("python", args, {
      cwd: scriptsDirectory(),
      env: { ...process.env, PYTHONPATH: scriptsDirectory() },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];

    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      const out = Buffer.concat(stdout).toString("utf8");
      const err = Buffer.concat(stderr).toString("utf8");

      if (code !== 0) {
        reject(new Error(err || `analyze_recovery exited with code ${code}`));
        return;
      }
      resolve(out);
    });
  });
}

function scriptsDirectory() {
  const currentFile = fileURLToPath(import.meta.url);

  return path.resolve(path.dirname(currentFile), "../../scripts");
}
