import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { pythonEnv, pythonExecutable } from "../python-runner.ts";

export type UniverseSelector = {
  id: string;
  params?: Record<string, unknown>;
};

export type RankUniverseFilter = {
  id: string;
  value: unknown;
};

export type RankUniverseRanking = {
  factor: string;
  direction: "high" | "low";
  weight: number;
};

export type RankUniverseInput = {
  universe_selector: UniverseSelector;
  filters?: RankUniverseFilter[];
  ranking: RankUniverseRanking[];
  limit?: number;
  asOf?: string;
  timeoutSeconds?: number;
};

export type RankUniverseOutput = {
  coin_id: string;
  rank: number;
  factor_values: Record<string, number | null>;
}[];

export async function rankUniverse(
  input: RankUniverseInput,
): Promise<RankUniverseOutput> {
  validateInput(input);
  const { asOf, timeoutSeconds, ...payload } = input;
  const args = [
    "-m",
    "agent_invest_scripts.rank_universe",
    "--input",
    JSON.stringify(payload),
  ];
  if (asOf) args.push("--as-of", asOf);
  if (timeoutSeconds !== undefined) {
    args.push("--timeout-seconds", String(timeoutSeconds));
  }

  return parseRankUniverseOutput(await runScript(args));
}

export function parseRankUniverseOutput(raw: string): RankUniverseOutput {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed))
    throw new Error("rank_universe returned invalid JSON");
  for (const [index, row] of parsed.entries()) {
    if (!isRecord(row))
      throw new Error(`rank_universe[${index}] must be an object`);
    if (typeof row.coin_id !== "string" || row.coin_id.length === 0) {
      throw new Error(
        `rank_universe[${index}].coin_id must be a non-empty string`,
      );
    }
    if (
      typeof row.rank !== "number" ||
      !Number.isInteger(row.rank) ||
      row.rank < 1
    ) {
      throw new Error(
        `rank_universe[${index}].rank must be a positive integer`,
      );
    }
    if (!isRecord(row.factor_values)) {
      throw new Error(
        `rank_universe[${index}].factor_values must be an object`,
      );
    }
  }
  return parsed as RankUniverseOutput;
}

function validateInput(input: RankUniverseInput) {
  if (!input.universe_selector?.id) {
    throw new Error("universe_selector.id is required");
  }
  if (!input.ranking?.length) {
    throw new Error("ranking must be non-empty");
  }
  for (const ranking of input.ranking) {
    if (!ranking.factor) throw new Error("ranking.factor is required");
    if (ranking.direction !== "high" && ranking.direction !== "low") {
      throw new Error("ranking.direction must be high or low");
    }
    if (!(ranking.weight > 0))
      throw new Error("ranking.weight must be positive");
  }
  if (
    input.limit !== undefined &&
    (!Number.isInteger(input.limit) || input.limit < 1)
  ) {
    throw new Error("limit must be a positive integer");
  }
}

function runScript(args: string[]) {
  return new Promise<string>((resolve, reject) => {
    const scriptsDir = scriptsDirectory();
    const child = spawn(pythonExecutable(scriptsDir), args, {
      cwd: scriptsDir,
      env: pythonEnv(scriptsDir),
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
        reject(new Error(err || `rank_universe exited with code ${code}`));
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
