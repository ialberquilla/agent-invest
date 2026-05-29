import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

export type Candidate = {
  candidate_id: string;
  label?: string;
  template_id: string;
  universe_override?: Record<string, unknown> | null;
  filters?: Array<{ id: string; value: unknown }>;
  ranking?: Array<{
    factor: string;
    direction: "high" | "low";
    weight: number;
  }>;
  select_top?: number;
  config: Record<string, unknown>;
  window_override?: { start: string; end: string } | null;
  thesis?: {
    objective?: "high_growth" | "balanced" | "preserve_capital" | "income";
    primary_factors?: Array<{
      factor: string;
      direction: "high" | "low";
      weight: number;
    }>;
  };
};

export type RunCandidateBatchInput = {
  run_id: string;
  round: 1 | 2 | 3;
  iteration_hypothesis?: string;
  candidates: Candidate[];
  timeoutSeconds?: number;
};

export type RunCandidateBatchOutput = {
  batch_id: string;
  run_id: string;
  round: 1 | 2 | 3;
  iteration_hypothesis?: string;
  results: Array<Record<string, unknown>>;
};

export async function runCandidateBatch(
  input: RunCandidateBatchInput,
): Promise<RunCandidateBatchOutput> {
  validateInput(input);
  const { timeoutSeconds, ...payload } = input;
  const args = [
    "-m",
    "agent_invest_scripts.run_candidate_batch",
    "--input",
    JSON.stringify(payload),
  ];
  if (timeoutSeconds !== undefined) {
    args.push("--timeout-seconds", String(timeoutSeconds));
  }
  return parseRunCandidateBatchOutput(await runScript(args));
}

export function parseRunCandidateBatchOutput(
  raw: string,
): RunCandidateBatchOutput {
  const parsed: unknown = JSON.parse(raw);
  if (!isRecord(parsed))
    throw new Error("run_candidate_batch returned invalid JSON");
  assertString(parsed.batch_id, "batch_id");
  assertString(parsed.run_id, "run_id");
  if (parsed.round !== 1 && parsed.round !== 2 && parsed.round !== 3) {
    throw new Error("round must be 1, 2, or 3");
  }
  if (
    parsed.iteration_hypothesis !== undefined &&
    typeof parsed.iteration_hypothesis !== "string"
  ) {
    throw new Error("iteration_hypothesis must be a string");
  }
  if (!Array.isArray(parsed.results))
    throw new Error("results must be an array");
  for (const [index, result] of parsed.results.entries()) {
    if (!isRecord(result))
      throw new Error(`results[${index}] must be an object`);
    assertString(result.candidate_id, `results[${index}].candidate_id`);
    assertString(result.template_id, `results[${index}].template_id`);
    if (!isRecord(result.robustness)) {
      throw new Error(`results[${index}].robustness must be an object`);
    }
    const hasAllocation =
      result.allocation_metrics !== null &&
      result.allocation_metrics !== undefined;
    const hasTactical =
      result.tactical_metrics !== null && result.tactical_metrics !== undefined;
    if (hasAllocation === hasTactical) {
      throw new Error(
        `results[${index}] must populate exactly one of allocation_metrics or tactical_metrics`,
      );
    }
  }
  return parsed as RunCandidateBatchOutput;
}

function validateInput(input: RunCandidateBatchInput) {
  if (!input.run_id) throw new Error("run_id is required");
  if (input.round !== 1 && input.round !== 2 && input.round !== 3) {
    throw new Error("round must be 1, 2, or 3");
  }
  if (
    input.iteration_hypothesis !== undefined &&
    typeof input.iteration_hypothesis !== "string"
  ) {
    throw new Error("iteration_hypothesis must be a string");
  }
  if (!Array.isArray(input.candidates))
    throw new Error("candidates must be an array");
  if (input.candidates.length < 3)
    throw new Error("candidates must include at least 3 items");
  for (const [index, candidate] of input.candidates.entries()) {
    if (!candidate.candidate_id)
      throw new Error(`candidates[${index}].candidate_id is required`);
    if (!candidate.template_id)
      throw new Error(`candidates[${index}].template_id is required`);
    if (!isRecord(candidate.config))
      throw new Error(`candidates[${index}].config must be an object`);
  }
}

function runScript(args: string[]) {
  return new Promise<string>((resolve, reject) => {
    const child = spawn("uv", ["run", "--project", ".", "python", ...args], {
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
        reject(
          new Error(err || `run_candidate_batch exited with code ${code}`),
        );
        return;
      }
      resolve(out);
    });
  });
}

function scriptsDirectory() {
  return path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../scripts",
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}
