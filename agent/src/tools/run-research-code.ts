import { spawn } from "node:child_process";

import { scriptEnv, scriptsDirectory, uvCommand } from "../scripts-runtime.ts";

export type RunResearchCodeInput = {
  code: string;
  purpose: string;
  timeoutSeconds?: number;
};

export type RunResearchCodeResult = {
  code: string;
  purpose: string;
  stdout: string;
  result: unknown;
  artifacts: { name: string; path: string; bytes: number }[];
  assumptions: string[];
  limits: Record<string, unknown>;
  error?: { type: string; message: string };
};

export async function runResearchCode(
  input: RunResearchCodeInput,
): Promise<RunResearchCodeResult> {
  const code = input.code.trim();
  const purpose = input.purpose.trim();
  if (!code) throw new Error("code is required");
  if (!purpose) throw new Error("purpose is required");
  if (process.env.RESEARCH_MODE !== "true") {
    throw new Error("run_research_code requires RESEARCH_MODE=true");
  }
  const args = ["-m", "agent_invest_scripts.run_research_code", "--code", code, "--purpose", purpose];
  if (input.timeoutSeconds !== undefined) args.push("--timeout-seconds", String(input.timeoutSeconds));
  const parsed: unknown = JSON.parse(await runScript(args));
  if (!isRecord(parsed)) throw new Error("run_research_code returned invalid JSON");
  return parsed as RunResearchCodeResult;
}

function runScript(args: string[]) {
  return new Promise<string>((resolve, reject) => {
    const scriptsDir = scriptsDirectory(import.meta.url, "../../scripts");
    const child = spawn(uvCommand(), ["run", "--project", ".", "python", ...args], {
      cwd: scriptsDir,
      env: scriptEnv(scriptsDir),
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
      if (code !== 0) reject(new Error(err || `run_research_code exited with code ${code}`));
      else resolve(out);
    });
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
