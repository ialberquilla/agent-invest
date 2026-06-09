import { spawn } from "node:child_process";

import { scriptEnv, scriptsDirectory, uvCommand } from "../scripts-runtime.ts";

export const LIST_REGISTRY_NAMES = [
  "ranking_factors",
  "filters",
  "weighting_schemes",
  "signal_indicators",
  "window_selectors",
  "rebalance_triggers",
  "exit_rules",
  "composite_scorers",
  "universe_selectors",
] as const;

export type RegistryName = (typeof LIST_REGISTRY_NAMES)[number];

export type ListRegistryInput = {
  registry: RegistryName;
};

export type RegistryEntry = {
  id: string;
  params_schema: Record<string, unknown>;
  metadata?: Record<string, unknown>;
};

export type ListRegistryOutput = RegistryEntry[];

export async function listRegistry(
  input: ListRegistryInput,
): Promise<ListRegistryOutput> {
  assertRegistryName(input.registry);
  const output = await runScript([
    "-m",
    "agent_invest_scripts.list_registry",
    "--registry",
    input.registry,
  ]);

  return parseListRegistryOutput(output);
}

export function parseListRegistryOutput(raw: string): ListRegistryOutput {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("list_registry returned invalid JSON");
  }
  for (const [index, entry] of parsed.entries()) {
    if (!isRecord(entry)) throw new Error(`registry[${index}] must be an object`);
    if (typeof entry.id !== "string" || entry.id.length === 0) {
      throw new Error(`registry[${index}].id must be a non-empty string`);
    }
    if (!isRecord(entry.params_schema)) {
      throw new Error(`registry[${index}].params_schema must be an object`);
    }
    if (entry.metadata !== undefined && !isRecord(entry.metadata)) {
      throw new Error(`registry[${index}].metadata must be an object`);
    }
  }

  return parsed as ListRegistryOutput;
}

function assertRegistryName(value: string): asserts value is RegistryName {
  if (!(LIST_REGISTRY_NAMES as readonly string[]).includes(value)) {
    throw new Error(`Unknown registry: ${value}`);
  }
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

      if (code !== 0) {
        reject(new Error(err || `list_registry exited with code ${code}`));
        return;
      }
      resolve(out);
    });
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
