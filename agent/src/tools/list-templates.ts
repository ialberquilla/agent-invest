import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

export type ListTemplatesInput = Record<string, never>;

export type TemplateMetadata = {
  id: string;
  category: "allocation" | "tactical";
  preferred_factors: string[];
  default_universe: Record<string, unknown>;
  min_history_days: number;
  composite_formula: string;
  slot_schema: Record<string, unknown>;
};

export type ListTemplatesOutput = TemplateMetadata[];

export async function listTemplates(
  _input: ListTemplatesInput = {},
): Promise<ListTemplatesOutput> {
  const output = await runScript(["-m", "agent_invest_scripts.list_templates"]);

  return parseListTemplatesOutput(output);
}

export function parseListTemplatesOutput(raw: string): ListTemplatesOutput {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("list_templates returned invalid JSON");
  }
  for (const [index, template] of parsed.entries()) {
    assertTemplateMetadata(template, index);
  }

  return parsed as ListTemplatesOutput;
}

function assertTemplateMetadata(value: unknown, index: number): void {
  if (!isRecord(value))
    throw new Error(`templates[${index}] must be an object`);
  if (typeof value.id !== "string" || value.id.length === 0) {
    throw new Error(`templates[${index}].id must be a non-empty string`);
  }
  if (value.category !== "allocation" && value.category !== "tactical") {
    throw new Error(
      `templates[${index}].category must be allocation or tactical`,
    );
  }
  if (!isStringArray(value.preferred_factors)) {
    throw new Error(
      `templates[${index}].preferred_factors must be a string array`,
    );
  }
  if (!isRecord(value.default_universe)) {
    throw new Error(`templates[${index}].default_universe must be an object`);
  }
  if (!Number.isInteger(value.min_history_days)) {
    throw new Error(`templates[${index}].min_history_days must be an integer`);
  }
  if (typeof value.composite_formula !== "string") {
    throw new Error(`templates[${index}].composite_formula must be a string`);
  }
  if (!isRecord(value.slot_schema)) {
    throw new Error(`templates[${index}].slot_schema must be an object`);
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
        reject(new Error(err || `list_templates exited with code ${code}`));
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

function isStringArray(value: unknown): value is string[] {
  return (
    Array.isArray(value) && value.every((entry) => typeof entry === "string")
  );
}
