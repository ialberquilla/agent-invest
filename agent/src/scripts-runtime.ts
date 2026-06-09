import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export function scriptsDirectory(moduleUrl: string, sourceRelativePath: string) {
  const candidates = [
    process.env.AGENT_SCRIPTS_DIR,
    path.resolve(process.cwd(), "agent/scripts"),
    path.resolve(process.cwd(), "scripts"),
    path.resolve(path.dirname(fileURLToPath(moduleUrl)), sourceRelativePath),
  ].filter((candidate): candidate is string => Boolean(candidate));

  for (const candidate of candidates) {
    if (existsSync(path.join(candidate, "agent_invest_scripts"))) {
      return candidate;
    }
  }

  return candidates[candidates.length - 1];
}

export function scriptEnv(projectDir: string) {
  return {
    ...process.env,
    PATH: scriptPath(),
    PYTHONPATH: process.env.PYTHONPATH
      ? `${projectDir}${path.delimiter}${process.env.PYTHONPATH}`
      : projectDir,
  };
}

export function uvCommand() {
  return process.env.UV_BIN?.trim() || "uv";
}

function scriptPath() {
  const extras = ["/usr/local/bin", "/root/.local/bin"];
  const current = process.env.PATH ?? "";
  return [...extras, current].filter(Boolean).join(path.delimiter);
}
