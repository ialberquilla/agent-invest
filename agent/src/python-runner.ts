import { existsSync } from "node:fs";
import path from "node:path";

export function pythonExecutable(projectDir: string) {
  const venvPython = path.join(
    projectDir,
    ".venv",
    process.platform === "win32" ? "Scripts/python.exe" : "bin/python",
  );
  return existsSync(venvPython) ? venvPython : "python";
}

export function pythonEnv(projectDir: string) {
  const pythonPath = process.env.PYTHONPATH
    ? `${projectDir}${path.delimiter}${process.env.PYTHONPATH}`
    : projectDir;
  return { ...process.env, PYTHONPATH: pythonPath };
}
