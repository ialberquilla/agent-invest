import assert from "node:assert/strict";
import { once } from "node:events";
import { spawn } from "node:child_process";
import type { Readable } from "node:stream";
import test from "node:test";
import { fileURLToPath } from "node:url";

function collect(stream: Readable | null) {
  if (!stream) {
    return Promise.resolve("");
  }

  let output = "";
  stream.setEncoding("utf8");
  stream.on("data", (chunk: string) => {
    output += chunk;
  });

  return once(stream, "end").then(() => output);
}

test("run_agent_script.sh kills long-running scripts at the configured timeout", async () => {
  const runnerPath = fileURLToPath(
    new URL("../../scripts/run_agent_script.sh", import.meta.url),
  );
  const proc = spawn(
    "bash",
    [runnerPath, "test_fixtures.sleep", "--seconds", "5"],
    {
      env: {
        ...process.env,
        AGENT_SCRIPT_TIMEOUT_MS: "250",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const stdout = collect(proc.stdout);
  const stderr = collect(proc.stderr);
  const closePromise = once(proc, "close") as Promise<
    [number | null, NodeJS.Signals | null]
  >;
  let timer: NodeJS.Timeout | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      proc.kill("SIGKILL");
      reject(new Error("wrapper did not exit after the configured timeout"));
    }, 30000);
  });
  const [exitCode] = await Promise.race([closePromise, timeoutPromise]);

  if (timer) {
    clearTimeout(timer);
  }

  assert.equal(exitCode, 124);
  assert.equal(await stdout, "");
  assert.match(
    await stderr,
    /AGENT_SCRIPT_TIMEOUT: test_fixtures\.sleep exceeded 250ms/,
  );
});
