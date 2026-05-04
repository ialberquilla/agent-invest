/**
 * Local probe: send a single prompt to the agent (via opencode) and stream
 * everything that comes back — every event, every tool call, every text chunk,
 * the final assistant reply, and the artifact files written under STORAGE_ROOT.
 *
 * Usage:
 *   STORAGE_ROOT=$PWD/../.data/storage \
 *     pnpm --filter @agent-invest/agent exec tsx probe.ts \
 *     "Backtest a 60/40 BTC/ETH portfolio for 2024 with monthly rebalance."
 *
 * Notes:
 *   - Bypasses Fastify and the Postgres `runs` table — talks straight to opencode.
 *   - Uses ephemeral userId/strategyId; nothing is persisted in the DB.
 *   - Requires STORAGE_ROOT to point at a directory containing
 *     `datasets/daily_prices.parquet` (and optionally `universe_history.parquet`).
 *   - The model used is whatever OPENCODE_MODEL is set to (default openai/gpt-5).
 *     If the model provider has no credentials, opencode will abort the turn
 *     within ~100ms with no parts — surfaced explicitly below.
 */

import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";

import {
  createOpencodeClient as createSdkOpencodeClient,
  createOpencodeServer,
  type Event as OpencodeEvent,
  type OpencodeClient,
} from "@opencode-ai/sdk";

import { buildSystemPrompt } from "./src/agent/prompt";
import {
  parseOpencodeModel,
  resolveOpencodeModel,
} from "./src/agent/session";

const PROMPT_TEXT =
  process.argv.slice(2).join(" ").trim() ||
  "Backtest a 60/40 BTC/ETH portfolio for 2024 with monthly rebalance. Report Sharpe, max drawdown, and the equity_curve.png path.";

const USER_ID = `probe-${Date.now()}`;
const STRATEGY_ID = `probe-strategy-${Date.now()}`;
const IDLE_TIMEOUT_MS = 5 * 60 * 1000;

function timestamp() {
  return new Date().toISOString().slice(11, 23);
}

function log(label: string, value?: unknown) {
  if (value === undefined) {
    process.stdout.write(`[${timestamp()}] ${label}\n`);
    return;
  }
  process.stdout.write(
    `[${timestamp()}] ${label} ${JSON.stringify(value, null, 2)}\n`,
  );
}

type ArtifactInfo = { path: string; mtimeMs: number; bytes: number };

async function listArtifacts(storageRoot: string): Promise<string[]> {
  const root = join(storageRoot, "artifacts", "run_backtest");
  try {
    const entries = await readdir(root, { withFileTypes: true });
    const files: string[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const labelDir = join(root, entry.name);
      const labelEntries = await readdir(labelDir);
      for (const file of labelEntries) {
        files.push(join(labelDir, file));
      }
    }
    return files;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}

async function snapshotArtifacts(
  storageRoot: string,
): Promise<Map<string, ArtifactInfo>> {
  const files = await listArtifacts(storageRoot);
  const sized = await Promise.all(
    files.map(async (path) => {
      const info = await stat(path);
      return { path, mtimeMs: info.mtimeMs, bytes: info.size };
    }),
  );
  return new Map(sized.map((entry) => [entry.path, entry]));
}

async function startOpencode() {
  const baseUrl = process.env.OPENCODE_BASE_URL?.trim();
  const directory = process.env.OPENCODE_DIRECTORY?.trim() || process.cwd();

  if (baseUrl) {
    log(`reusing opencode server at ${baseUrl}`);
    return {
      client: createSdkOpencodeClient({ baseUrl, directory }),
      close() {},
    };
  }

  log("starting embedded opencode server");
  const server = await createOpencodeServer({
    hostname: "127.0.0.1",
    port: 0,
    config: { model: resolveOpencodeModel() },
  });
  log(`embedded opencode server listening at ${server.url}`);
  return {
    client: createSdkOpencodeClient({ baseUrl: server.url, directory }),
    close() {
      server.close();
    },
  };
}

async function fetchAssistantMessage(client: OpencodeClient, sessionId: string) {
  const response = await client.session.messages({
    path: { id: sessionId },
    throwOnError: true,
  });
  const messages = response.data ?? [];
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const entry = messages[index];
    if (entry.info.role === "assistant") return entry;
  }
  return null;
}

async function main() {
  const storageRoot = process.env.STORAGE_ROOT?.trim();
  if (!storageRoot) {
    process.stderr.write(
      "STORAGE_ROOT must be set (point it at a dir containing datasets/daily_prices.parquet)\n",
    );
    process.exit(1);
  }

  const modelString = resolveOpencodeModel();
  log(`STORAGE_ROOT = ${storageRoot}`);
  log(`OPENCODE_MODEL = ${modelString}`);
  log(`USER_ID = ${USER_ID}`);
  log(`STRATEGY_ID = ${STRATEGY_ID}`);
  log(`PROMPT = ${PROMPT_TEXT}`);

  const before = await snapshotArtifacts(storageRoot);
  log(`pre-existing artifact files: ${before.size}`);

  const { client, close: closeOpencode } = await startOpencode();

  const sessionResponse = await client.session.create({
    body: { title: `probe ${USER_ID}` },
    throwOnError: true,
  });
  const sessionId = sessionResponse.data.id;
  log("session created", { sessionId });

  const system = await buildSystemPrompt({
    userId: USER_ID,
    strategyId: STRATEGY_ID,
  });

  const eventsAbort = new AbortController();
  const eventStream = await client.event.subscribe({
    signal: eventsAbort.signal,
  });

  let sawIdleForSession = false;
  let sessionAborted = false;
  let messageError: string | null = null;
  const idleSignals: Array<() => void> = [];

  const eventLoop = (async () => {
    try {
      for await (const event of eventStream.stream as AsyncIterable<OpencodeEvent>) {
        if (event.type === "session.error") {
          log("event session.error", event);
          messageError = JSON.stringify(event.properties);
          continue;
        }
        if (event.type === "session.idle") {
          const properties = event.properties as { sessionID?: string };
          if (properties.sessionID === sessionId) {
            sawIdleForSession = true;
            for (const resolve of idleSignals.splice(0)) resolve();
          }
        }
        if (event.type === "message.updated") {
          const info = (event.properties as { info?: { error?: unknown } })
            .info;
          if (info?.error) {
            log("message error in flight", info.error);
            messageError ??= JSON.stringify(info.error);
          }
        }
        if (event.type === "session.status") {
          const properties = event.properties as {
            sessionID?: string;
            status?: { type?: string };
          };
          if (
            properties.sessionID === sessionId &&
            properties.status?.type === "aborted"
          ) {
            sessionAborted = true;
          }
        }
        log(`event ${event.type}`, event);
      }
    } catch (error) {
      if (eventsAbort.signal.aborted) return;
      log("event stream error", { message: (error as Error).message });
    }
  })();

  const waitForIdle = () =>
    new Promise<void>((resolve, reject) => {
      if (sawIdleForSession) {
        resolve();
        return;
      }
      const timeout = setTimeout(() => {
        reject(new Error(`session did not go idle within ${IDLE_TIMEOUT_MS}ms`));
      }, IDLE_TIMEOUT_MS);
      idleSignals.push(() => {
        clearTimeout(timeout);
        resolve();
      });
    });

  log("calling client.session.prompt — this may take a while");
  const promptStart = Date.now();
  const promptResponse = await client.session.prompt({
    path: { id: sessionId },
    body: {
      model: parseOpencodeModel(modelString),
      tools: { question: false },
      system,
      parts: [{ type: "text", text: PROMPT_TEXT }],
    },
    throwOnError: true,
  });
  const elapsedMs = Date.now() - promptStart;
  log(`client.session.prompt resolved in ${elapsedMs}ms`);

  const directParts = promptResponse.data?.parts;
  if (!directParts || directParts.length === 0) {
    log(
      `direct prompt response has no parts — waiting for session.idle and re-fetching`,
      { rawResponse: promptResponse.data },
    );
    try {
      await waitForIdle();
    } catch (error) {
      log(`idle wait failed: ${(error as Error).message}`);
    }
  }

  const finalAssistant = await fetchAssistantMessage(client, sessionId);
  log("=== ASSISTANT PARTS (from session.messages) ===");
  if (!finalAssistant) {
    log("no assistant message in session — turn never started");
  } else {
    log(`assistant message id=${finalAssistant.info.id}`, {
      role: finalAssistant.info.role,
      error: (finalAssistant.info as { error?: unknown }).error,
      time: finalAssistant.info.time,
    });
    for (const part of finalAssistant.parts ?? []) {
      if (part.type === "text") {
        log(`text part`);
        process.stdout.write(`${part.text}\n`);
        continue;
      }
      if (part.type === "tool") {
        log(`tool part name=${part.tool} status=${part.state.status}`, part);
        continue;
      }
      log(`part type=${part.type}`, part);
    }
  }

  if (messageError) log("aggregate message error", messageError);
  if (sessionAborted) log("session.status reported aborted");

  const after = await snapshotArtifacts(storageRoot);
  const newOrChanged = [...after.values()].filter((entry) => {
    const previous = before.get(entry.path);
    return !previous || previous.mtimeMs !== entry.mtimeMs;
  });

  log("=== NEW OR CHANGED ARTIFACTS ===");
  for (const entry of newOrChanged) {
    log(`  ${entry.path} (${entry.bytes} bytes)`);
  }
  if (!newOrChanged.length) {
    log(
      "  (none — agent did not run a backtest, errored, or wrote outside the expected dir)",
    );
  }

  eventsAbort.abort();
  await eventLoop.catch(() => undefined);
  closeOpencode();
  process.exit(0);
}

main().catch((error: unknown) => {
  process.stderr.write(`probe failed: ${(error as Error).stack ?? error}\n`);
  process.exit(1);
});
