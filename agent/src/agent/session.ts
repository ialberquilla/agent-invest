import {
  createOpencodeClient as createSdkOpencodeClient,
  createOpencodeServer,
  type Auth,
  type AssistantMessage,
  type OpencodeClient,
  type Event as OpencodeEvent,
  type Part,
} from "@opencode-ai/sdk";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { db as defaultDb } from "../db/client";
import {
  readStrategySession,
  updateStrategySession,
} from "../db/repositories/strategies";

export const DEFAULT_OPENCODE_MODEL = "azure/gpt-5.4";

export const OPENCODE_BUILTIN_TOOLS = [
  "bash",
  "edit",
  "glob",
  "google_search",
  "grep",
  "list",
  "lsp",
  "patch",
  "read",
  "skill",
  "task",
  "todowrite",
  "webfetch",
  "websearch",
  "write",
] as const;

export const AGENT_INVEST_MCP_NAME = "agent_invest";
export const RUN_STRATEGY_PIPELINE_TOOL = `${AGENT_INVEST_MCP_NAME}_run_strategy_pipeline`;
export const SCREEN_MARKETS_TOOL = `${AGENT_INVEST_MCP_NAME}_screen_markets`;

export function disabledOpencodeBuiltinsTools(
  options: { except?: readonly string[] } = {},
): Record<string, boolean> {
  const except = new Set(options.except ?? []);
  return Object.fromEntries(
    OPENCODE_BUILTIN_TOOLS.map((tool) => [tool, except.has(tool)]),
  );
}

type SessionDb = Parameters<typeof readStrategySession>[2];

export type DatabaseClient = NonNullable<SessionDb>;

export type SessionClient = {
  createSession(title: string): Promise<string>;
};

export type OpencodePromptRequest = {
  sessionId: string;
  text: string;
  system: string;
  messageId?: string;
};

export type OpencodePromptResult = {
  info: AssistantMessage;
  parts: Part[];
};

export type OpencodeStreamOptions = {
  signal?: AbortSignal;
};

export type OpencodeTurnClient = {
  prompt(request: OpencodePromptRequest): Promise<OpencodePromptResult>;
  getSession(sessionId: string): Promise<{ title: string }>;
  subscribeEvents(
    options?: OpencodeStreamOptions,
  ): Promise<AsyncIterable<OpencodeEvent>>;
};

type SessionManagerOptions = {
  db?: DatabaseClient;
  getOpencodeClient?: () => Promise<SessionClient>;
  readStrategySession?: typeof readStrategySession;
  updateStrategySession?: typeof updateStrategySession;
};

export type ManagedOpencode = {
  client: OpencodeClient;
  close(): void;
};

let sharedOpencode: Promise<ManagedOpencode> | undefined;

export function resolveOpencodeModel(env: NodeJS.ProcessEnv = process.env) {
  const model = env.OPENCODE_MODEL?.trim();

  return model ? model : DEFAULT_OPENCODE_MODEL;
}

export function parseOpencodeModel(modelString: string) {
  const slash = modelString.indexOf("/");
  if (slash <= 0 || slash === modelString.length - 1) {
    throw new Error(
      `OPENCODE_MODEL must be in the form '<providerID>/<modelID>': ${modelString}`,
    );
  }
  return {
    providerID: modelString.slice(0, slash),
    modelID: modelString.slice(slash + 1),
  };
}

function resolveOpencodeBaseUrl(env: NodeJS.ProcessEnv = process.env) {
  const baseUrl = env.OPENCODE_BASE_URL?.trim();

  return baseUrl ? baseUrl : undefined;
}

function resolveOpencodeDirectory(env: NodeJS.ProcessEnv = process.env) {
  const directory = env.OPENCODE_DIRECTORY?.trim();

  return directory ? directory : process.cwd();
}

function resolveAgentToolUrl(env: NodeJS.ProcessEnv = process.env) {
  const explicit = env.AGENT_TOOL_URL?.trim();
  if (explicit) return explicit;

  const port = env.PORT?.trim() || "3000";
  return `http://127.0.0.1:${port}`;
}

function resolveRunStrategyPipelineMcpScript() {
  const candidates: string[] = [];
  let current = path.dirname(fileURLToPath(import.meta.url));
  while (true) {
    candidates.push(
      path.join(current, "src/agent/tools/run-strategy-pipeline-mcp.mjs"),
      path.join(current, "agent/src/agent/tools/run-strategy-pipeline-mcp.mjs"),
    );

    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }

  const script = candidates.find((candidate) => existsSync(candidate));
  if (!script) {
    throw new Error("Unable to locate run-strategy-pipeline MCP script");
  }

  return script;
}

function resolveOpencodeAuth(env: NodeJS.ProcessEnv = process.env) {
  const providerID = env.OPENCODE_AUTH_PROVIDER?.trim();
  const key =
    env.OPENCODE_AUTH_KEY?.trim() ||
    (providerID === "azure" ? env.AZURE_API_KEY?.trim() : undefined);

  if (!providerID && !key) return undefined;
  if (!providerID)
    throw new Error(
      "OPENCODE_AUTH_PROVIDER is required when OPENCODE_AUTH_KEY is set",
    );
  if (!key)
    throw new Error(
      providerID === "azure"
        ? "AZURE_API_KEY or OPENCODE_AUTH_KEY is required when OPENCODE_AUTH_PROVIDER=azure"
        : "OPENCODE_AUTH_KEY is required when OPENCODE_AUTH_PROVIDER is set",
    );
  if (providerID === "azure" && !env.AZURE_RESOURCE_NAME?.trim()) {
    throw new Error(
      "AZURE_RESOURCE_NAME is required when OPENCODE_AUTH_PROVIDER=azure",
    );
  }

  return {
    providerID,
    auth: { type: "api", key } satisfies Auth,
  };
}

async function configureOpencodeAuth(
  client: OpencodeClient,
  env: NodeJS.ProcessEnv = process.env,
) {
  const resolved = resolveOpencodeAuth(env);

  if (!resolved) return;

  await client.auth.set({
    path: { id: resolved.providerID },
    body: resolved.auth,
    throwOnError: true,
  });
}

function normalizeSessionId(sessionId: string | null | undefined) {
  const normalized = sessionId?.trim();

  return normalized ? normalized : undefined;
}

async function createManagedOpencode(
  env: NodeJS.ProcessEnv = process.env,
): Promise<ManagedOpencode> {
  const baseUrl = resolveOpencodeBaseUrl(env);
  const directory = resolveOpencodeDirectory(env);

  if (baseUrl) {
    const client = createSdkOpencodeClient({
      baseUrl,
      directory,
    });

    await configureOpencodeAuth(client, env);

    return {
      client,
      close() {},
    };
  }

  const server = await createOpencodeServer({
    hostname: "127.0.0.1",
    port: 0,
    config: {
      model: resolveOpencodeModel(env),
      mcp: {
        [AGENT_INVEST_MCP_NAME]: {
          type: "local",
          command: ["node", resolveRunStrategyPipelineMcpScript()],
          enabled: true,
          environment: {
            AGENT_TOOL_URL: resolveAgentToolUrl(env),
            ...(env.AGENT_API_KEY ? { AGENT_API_KEY: env.AGENT_API_KEY } : {}),
          },
        },
      },
      permission: {
        bash: {
          "./agent-invest *": "allow",
          "./agent-invest": "allow",
          "agent-invest *": "allow",
          "agent-invest": "allow",
          "*": "deny",
        },
        edit: "deny",
        webfetch: "deny",
      },
    },
  });

  const client = createSdkOpencodeClient({
    baseUrl: server.url,
    directory,
  });

  await configureOpencodeAuth(client, env);

  return {
    client,
    close() {
      server.close();
    },
  };
}

export async function getOrCreateManagedOpencode(
  env: NodeJS.ProcessEnv = process.env,
): Promise<ManagedOpencode> {
  if (!sharedOpencode) {
    sharedOpencode = createManagedOpencode(env).catch((error: unknown) => {
      sharedOpencode = undefined;
      throw error;
    });
  }

  return sharedOpencode;
}

export async function createOpencodeClient(
  env: NodeJS.ProcessEnv = process.env,
): Promise<SessionClient & OpencodeTurnClient> {
  const { client } = await getOrCreateManagedOpencode(env);

  return {
    async createSession(title: string) {
      const normalizedTitle = title.trim();
      const session = await client.session.create({
        body: normalizedTitle ? { title: normalizedTitle } : undefined,
        throwOnError: true,
      });

      return session.data.id;
    },
    async prompt({ sessionId, text, system, messageId }) {
      const model = parseOpencodeModel(resolveOpencodeModel(env));
      const response = await client.session.prompt({
        path: { id: sessionId },
        body: {
          messageID: messageId,
          model,
          tools: { question: false },
          system,
          parts: [{ type: "text", text }],
        },
        throwOnError: true,
      });

      return response.data;
    },
    async getSession(sessionId: string) {
      const response = await client.session.get({
        path: { id: sessionId },
        throwOnError: true,
      });

      return response.data;
    },
    async subscribeEvents(options = {}) {
      const response = await client.event.subscribe({
        signal: options.signal,
      });

      return response.stream;
    },
  };
}

export function createSessionManager(options: SessionManagerOptions = {}) {
  const db = options.db ?? defaultDb;
  const getOpencodeClient = options.getOpencodeClient ?? createOpencodeClient;
  const readSession = options.readStrategySession ?? readStrategySession;
  const updateSession = options.updateStrategySession ?? updateStrategySession;

  async function resolveWithDb(
    tx: DatabaseClient,
    strategyId: string,
    lockRow: boolean,
  ) {
    const strategy = await readSession(
      strategyId,
      { lockForUpdate: lockRow },
      tx,
    );
    if (!strategy) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    const existingSessionId = normalizeSessionId(strategy.opencodeSessionId);
    if (existingSessionId) return existingSessionId;

    const opencode = await getOpencodeClient();
    const sessionId = await opencode.createSession(strategy.title);
    const updatedRows = await updateSession(strategyId, sessionId, tx);

    if (updatedRows !== 1) {
      throw new Error(
        `Failed to persist opencode session for strategy: ${strategyId}`,
      );
    }

    return sessionId;
  }

  return {
    async getOrCreateSession(strategyId: string, existingDb?: DatabaseClient) {
      // When called inside an existing transaction, reuse it to avoid a
      // self-deadlock between the caller's row lock and another FOR UPDATE.
      if (existingDb) {
        return resolveWithDb(existingDb, strategyId, false);
      }

      return db.transaction((tx) => resolveWithDb(tx, strategyId, true));
    },
  };
}

export const { getOrCreateSession } = createSessionManager();
