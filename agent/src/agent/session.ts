import {
  createOpencodeClient as createSdkOpencodeClient,
  createOpencodeServer,
  type AssistantMessage,
  type OpencodeClient,
  type Event as OpencodeEvent,
  type Part,
} from "@opencode-ai/sdk";
import type { QueryResultRow } from "pg";

import { pg } from "../db/client";

export const DEFAULT_OPENCODE_MODEL = "openai/gpt-5";

type QueryResult<TRow extends QueryResultRow> = {
  rowCount: number | null;
  rows: TRow[];
};

export type DatabaseClient = {
  query<TRow extends QueryResultRow = QueryResultRow>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<TRow>>;
  release(): void;
};

export type DatabasePool = {
  connect(): Promise<DatabaseClient>;
};

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
  getOpencodeClient?: () => Promise<SessionClient>;
  pool?: DatabasePool;
};

type StrategySessionRow = {
  opencode_session_id: string | null;
  title: string;
};

type ManagedOpencode = {
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
    return {
      client: createSdkOpencodeClient({
        baseUrl,
        directory,
      }),
      close() {},
    };
  }

  const server = await createOpencodeServer({
    hostname: "127.0.0.1",
    port: 0,
    config: {
      model: resolveOpencodeModel(env),
    },
  });

  return {
    client: createSdkOpencodeClient({
      baseUrl: server.url,
      directory,
    }),
    close() {
      server.close();
    },
  };
}

async function getOrCreateManagedOpencode(
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
  const pool = options.pool ?? (pg as unknown as DatabasePool);
  const getOpencodeClient = options.getOpencodeClient ?? createOpencodeClient;

  async function resolveWithClient(
    client: DatabaseClient,
    strategyId: string,
    lockRow: boolean,
  ) {
    const result = await client.query<StrategySessionRow>(
      [
        "SELECT title, opencode_session_id",
        "FROM strategies",
        "WHERE strategy_id = $1",
        lockRow ? "FOR UPDATE" : "",
      ]
        .join(" ")
        .trim(),
      [strategyId],
    );

    const strategy = result.rows[0];
    if (!strategy) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    const existingSessionId = normalizeSessionId(strategy.opencode_session_id);
    if (existingSessionId) return existingSessionId;

    const opencode = await getOpencodeClient();
    const sessionId = await opencode.createSession(strategy.title);
    const updateResult = await client.query(
      [
        "UPDATE strategies",
        "SET opencode_session_id = $2",
        "WHERE strategy_id = $1",
      ].join(" "),
      [strategyId, sessionId],
    );

    if (updateResult.rowCount !== 1) {
      throw new Error(
        `Failed to persist opencode session for strategy: ${strategyId}`,
      );
    }

    return sessionId;
  }

  return {
    async getOrCreateSession(strategyId: string, existingClient?: DatabaseClient) {
      // When called inside an existing turn transaction, reuse its client to
      // avoid a self-deadlock between the turn's row lock and a fresh
      // SELECT FOR UPDATE on the same row from a separate pool connection.
      if (existingClient) {
        return resolveWithClient(existingClient, strategyId, false);
      }

      const client = await pool.connect();
      let inTransaction = false;
      try {
        await client.query("BEGIN");
        inTransaction = true;
        const sessionId = await resolveWithClient(client, strategyId, true);
        await client.query("COMMIT");
        inTransaction = false;
        return sessionId;
      } catch (error) {
        if (inTransaction) {
          await client.query("ROLLBACK");
        }
        throw error;
      } finally {
        client.release();
      }
    },
  };
}

export const { getOrCreateSession } = createSessionManager();
