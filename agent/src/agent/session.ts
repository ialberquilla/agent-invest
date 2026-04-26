import {
  createOpencodeClient,
  createOpencodeServer,
  type AssistantMessage,
  type OpencodeClient,
  type Event as OpencodeEvent,
  type Part,
} from "@opencode-ai/sdk";
import type { QueryResultRow } from "pg";

import { pg } from "../db/client.js";

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
  getSessionClient?: () => Promise<SessionClient>;
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
      client: createOpencodeClient({
        baseUrl,
        directory,
      }),
      close() {},
    };
  }

  const server = await createOpencodeServer({
    config: {
      model: resolveOpencodeModel(env),
    },
  });

  return {
    client: createOpencodeClient({
      baseUrl: server.url,
      directory,
    }),
    close() {
      server.close();
    },
  };
}

async function getSharedOpencode(
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

async function createDefaultSessionClient(
  env: NodeJS.ProcessEnv = process.env,
): Promise<SessionClient> {
  const { client } = await getSharedOpencode(env);

  return {
    async createSession(title: string) {
      const normalizedTitle = title.trim();
      const session = await client.session.create({
        body: normalizedTitle ? { title: normalizedTitle } : undefined,
        throwOnError: true,
      });

      return session.data.id;
    },
  };
}

export async function createOpencodeTurnClient(
  env: NodeJS.ProcessEnv = process.env,
): Promise<OpencodeTurnClient> {
  const { client } = await getSharedOpencode(env);

  return {
    async prompt({ sessionId, text, system, messageId }) {
      const response = await client.session.prompt({
        path: { id: sessionId },
        body: {
          messageID: messageId,
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
  const getSessionClient =
    options.getSessionClient ?? createDefaultSessionClient;

  return {
    async getOrCreateSession(strategyId: string) {
      const client = await pool.connect();
      let inTransaction = false;

      try {
        await client.query("BEGIN");
        inTransaction = true;

        const result = await client.query<StrategySessionRow>(
          [
            "SELECT title, opencode_session_id",
            "FROM strategies",
            "WHERE strategy_id = $1",
            "FOR UPDATE",
          ].join(" "),
          [strategyId],
        );

        const strategy = result.rows[0];

        if (!strategy) {
          throw new Error(`Strategy not found: ${strategyId}`);
        }

        const existingSessionId = normalizeSessionId(
          strategy.opencode_session_id,
        );

        if (existingSessionId) {
          await client.query("COMMIT");
          inTransaction = false;
          return existingSessionId;
        }

        const sessionClient = await getSessionClient();
        const sessionId = await sessionClient.createSession(strategy.title);
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
